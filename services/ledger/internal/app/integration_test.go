package app

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ledgerv1 "github.com/tonimnim/Pesaro/contracts/gen/ledger/v1"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
	"github.com/tonimnim/Pesaro/services/ledger/internal/store/cockroach"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type apiEnvironment struct {
	admin         *cockroach.Store
	fixture       cockroach.Fixture
	pki           testPKI
	dsn           string
	runtime       *Runtime
	address, http string
	stop          func()
	client        ledgerv1.LedgerClient
}

func apiSetup(t *testing.T) *apiEnvironment {
	t.Helper()
	adminDSN, runtimeDSN := os.Getenv("PESARO_LEDGER_TEST_ADMIN_URL"), os.Getenv("PESARO_LEDGER_TEST_RUNTIME_URL")
	if adminDSN == "" || runtimeDSN == "" {
		t.Skip("requires real CockroachDB: PESARO_LEDGER_TEST_ADMIN_URL and PESARO_LEDGER_TEST_RUNTIME_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := cockroach.Open(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err = admin.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = admin.GrantRuntime(ctx); err != nil {
		t.Fatal(err)
	}
	fixture, err := admin.BootstrapSynthetic(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e := &apiEnvironment{admin: admin, fixture: fixture, pki: pki(t, string(fixture.BookID)), dsn: runtimeDSN}
	e.start(t)
	return e
}
func clientAt(t *testing.T, address string, security *tls.Config) ledgerv1.LedgerClient {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(security.Clone())), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ledgerv1.NewLedgerClient(conn)
}
func (e *apiEnvironment) start(t *testing.T) {
	t.Helper()
	r, err := Open(context.Background(), e.pki.path, e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	rpc, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	ops, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		rpc.Close()
		r.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Serve(ctx, rpc, ops) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(8 * time.Second):
				t.Error("runtime did not drain")
			}
			r.Close()
		})
	}
	t.Cleanup(stop)
	e.runtime = r
	e.stop = stop
	e.address = rpc.Addr().String()
	e.http = "http://" + ops.Addr().String()
	e.client = clientAt(t, e.address, e.pki.clients["payments"])
	deadline := time.Now().Add(5 * time.Second)
	httpClient := &http.Client{Timeout: time.Second}
	for {
		response, err := httpClient.Get(e.http + "/readyz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("synthetic Ledger not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func amount(t *testing.T, units string) domain.Amount {
	t.Helper()
	a, e := domain.ParseAmount(units)
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func toProto(t *testing.T, source any, destination proto.Message) {
	t.Helper()
	data, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = protojson.Unmarshal(data, destination); err != nil {
		t.Fatal(err)
	}
}
func (e *apiEnvironment) envelope() *ledgerv1.Envelope {
	return &ledgerv1.Envelope{SchemaVersion: "1", BookId: string(e.fixture.BookID), OperationId: string(domain.NewID())}
}
func (e *apiEnvironment) terms(t *testing.T, principal, fee string, policy domain.ID) *ledgerv1.Terms {
	t.Helper()
	now := time.Now().UTC()
	f := e.fixture
	a := application.Terms{PaymentID: domain.NewID(), SourceID: f.WalletA, BeneficiaryID: f.WalletB, Principal: amount(t, principal), Fee: amount(t, fee), PolicyID: policy, QuoteID: domain.NewID(), Currency: "KES"}
	g := application.Grant{ID: domain.NewID(), Issuer: "grant-test", BookID: f.BookID, PaymentID: a.PaymentID, SubjectID: f.OwnerA, SourceID: a.SourceID, BeneficiaryID: a.BeneficiaryID, Principal: a.Principal, Fee: a.Fee, PolicyID: policy, QuoteID: a.QuoteID, Currency: "KES", SubjectEpoch: 1, AccountEpoch: 1, NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour)}
	var err error
	a.Grant, err = application.SignGrant(g, e.pki.grantKey)
	if err != nil {
		t.Fatal(err)
	}
	wire := &ledgerv1.Terms{}
	toProto(t, a, wire)
	return wire
}
func (e *apiEnvironment) balance(t *testing.T, account domain.ID, posted, held, available string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := e.client.GetBalance(ctx, &ledgerv1.GetBalanceRequest{BookId: string(e.fixture.BookID), AccountId: string(account)})
	if err != nil || b.GetPosted() != posted || b.GetHeld() != held || b.GetAvailable() != available {
		t.Fatalf("balance got %v error %v; want posted=%s held=%s available=%s", b, err, posted, held, available)
	}
}
func applied(t *testing.T, r *ledgerv1.Receipt, err error) *ledgerv1.Receipt {
	t.Helper()
	if err != nil || r.GetOutcome() != "APPLIED" {
		t.Fatalf("want applied: %v %v", r, err)
	}
	return r
}
func TestLedgerChildProcess(t *testing.T) {
	if os.Getenv("PESAR_TEST_CHILD") != "1" {
		t.Skip("subprocess helper")
	}
	if err := Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func startChild(t *testing.T, e *apiEnvironment) (string, func()) {
	t.Helper()
	unused := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := l.Addr().String()
		l.Close()
		return address
	}
	e.pki.config.GRPCAddress = unused()
	e.pki.config.HTTPAddress = unused()
	e.pki.save(t)
	child := exec.Command(os.Args[0], "-test.run=^TestLedgerChildProcess$")
	// The serving process receives only the restricted SQL credentials.
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "PESARO_LEDGER_TEST_") || strings.HasPrefix(entry, "PESAR_LEDGER_") || strings.HasPrefix(entry, "PESAR_TEST_CHILD=") {
			continue
		}
		child.Env = append(child.Env, entry)
	}
	child.Env = append(child.Env, "PESAR_TEST_CHILD=1", "PESAR_LEDGER_CONFIG="+e.pki.path, "PESAR_LEDGER_DATABASE_URL="+e.dsn)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- child.Wait() }()
	var once sync.Once
	kill := func() {
		once.Do(func() {
			_ = child.Process.Kill()
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Error("child did not terminate")
			}
		})
	}
	t.Cleanup(kill)
	httpClient := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, err := httpClient.Get("http://" + e.pki.config.HTTPAddress + "/readyz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				return e.pki.config.GRPCAddress, kill
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("child runtime failed readiness")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestAPITransferRestartAndAuthorization(t *testing.T) {
	e := apiSetup(t)
	f := e.fixture
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	req := &ledgerv1.TransferInternalRequest{Envelope: e.envelope(), Terms: e.terms(t, "10000", "100", f.Policy100)}
	result, err := e.client.TransferInternal(ctx, req)
	r := applied(t, result, err)
	e.balance(t, f.WalletA, "89900", "0", "89900")
	e.balance(t, f.WalletB, "10000", "0", "10000")
	e.balance(t, f.FeeAccount, "100", "0", "100")
	if r.CallerIdentity != "spiffe://pesar.test/payments" {
		t.Fatal("missing authenticated audit actor")
	}
	reader := clientAt(t, e.address, e.pki.clients["reader"])
	if _, err = reader.TransferInternal(ctx, req); status.Code(err) != codes.PermissionDenied {
		t.Fatal("read-only identity could replay spend", err)
	}
	for _, name := range []string{"outsider", "no-uri"} {
		client := clientAt(t, e.address, e.pki.clients[name])
		if _, err = client.GetBalance(ctx, &ledgerv1.GetBalanceRequest{BookId: string(f.BookID), AccountId: string(f.WalletA)}); status.Code(err) != codes.PermissionDenied {
			t.Fatal("unscoped identity accepted", name, err)
		}
	}
	noCertificate := e.pki.clients["payments"].Clone()
	noCertificate.Certificates = nil
	unsigned := clientAt(t, e.address, noCertificate)
	short, stop := context.WithTimeout(ctx, time.Second)
	_, err = unsigned.GetBalance(short, &ledgerv1.GetBalanceRequest{BookId: string(f.BookID), AccountId: string(f.WalletA)})
	stop()
	if err == nil {
		t.Fatal("accepted client without certificate")
	}
	if _, err = e.client.GetBalance(ctx, &ledgerv1.GetBalanceRequest{BookId: string(domain.NewID()), AccountId: string(f.WalletA)}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("cross-book disclosure", err)
	}
	changed := proto.Clone(req).(*ledgerv1.TransferInternalRequest)
	changed.Terms.Principal = "10001"
	if _, err = e.client.TransferInternal(ctx, changed); status.Code(err) != codes.AlreadyExists {
		t.Fatal("material identity conflict not rejected", err)
	}
	unknown := proto.Clone(req).(*ledgerv1.TransferInternalRequest)
	unknown.Terms.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	if _, err = e.client.TransferInternal(ctx, unknown); status.Code(err) != codes.InvalidArgument {
		t.Fatal("unknown material field accepted", err)
	}
	invalid := proto.Clone(req).(*ledgerv1.TransferInternalRequest)
	invalid.Terms.Principal = "1.00"
	if _, err = e.client.TransferInternal(ctx, invalid); status.Code(err) != codes.InvalidArgument {
		t.Fatal("fractional minor units accepted", err)
	}
	e.stop()
	e.start(t)
	replay, err := e.client.TransferInternal(ctx, req)
	if err != nil || !proto.Equal(r, replay) {
		t.Fatal("restart changed durable result", err)
	}
	e.balance(t, f.WalletA, "89900", "0", "89900")
	stream, err := e.client.ExportLedgerSnapshot(ctx, &ledgerv1.ExportLedgerSnapshotRequest{BookId: string(f.BookID)})
	if err != nil {
		t.Fatal(err)
	}
	var artifact []byte
	var digest string
	var cut string
	var index uint32
	last := false
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if last || chunk.Index != index || chunk.BookId != string(f.BookID) || chunk.SchemaVersion != "1" {
			t.Fatal("invalid snapshot framing")
		}
		if index == 0 {
			digest = chunk.Sha256
			cut = chunk.Cut
		}
		if digest != chunk.Sha256 || cut != chunk.Cut {
			t.Fatal("snapshot cut/digest changed")
		}
		artifact = append(artifact, chunk.Data...)
		index++
		last = chunk.Last
	}
	hash := sha256.Sum256(artifact)
	if !last || hex.EncodeToString(hash[:]) != digest {
		t.Fatal("incomplete snapshot")
	}
	var snap application.Snapshot
	if json.Unmarshal(artifact, &snap) != nil || snap.Counts["journals"] != 2 || snap.Counts["financial_operations"] != 2 || snap.Counts["outbox_facts"] != 2 {
		t.Fatal("duplicate or missing financial effect")
	}
	verifyArtifact(t, artifact, "transfer")
}
func TestAPIHoldCaptureReleaseAndControls(t *testing.T) {
	e := apiSetup(t)
	f := e.fixture
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	reserve := &ledgerv1.ReservePayoutRequest{Envelope: e.envelope(), Terms: e.terms(t, "20000", "200", f.Policy200), HoldId: string(domain.NewID()), AttemptId: string(domain.NewID()), PoolId: string(f.Pool), ExpiresAt: time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339Nano)}
	result, err := e.client.ReservePayout(ctx, reserve)
	applied(t, result, err)
	e.balance(t, f.WalletA, "100000", "20200", "79800")
	e.balance(t, f.Pool, "100000", "20000", "80000")
	verifyEnvironment(t, e, "reserved")
	ref := &ledgerv1.HoldRef{PaymentId: reserve.Terms.PaymentId, HoldId: reserve.HoldId, AttemptId: reserve.AttemptId, ExpectedVersion: "1"}
	result, err = e.client.MarkHoldExposed(ctx, &ledgerv1.MarkHoldExposedRequest{Envelope: e.envelope(), Ref: ref, GrantId: reserve.Terms.Grant.Claims.Id})
	applied(t, result, err)
	verifyEnvironment(t, e, "exposed")
	result, err = e.client.SetAccountControl(ctx, &ledgerv1.SetAccountControlRequest{Envelope: e.envelope(), Key: &ledgerv1.ControlKey{Kind: "SUBJECT", Id: string(f.OwnerA)}, ExpectedVersion: "1", DebitFrozen: true, DailyCap: "200000", RevokeGrants: true, Reason: "TEST_FREEZE"})
	applied(t, result, err)
	ev := application.Evidence{ID: domain.NewID(), Issuer: "evidence-test", BookID: f.BookID, PaymentID: domain.ID(ref.PaymentId), HoldID: domain.ID(ref.HoldId), AttemptID: domain.ID(ref.AttemptId), ProviderAccountID: f.ProviderAccount, BeneficiaryID: f.WalletB, Principal: amount(t, "20000"), Currency: "KES", CapabilityID: f.Capability, FinalState: "FINAL_SUCCESS", ObservedAt: time.Now().UTC(), SourceDigest: strings.Repeat("ab", 32)}
	signed, err := application.SignEvidence(ev, e.pki.evidenceKey)
	if err != nil {
		t.Fatal(err)
	}
	proof := &ledgerv1.SignedEvidence{}
	toProto(t, signed, proof)
	ref.ExpectedVersion = "2"
	result, err = e.client.CapturePayout(ctx, &ledgerv1.CapturePayoutRequest{Envelope: e.envelope(), Ref: ref, Evidence: proof})
	applied(t, result, err)
	e.balance(t, f.WalletA, "79800", "0", "79800")
	e.balance(t, f.Pool, "80000", "0", "80000")
	e.balance(t, f.FeeAccount, "200", "0", "200")
	// Provisioning cannot mint opening money; uppercase UUID input normalizes.
	account := domain.NewID()
	result, err = e.client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{Envelope: e.envelope(), AccountId: strings.ToUpper(string(account)), OwnerId: string(domain.NewID()), Purpose: "WALLET"})
	applied(t, result, err)
	e.balance(t, account, "0", "0", "0")
	// Separate fresh book: release an unexposed hold with no fee posting.
	released := apiSetup(t)
	q := &ledgerv1.ReservePayoutRequest{Envelope: released.envelope(), Terms: released.terms(t, "20000", "200", released.fixture.Policy200), HoldId: string(domain.NewID()), AttemptId: string(domain.NewID()), PoolId: string(released.fixture.Pool), ExpiresAt: time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339Nano)}
	result, err = released.client.ReservePayout(ctx, q)
	applied(t, result, err)
	result, err = released.client.ReleasePayout(ctx, &ledgerv1.ReleasePayoutRequest{Envelope: released.envelope(), Ref: &ledgerv1.HoldRef{PaymentId: q.Terms.PaymentId, HoldId: q.HoldId, AttemptId: q.AttemptId, ExpectedVersion: "1"}, Reason: "CANCEL"})
	applied(t, result, err)
	released.balance(t, released.fixture.WalletA, "100000", "0", "100000")
	released.balance(t, released.fixture.Pool, "100000", "0", "100000")
	released.balance(t, released.fixture.FeeAccount, "0", "0", "0")
	for name, environment := range map[string]*apiEnvironment{"capture": e, "release": released} {
		snapshot, err := environment.admin.Snapshot(ctx, environment.fixture.BookID)
		if err != nil {
			t.Fatal(err)
		}
		artifact, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		verifyArtifact(t, artifact, name)
	}
}

// responseDropProxy forwards TLS without decrypting it. After arming, it consumes
// but discards server bytes while the real server/database continue to commit.
func responseDropProxy(t *testing.T, target string) (string, *atomic.Bool, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	armed := &atomic.Bool{}
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			backend, err := net.DialTimeout("tcp", target, time.Second)
			if err != nil {
				client.Close()
				continue
			}
			mu.Lock()
			connections = append(connections, client, backend)
			mu.Unlock()
			workers.Add(2)
			go func() { defer workers.Done(); _, _ = io.Copy(backend, client); backend.Close() }()
			go func() {
				defer workers.Done()
				defer client.Close()
				buf := make([]byte, 32<<10)
				for {
					n, err := backend.Read(buf)
					if n > 0 && !armed.Load() {
						if _, e := client.Write(buf[:n]); e != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			listener.Close()
			<-acceptDone
			mu.Lock()
			for _, c := range connections {
				c.Close()
			}
			mu.Unlock()
			workers.Wait()
		})
	}
	t.Cleanup(stop)
	return listener.Addr().String(), armed, stop
}
func TestAPILostNetworkResponseRecovery(t *testing.T) {
	e := apiSetup(t)
	f := e.fixture
	e.stop()
	// Run the actual composition root in a separate process and kill it after
	// the database commit, with the caller's network response still suppressed.
	childAddress, kill := startChild(t, e)
	address, drop, stop := responseDropProxy(t, childAddress)
	client := clientAt(t, address, e.pki.clients["payments"])
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Establish TLS and HTTP/2 before dropping encrypted response frames.
	if _, err := client.GetBalance(ctx, &ledgerv1.GetBalanceRequest{BookId: string(f.BookID), AccountId: string(f.WalletA)}); err != nil {
		t.Fatal(err)
	}
	request := &ledgerv1.TransferInternalRequest{Envelope: e.envelope(), Terms: e.terms(t, "10000", "100", f.Policy100)}
	drop.Store(true)
	reply := make(chan error, 1)
	go func() { _, err := client.TransferInternal(ctx, request); reply <- err }()
	var committed *application.Operation
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		committed, err = e.admin.Operation(ctx, f.BookID, domain.ID(request.Envelope.OperationId))
		if err != nil {
			t.Fatal(err)
		}
		if committed != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if committed == nil {
		t.Fatal("request did not commit while response was dropped")
	}
	kill()
	stop()
	if err := <-reply; err == nil {
		t.Fatal("response unexpectedly delivered")
	}
	e.start(t)
	recovered, err := e.client.GetOperation(ctx, &ledgerv1.GetOperationRequest{BookId: string(f.BookID), OperationId: request.Envelope.OperationId})
	if err != nil || recovered.GetJournalId() != string(committed.Receipt.JournalID) {
		t.Fatal("cannot recover committed network outcome", err)
	}
	replay, err := e.client.TransferInternal(ctx, request)
	if err != nil || !proto.Equal(recovered, replay) {
		t.Fatal("lost-response retry changed result", err)
	}
	e.balance(t, f.WalletA, "89900", "0", "89900")
	snapshot, err := e.admin.Snapshot(ctx, f.BookID)
	if err != nil || snapshot.Counts["journals"] != 2 || snapshot.Counts["outbox_facts"] != 2 {
		t.Fatal("duplicate posting or event", err)
	}
	artifact, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	verifyArtifact(t, artifact, "process-recovery")
}

func verifyEnvironment(t *testing.T, e *apiEnvironment, label string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snapshot, err := e.admin.Snapshot(ctx, e.fixture.BookID)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	verifyArtifact(t, artifact, label)
}

type dropCommitStore struct {
	*cockroach.Store
	drop *atomic.Bool
}

func (s *dropCommitStore) Transact(ctx context.Context, fn func(application.Transaction) (application.Receipt, error)) (application.Receipt, error) {
	return s.Store.Transact(ctx, func(tx application.Transaction) (application.Receipt, error) {
		receipt, err := fn(tx)
		// All SQL work has replied. Drop the next response: COMMIT's result.
		if err == nil {
			s.drop.Store(true)
		}
		return receipt, err
	})
}
func TestLostDatabaseCommitReply(t *testing.T) {
	e := apiSetup(t)
	f := e.fixture
	dbURL, err := url.Parse(e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	proxy, drop, stop := responseDropProxy(t, dbURL.Host)
	dbURL.Host = proxy
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := cockroach.Open(ctx, dbURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	config, err := loadConfig(e.pki.path)
	if err != nil {
		t.Fatal(err)
	}
	service := application.New(&dropCommitStore{Store: store, drop: drop}, config.trust, nil)
	caller := config.rules["spiffe://pesar.test/payments"]
	var terms application.Terms
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(e.terms(t, "10000", "100", f.Policy100))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &terms); err != nil {
		t.Fatal(err)
	}
	command := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: terms}}
	commitCtx, commitCancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = service.Execute(commitCtx, caller, command)
	commitCancel()
	if !errors.Is(err, application.ErrUnknown) {
		t.Fatalf("lost commit reply must be unknown, got %v", err)
	}
	stop()
	persisted, err := e.admin.Operation(ctx, f.BookID, command.OperationID)
	if err != nil || persisted == nil || persisted.Receipt.Outcome != "APPLIED" {
		t.Fatal("lost commit reply did not persist applied operation", err)
	}
	request := &ledgerv1.TransferInternalRequest{Envelope: &ledgerv1.Envelope{SchemaVersion: "1", BookId: string(f.BookID), OperationId: string(command.OperationID)}, Terms: &ledgerv1.Terms{}}
	toProto(t, terms, request.Terms)
	replay, err := e.client.TransferInternal(ctx, request)
	if err != nil || replay.GetJournalId() != string(persisted.Receipt.JournalID) {
		t.Fatal("ambiguous commit recovery diverged", err)
	}
	e.balance(t, f.WalletA, "89900", "0", "89900")
	verifyEnvironment(t, e, "lost-database-commit")
}

type restartRecord struct {
	Fixture cockroach.Fixture
	Request *ledgerv1.TransferInternalRequest
	Receipt *ledgerv1.Receipt
	HoldRef *ledgerv1.HoldRef
}

func TestDatabaseRestartPrepare(t *testing.T) {
	path := os.Getenv("PESAR_LEDGER_RESTART_PREPARE")
	if path == "" {
		t.Skip("external database-restart drill preparation")
	}
	e := apiSetup(t)
	f := e.fixture
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request := &ledgerv1.TransferInternalRequest{Envelope: e.envelope(), Terms: e.terms(t, "10000", "100", f.Policy100)}
	result, err := e.client.TransferInternal(ctx, request)
	receipt := applied(t, result, err)
	q := &ledgerv1.ReservePayoutRequest{Envelope: e.envelope(), Terms: e.terms(t, "20000", "200", f.Policy200), HoldId: string(domain.NewID()), AttemptId: string(domain.NewID()), PoolId: string(f.Pool), ExpiresAt: time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339Nano)}
	result, err = e.client.ReservePayout(ctx, q)
	applied(t, result, err)
	ref := &ledgerv1.HoldRef{PaymentId: q.Terms.PaymentId, HoldId: q.HoldId, AttemptId: q.AttemptId, ExpectedVersion: "1"}
	result, err = e.client.MarkHoldExposed(ctx, &ledgerv1.MarkHoldExposedRequest{Envelope: e.envelope(), Ref: ref, GrantId: q.Terms.Grant.Claims.Id})
	applied(t, result, err)
	ref.ExpectedVersion = "2"
	record := restartRecord{Fixture: f, Request: request, Receipt: receipt, HoldRef: ref}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	e.balance(t, f.WalletA, "89900", "20200", "69700")
	verifyEnvironment(t, e, "before-database-restart")
}
func TestDatabaseRestartRecover(t *testing.T) {
	path := os.Getenv("PESAR_LEDGER_RESTART_RECOVER")
	if path == "" {
		t.Skip("external database-restart drill recovery")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record restartRecord
	if err = json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	adminURL, dsn := os.Getenv("PESARO_LEDGER_TEST_ADMIN_URL"), os.Getenv("PESARO_LEDGER_TEST_RUNTIME_URL")
	if adminURL == "" || dsn == "" {
		t.Fatal("database recovery configuration missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := cockroach.Open(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	f := record.Fixture
	e := &apiEnvironment{admin: admin, fixture: f, pki: pki(t, string(f.BookID)), dsn: dsn}
	e.start(t)
	replay, err := e.client.TransferInternal(ctx, record.Request)
	if err != nil || !proto.Equal(replay, record.Receipt) {
		t.Fatal("database restart lost/changed acknowledged receipt", err)
	}
	e.balance(t, f.WalletA, "89900", "20200", "69700")
	e.balance(t, f.Pool, "100000", "20000", "80000")
	evidence := application.Evidence{ID: domain.NewID(), Issuer: "evidence-test", BookID: f.BookID, PaymentID: domain.ID(record.HoldRef.PaymentId), HoldID: domain.ID(record.HoldRef.HoldId), AttemptID: domain.ID(record.HoldRef.AttemptId), ProviderAccountID: f.ProviderAccount, BeneficiaryID: f.WalletB, Principal: amount(t, "20000"), Currency: "KES", CapabilityID: f.Capability, FinalState: "FINAL_SUCCESS", ObservedAt: time.Now().UTC(), SourceDigest: strings.Repeat("cd", 32)}
	signed, err := application.SignEvidence(evidence, e.pki.evidenceKey)
	if err != nil {
		t.Fatal(err)
	}
	proof := &ledgerv1.SignedEvidence{}
	toProto(t, signed, proof)
	result, err := e.client.CapturePayout(ctx, &ledgerv1.CapturePayoutRequest{Envelope: e.envelope(), Ref: record.HoldRef, Evidence: proof})
	applied(t, result, err)
	e.balance(t, f.WalletA, "69700", "0", "69700")
	e.balance(t, f.FeeAccount, "300", "0", "300")
	e.balance(t, f.Pool, "80000", "0", "80000")
	verifyEnvironment(t, e, "after-database-restart")
}
func TestUnauthenticatedInstructionCannotClaimPayment(t *testing.T) {
	e := apiSetup(t)
	f := e.fixture
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request := &ledgerv1.TransferInternalRequest{Envelope: e.envelope(), Terms: e.terms(t, "10000", "100", f.Policy100)}
	request.Terms.SourceId = string(f.Pool)
	// The service identity is valid, but the grant does not authorize this source.
	if _, err := e.client.TransferInternal(ctx, request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("invalid grant admitted: %v", err)
	}
	if _, err := e.client.GetOperation(ctx, &ledgerv1.GetOperationRequest{BookId: string(f.BookID), OperationId: request.Envelope.OperationId}); status.Code(err) != codes.NotFound {
		t.Fatal("unauthorized instruction persisted a receipt", err)
	}
	snapshot, err := e.admin.Snapshot(ctx, f.BookID)
	if err != nil || snapshot.Counts["business_claims"] != 0 || snapshot.Counts["financial_operations"] != 1 {
		t.Fatal("unauthorized instruction claimed business identity", err)
	}
}

func verifyArtifact(t *testing.T, artifact []byte, label string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, artifact, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Cross-service verification uses an artifact and executable, never imports
	// another service's private accounting implementation.
	cmd := exec.CommandContext(ctx, "go", "run", "../../../reconciliation/cmd/ledger-verify", "-snapshot", path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("independent verifier (%s): %v\n%s", label, err, output)
	}
	if directory := os.Getenv("PESAR_LEDGER_EVIDENCE_DIR"); directory != "" {
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, label+".json"), artifact, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, label+"-report.json"), output, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
