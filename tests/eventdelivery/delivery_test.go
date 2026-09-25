package eventdelivery_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
	ledgerevent "github.com/tonimnim/Pesaro/contracts/events/ledger/v1"
	ledgerv1 "github.com/tonimnim/Pesaro/contracts/gen/ledger/v1"
	"github.com/tonimnim/Pesaro/internal/platform/eventtransport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestContinuousDeliveryAcrossCrashesAndLostAck(t *testing.T) {
	required(t)
	bin := binaries(t)
	f := fixture(t, bin["ledger-admin"])
	book := f["BookID"]
	p := testPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ledgerSQL := observer(t, "PESARO_LEDGER_TEST_RUNTIME_URL")
	inboxSQL := observer(t, "PESAR_RECONCILIATION_TEST_RUNTIME_URL")
	consumerAddress := address(t)
	consumerConfig := filepath.Join(p.dir, "receiver.json")
	jsonFile(t, consumerConfig, map[string]any{"mode": "synthetic", "address": consumerAddress, "producer_identity": producer, "books": []string{book}, "tls": p.files["consumer"]})
	consumerEnv := map[string]string{"PESAR_RECONCILIATION_CONFIG": consumerConfig, "PESAR_RECONCILIATION_DATABASE_URL": os.Getenv("PESAR_RECONCILIATION_TEST_RUNTIME_URL")}
	clientSecurity, err := eventtransport.LoadTLS(consumerConfig, p.files["producer"], false, consumer)
	if err != nil {
		t.Fatal(err)
	}
	forward := &http.Client{Transport: &http.Transport{TLSClientConfig: clientSecurity}, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer forward.CloseIdleConnections()
	var mode, attempts atomic.Int32
	paused := make(chan int32, 8)
	releaseGates := make(chan struct{})
	// The proxy uses an isolated test CA to intercept the consumer response. It
	// never fabricates success: every 204 must first come from the real receiver.
	proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		current := mode.Load()
		if current == 0 {
			w.WriteHeader(503)
			return
		}
		if current == 1 {
			select {
			case paused <- current:
			default:
			}
			select {
			case <-r.Context().Done():
			case <-releaseGates:
			}
			return
		}
		data, err := io.ReadAll(io.LimitReader(r.Body, ledgerevent.MaxBytes+1))
		if err != nil {
			w.WriteHeader(503)
			return
		}
		request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, "https://"+consumerAddress+ledgerevent.Path, bytes.NewReader(data))
		if err != nil {
			w.WriteHeader(503)
			return
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := forward.Do(request)
		if err != nil {
			w.WriteHeader(503)
			return
		}
		defer response.Body.Close()
		if current == 2 && response.StatusCode == 204 {
			select {
			case paused <- current:
			default:
			}
			select {
			case <-r.Context().Done():
			case <-releaseGates:
			}
			return
		}
		w.WriteHeader(response.StatusCode)
	}))
	proxy.TLS, err = eventtransport.LoadTLS(consumerConfig, p.files["consumer"], true, producer)
	if err != nil {
		t.Fatal(err)
	}
	proxy.StartTLS()
	defer func() { close(releaseGates); proxy.Close() }()
	grpcAddress, opsAddress := address(t), address(t)
	ledgerConfig := filepath.Join(p.dir, "ledger.json")
	public := base64.RawURLEncoding.EncodeToString(p.keys["payments"].Public().(ed25519.PublicKey))
	jsonFile(t, ledgerConfig, map[string]any{"mode": "synthetic", "grpc_address": grpcAddress, "http_address": opsAddress,
		"tls":        map[string]string{"certificate": p.files["ledger"].Certificate, "key": p.files["ledger"].Key, "client_ca": p.files["ledger"].CA},
		"callers":    []any{map[string]any{"identity": "spiffe://pesar.test/payments", "books": []string{book}, "permissions": []string{"spend", "provision", "read", "verify"}, "all_subjects": true}},
		"grant_keys": map[string]string{"event-test": public}, "evidence_keys": map[string]string{"event-test": public},
		"outbox": map[string]any{"endpoint": proxy.URL + ledgerevent.Path, "consumer_identity": consumer, "tls": p.files["producer"], "worker": map[string]int{"poll_ms": 50, "lease_ms": 4000, "timeout_ms": 1500}},
	})
	ledgerEnv := map[string]string{"PESAR_LEDGER_CONFIG": ledgerConfig, "PESAR_LEDGER_DATABASE_URL": os.Getenv("PESARO_LEDGER_TEST_RUNTIME_URL")}
	producerProcess := start(t, bin["ledger"], ledgerEnv)
	ready(t, producerProcess, "http://"+opsAddress+"/readyz", &http.Client{Timeout: time.Second})
	connection, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(credentials.NewTLS(p.security(t, "payments"))), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	rpc := ledgerv1.NewLedgerClient(connection)
	// Commit principal+fee while delivery is unavailable. Ledger response and
	// balances depend on the financial transaction, never on the remote inbox.
	now := time.Now().UTC()
	payment, quote := id(), id()
	claims := map[string]string{"id": id(), "issuer": "event-test", "book_id": book, "payment_id": payment, "subject_id": f["OwnerA"], "source_id": f["WalletA"], "beneficiary_id": f["WalletB"], "principal": "10000", "fee": "100", "policy_id": f["Policy100"], "quote_id": quote, "currency": "KES", "subject_epoch": "1", "account_epoch": "1", "not_before": now.Add(-time.Minute).Format(time.RFC3339Nano), "expires_at": now.Add(time.Hour).Format(time.RFC3339Nano)}
	claimsJSON, _ := json.Marshal(claims)
	canonical, err := jsoncanonicalizer.Transform(claimsJSON)
	if err != nil {
		t.Fatal(err)
	}
	grant := &ledgerv1.Grant{}
	if protojson.Unmarshal(claimsJSON, grant) != nil {
		t.Fatal("grant encoding")
	}
	signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(p.keys["payments"], append([]byte("pesaro.ledger/grant/v1\n"), canonical...)))
	request := &ledgerv1.TransferInternalRequest{Envelope: &ledgerv1.Envelope{SchemaVersion: "1", BookId: book, OperationId: id()}, Terms: &ledgerv1.Terms{PaymentId: payment, SourceId: f["WalletA"], BeneficiaryId: f["WalletB"], Principal: "10000", Fee: "100", Currency: "KES", PolicyId: f["Policy100"], QuoteId: quote, Grant: &ledgerv1.SignedGrant{Claims: grant, Signature: signature}}}
	receipt, err := rpc.TransferInternal(ctx, request)
	if err != nil || receipt.GetOutcome() != "APPLIED" {
		t.Fatal(receipt, err)
	}
	balance, err := rpc.GetBalance(ctx, &ledgerv1.GetBalanceRequest{BookId: book, AccountId: f["WalletA"]})
	if err != nil || balance.GetAvailable() != "89900" {
		t.Fatal("consumer outage blocked/changed money", balance, err)
	}
	eventually(t, "failed delivery attempt", func() bool { return attempts.Load() > 0 })
	assertRows := func(want int) {
		t.Helper()
		var received, projected int
		if err := inboxSQL.QueryRow(ctx, "SELECT (SELECT count(*) FROM event_inbox WHERE book_id=$1),(SELECT count(*) FROM ledger_operations WHERE book_id=$1)", book).Scan(&received, &projected); err != nil || received != want || projected != want {
			t.Fatalf("inbox=%d projection=%d want=%d: %v", received, projected, want, err)
		}
	}
	assertRows(0)
	waitGate := func(want int32) {
		t.Helper()
		select {
		case got := <-paused:
			if got != want {
				t.Fatal("wrong failure gate", got, want)
			}
		case <-ctx.Done():
			t.Fatal("failure gate not reached")
		}
	}
	mode.Store(1)
	waitGate(1)
	producerProcess.kill(t)
	assertRows(0)
	t.Log("publisher killed after claim, before consumer submission: no inbox mutation")
	receiverProcess := start(t, bin["reconciliation"], consumerEnv)
	ready(t, receiverProcess, "https://"+consumerAddress+"/readyz", forward)
	mode.Store(2)
	producerProcess = start(t, bin["ledger"], ledgerEnv)
	ready(t, producerProcess, "http://"+opsAddress+"/readyz", &http.Client{Timeout: time.Second})
	waitGate(2)
	producerProcess.kill(t)
	receiverProcess.kill(t)
	assertRows(1)
	var acknowledged int
	if err = ledgerSQL.QueryRow(ctx, "SELECT count(*) FROM outbox_delivery WHERE book_id=$1 AND delivered", book).Scan(&acknowledged); err != nil || acknowledged != 0 {
		t.Fatal("lost consumer ack was treated as delivered", acknowledged, err)
	}
	t.Log("receiver committed; response withheld; both processes killed; one durable inbox/projection, no producer ack")
	mode.Store(3)
	receiverProcess = start(t, bin["reconciliation"], consumerEnv)
	ready(t, receiverProcess, "https://"+consumerAddress+"/readyz", forward)
	producerProcess = start(t, bin["ledger"], ledgerEnv)
	ready(t, producerProcess, "http://"+opsAddress+"/readyz", &http.Client{Timeout: time.Second})
	waitDelivery := func(want int) {
		t.Helper()
		eventually(t, "durable publisher acknowledgement", func() bool {
			producerProcess.check(t)
			receiverProcess.check(t)
			return ledgerSQL.QueryRow(ctx, "SELECT count(*) FROM outbox_delivery WHERE book_id=$1 AND delivered", book).Scan(&acknowledged) == nil && acknowledged == want
		})
		assertRows(want)
	}
	waitDelivery(2)
	// New facts committed after the worker started must be discovered by polling.
	created, err := rpc.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{Envelope: &ledgerv1.Envelope{SchemaVersion: "1", BookId: book, OperationId: id()}, AccountId: id(), OwnerId: id(), Purpose: "WALLET"})
	if err != nil || created.GetOutcome() != "APPLIED" {
		t.Fatal(created, err)
	}
	waitDelivery(3)
	producerProcess.kill(t)
	producerProcess = start(t, bin["ledger"], ledgerEnv)
	ready(t, producerProcess, "http://"+opsAddress+"/readyz", &http.Client{Timeout: time.Second})
	waitDelivery(3)
	var eventID string
	var body []byte
	if err = ledgerSQL.QueryRow(ctx, "SELECT event_id::STRING,body FROM outbox_facts WHERE book_id=$1 AND operation_id=$2", book, receipt.OperationId).Scan(&eventID, &body); err != nil {
		t.Fatal(err)
	}
	event, err := ledgerevent.New(book, eventID, receipt.OperationId, body)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := event.Encode()
	post := func(client *http.Client, data []byte) int {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+consumerAddress+ledgerevent.Path, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		if err != nil {
			return 0
		}
		defer res.Body.Close()
		return res.StatusCode
	}
	for range 3 {
		if status := post(forward, wire); status != 204 {
			t.Fatal("identical redelivery rejected", status)
		}
	}
	assertRows(3)
	var changed map[string]any
	if json.Unmarshal(body, &changed) != nil {
		t.Fatal("receipt")
	}
	changed["outcome"] = "REJECTED"
	event.Receipt, _ = json.Marshal(changed)
	conflict, _ := event.Encode()
	if status := post(forward, conflict); status != 409 {
		t.Fatal("changed event accepted", status)
	}
	changed["outcome"] = "APPLIED"
	changed["event_id"] = id()
	event.EventID = changed["event_id"].(string)
	event.Receipt, _ = json.Marshal(changed)
	conflict, _ = event.Encode()
	if status := post(forward, conflict); status != 409 {
		t.Fatal("new event bypassed operation binding", status)
	}
	changed["book_id"] = id()
	event.BookID = changed["book_id"].(string)
	event.Receipt, _ = json.Marshal(changed)
	conflict, _ = event.Encode()
	if status := post(forward, conflict); status != 403 {
		t.Fatal("cross-book event accepted", status)
	}
	for _, security := range []*tls.Config{p.security(t, "outsider"), {MinVersion: tls.VersionTLS13, RootCAs: p.roots}} {
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: security}, Timeout: time.Second}
		if status := post(client, wire); status == 204 {
			t.Fatal("unauthenticated producer accepted")
		}
		client.CloseIdleConnections()
	}
	assertRows(3)
	// Confirm the delivery/replay path never changed the original money effect.
	replayed, err := rpc.TransferInternal(ctx, request)
	if err != nil || replayed.GetJournalId() != receipt.GetJournalId() {
		t.Fatal("financial identity changed", replayed, err)
	}
	verifyLedger(t, ctx, rpc, book, bin["verify"])
	t.Logf("recovered all 3 committed events; exactly 3 inbox/projection rows; transfer still A=89900; %d transport attempts (duplicates expected)", attempts.Load())
}

func verifyLedger(t *testing.T, ctx context.Context, client ledgerv1.LedgerClient, book, binary string) {
	t.Helper()
	stream, err := client.ExportLedgerSnapshot(ctx, &ledgerv1.ExportLedgerSnapshotRequest{BookId: book})
	if err != nil {
		t.Fatal(err)
	}
	var data []byte
	var expected string
	var index uint32
	var cut string
	last := false
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if last || chunk.Index != index || chunk.BookId != book || chunk.SchemaVersion != "1" {
			t.Fatal("invalid snapshot frame")
		}
		if index == 0 {
			cut, expected = chunk.Cut, chunk.Sha256
		}
		if cut != chunk.Cut || expected != chunk.Sha256 {
			t.Fatal("snapshot cut changed")
		}
		index++
		data = append(data, chunk.Data...)
		last = chunk.Last
	}
	sum := sha256.Sum256(data)
	if !last || expected == "" || hex.EncodeToString(sum[:]) != expected {
		t.Fatal("incomplete snapshot")
	}
	dir := t.TempDir()
	if evidence := os.Getenv("PESAR_LEDGER_EVIDENCE_DIR"); evidence != "" {
		dir = evidence
	}
	path := filepath.Join(dir, "event-delivery-snapshot.json")
	if os.WriteFile(path, data, 0600) != nil {
		t.Fatal("snapshot file")
	}
	cmd := exec.CommandContext(ctx, binary, "-snapshot", path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("independent verification: %v %s", err, output)
	}
	if os.WriteFile(filepath.Join(dir, "event-delivery-report.json"), output, 0600) != nil {
		t.Fatal("verifier report")
	}
}
