package transport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
	ledgerv1 "github.com/tonimnim/Pesaro/contracts/gen/ledger/v1"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type canonicalVector struct {
	ID        string            `json:"id"`
	Method    string            `json:"method"`
	JSON      string            `json:"request_json"`
	Protobuf  string            `json:"protobuf_hex"`
	Canonical string            `json:"expected_canonical"`
	SHA256    string            `json:"expected_sha256"`
	Same      string            `json:"same_as"`
	Different string            `json:"different_from"`
	Metadata  map[string]string `json:"metadata"`
}

type canonicalCorpus struct {
	Version  string            `json:"schema_version"`
	Profile  string            `json:"profile"`
	Notice   string            `json:"notice"`
	Seed     string            `json:"test_only_ed25519_seed_hex"`
	Public   string            `json:"public_key_hex"`
	Commands []canonicalVector `json:"commands"`
	Rejected []canonicalVector `json:"rejected"`
	JCS      []struct {
		ID        string `json:"id"`
		Input     string `json:"input"`
		Canonical string `json:"expected_canonical"`
		SHA256    string `json:"expected_sha256"`
		Reject    bool   `json:"reject"`
	} `json:"jcs"`
	Proofs []struct {
		ID        string `json:"id"`
		Purpose   string `json:"purpose"`
		Claims    string `json:"claims_json"`
		Message   string `json:"expected_message"`
		SHA256    string `json:"expected_sha256"`
		Signature string `json:"signature"`
	} `json:"proofs"`
}

func loadCanonicalCorpus(t *testing.T) canonicalCorpus {
	t.Helper()
	data, err := os.ReadFile("../../../../contracts/ledger/v1/testdata/canonical-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var c canonicalCorpus
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		t.Fatal(err)
	}
	if c.Version != "1" || c.Profile != "pesaro.ledger/canonical-v1" {
		t.Fatal("unexpected fixture version")
	}
	if len(c.Commands) == 0 || len(c.Rejected) == 0 || len(c.JCS) == 0 || len(c.Proofs) == 0 {
		t.Fatal("empty fixture group")
	}
	return c
}

func checkCanonical(t *testing.T, actual []byte, expected, digest string) {
	t.Helper()
	if string(actual) != expected {
		t.Fatalf("canonical bytes differ\nactual: %s\nexpected: %s", actual, expected)
	}
	h := sha256.Sum256(actual)
	if hex.EncodeToString(h[:]) != digest {
		t.Fatal("SHA-256 differs from frozen fixture")
	}
}

func fixtureMessage(t *testing.T, method string) proto.Message {
	t.Helper()
	service := ledgerv1.File_contracts_ledger_v1_ledger_proto.Services().ByName("Ledger")
	for i := 0; i < service.Methods().Len(); i++ {
		m := service.Methods().Get(i)
		if string(m.Name()) == method {
			typeOf, err := protoregistry.GlobalTypes.FindMessageByName(m.Input().FullName())
			if err != nil {
				t.Fatal(err)
			}
			return typeOf.New().Interface()
		}
	}
	t.Fatalf("fixture names unknown method %s", method)
	return nil
}

// This probe stops before authorization-policy evaluation and financial state
// loading. It exercises the real interceptor, RPC mapping, validation and material
// generation, not durability. Existing real-adapter suites supply that evidence.
type canonicalProbe struct {
	application.Store
	application.Transaction
	command   *application.Command
	existing  *application.Operation
	book      domain.ID
	operation domain.ID
	entered   bool
}

func (p *canonicalProbe) Transact(ctx context.Context, f func(application.Transaction) (application.Receipt, error)) (application.Receipt, error) {
	p.entered = true
	return f(p)
}
func (p *canonicalProbe) Operation(_ context.Context, book, operation domain.ID) (*application.Operation, error) {
	if book != p.book || operation != p.operation {
		return nil, nil
	}
	return p.existing, nil
}
func (p *canonicalProbe) Load(_ context.Context, c application.Command, _ time.Time) (application.State, error) {
	p.command = &c
	return application.State{}, application.ErrUnavailable
}

func invokeCanonical(t *testing.T, v canonicalVector, message proto.Message, p *canonicalProbe) error {
	t.Helper()
	// Unit-test peer state models a verified workload. Actual TLS handshakes and
	// identity denials are covered by the configured app's network integration tests.
	identity := "spiffe://pesaro.test/canonical-fixtures"
	uri, _ := url.Parse(identity)
	cert := &x509.Certificate{URIs: []*url.URL{uri}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}}})
	ctx = metadata.NewIncomingContext(ctx, metadata.New(v.Metadata))
	books := map[domain.ID]bool{}
	// Give the probe scope for each synthetic book; no caller-controlled authority
	// is configured in a running service by this test helper.
	for _, c := range loadCanonicalCorpus(t).Commands {
		var material struct {
			Book domain.ID `json:"book_id"`
		}
		if err := json.Unmarshal([]byte(c.Canonical), &material); err != nil {
			t.Fatal(err)
		}
		books[material.Book] = true
	}
	auth := NewAuthenticator(map[string]application.Caller{identity: {Identity: identity, Books: books, Permissions: map[string]bool{"provision": true, "control": true, "spend": true, "resolve": true}}})
	server := New(application.New(p, application.Trust{}, nil))
	for _, method := range ledgerv1.Ledger_ServiceDesc.Methods {
		if method.MethodName == v.Method {
			_, err := method.Handler(server, ctx, func(out any) error { proto.Merge(out.(proto.Message), message); return nil }, grpc.UnaryServerInterceptor(auth.Unary))
			return err
		}
	}
	t.Fatal("method missing from generated service")
	return nil
}

func reverseProtobufFields(t *testing.T, data []byte) []byte {
	t.Helper()
	var fields [][]byte
	for len(data) != 0 {
		_, _, n := protowire.ConsumeField(data)
		if n < 0 {
			t.Fatal("invalid frozen wire bytes")
		}
		fields = append(fields, data[:n])
		data = data[n:]
	}
	var out []byte
	for i := len(fields) - 1; i >= 0; i-- {
		out = append(out, fields[i]...)
	}
	return out
}

func TestCanonicalCommandFixtures(t *testing.T) {
	corpus := loadCanonicalCorpus(t)
	byID := map[string]canonicalVector{}
	methods := map[string]bool{}
	for _, v := range corpus.Commands {
		if _, exists := byID[v.ID]; exists {
			t.Fatalf("duplicate fixture %s", v.ID)
		}
		byID[v.ID] = v
		methods[v.Method] = true
	}
	if len(methods) != 7 {
		t.Fatal("fixtures must cover all seven write commands")
	}
	for _, v := range corpus.Commands {
		t.Run(v.ID, func(t *testing.T) {
			if v.Same != "" && (v.Canonical != byID[v.Same].Canonical || v.SHA256 != byID[v.Same].SHA256) {
				t.Fatal("equivalence fixture changed material")
			}
			if v.Different != "" && (v.Canonical == byID[v.Different].Canonical || v.SHA256 == byID[v.Different].SHA256) {
				t.Fatal("material change did not change identity")
			}
			binary, err := hex.DecodeString(v.Protobuf)
			if err != nil || len(binary) == 0 {
				t.Fatal("missing or invalid frozen protobuf")
			}
			for _, encoding := range []string{"json", "protobuf", "reordered-protobuf"} {
				t.Run(encoding, func(t *testing.T) {
					message := fixtureMessage(t, v.Method)
					var err error
					switch encoding {
					case "json":
						err = protojson.Unmarshal([]byte(v.JSON), message)
					case "protobuf":
						err = proto.Unmarshal(binary, message)
					default:
						reordered := reverseProtobufFields(t, binary)
						if bytes.Equal(binary, reordered) {
							t.Fatal("field order probe did not change wire encoding")
						}
						err = proto.Unmarshal(reordered, message)
					}
					if err != nil {
						t.Fatal(err)
					}
					probe := &canonicalProbe{}
					if err := invokeCanonical(t, v, message, probe); status.Code(err) != codes.Unavailable || probe.command == nil {
						t.Fatalf("did not reach material boundary: %v", err)
					}
					actual, err := probe.command.Material()
					if err != nil {
						t.Fatal(err)
					}
					checkCanonical(t, actual, v.Canonical, v.SHA256)
					// Same identity and frozen material must replay; changing that material
					// conflicts before loading balances, even if claims/proofs have expired.
					original := v
					want := codes.OK
					if v.Same != "" {
						original = byID[v.Same]
					}
					if v.Different != "" {
						original = byID[v.Different]
						want = codes.AlreadyExists
					}
					var request struct {
						Envelope struct {
							Book      domain.ID `json:"book_id"`
							Operation domain.ID `json:"operation_id"`
						} `json:"envelope"`
					}
					if err := json.Unmarshal([]byte(original.JSON), &request); err != nil {
						t.Fatal(err)
					}
					newIdentity := probe.command.BookID != request.Envelope.Book || probe.command.OperationID != request.Envelope.Operation
					if newIdentity {
						want = codes.Unavailable
					} // no receipt at a different scoped key
					probe = &canonicalProbe{book: request.Envelope.Book, operation: request.Envelope.Operation, existing: &application.Operation{Canonical: []byte(original.Canonical)}}
					if err := invokeCanonical(t, v, message, probe); status.Code(err) != want || (probe.command != nil) != newIdentity {
						t.Fatalf("replay/conflict boundary: %v, want %v", err, want)
					}
				})
			}
		})
	}
}

func TestCanonicalRejectedFixtures(t *testing.T) {
	for _, v := range loadCanonicalCorpus(t).Rejected {
		t.Run(v.ID, func(t *testing.T) {
			if v.JSON == "" && v.Protobuf == "" {
				t.Fatal("empty rejection fixture")
			}
			for _, encoding := range []string{"json", "protobuf"} {
				if encoding == "json" && v.JSON == "" || encoding == "protobuf" && v.Protobuf == "" {
					continue
				}
				t.Run(encoding, func(t *testing.T) {
					message := fixtureMessage(t, v.Method)
					var err error
					if encoding == "json" {
						err = protojson.Unmarshal([]byte(v.JSON), message)
					} else {
						var data []byte
						data, err = hex.DecodeString(v.Protobuf)
						if err != nil {
							t.Fatal(err)
						}
						err = proto.Unmarshal(data, message)
					}
					if err != nil {
						return
					}
					probe := &canonicalProbe{}
					if err := invokeCanonical(t, v, message, probe); status.Code(err) != codes.InvalidArgument || probe.entered {
						t.Fatalf("invalid request reached transaction: %v", err)
					}
				})
			}
		})
	}
}

func TestCanonicalJCSFixtures(t *testing.T) {
	for _, v := range loadCanonicalCorpus(t).JCS {
		t.Run(v.ID, func(t *testing.T) {
			data, err := jsoncanonicalizer.Transform([]byte(v.Input))
			if v.Reject {
				if err == nil {
					t.Fatal("invalid JCS accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			checkCanonical(t, data, v.Canonical, v.SHA256)
		})
	}
}

func TestCanonicalProofFixtures(t *testing.T) {
	corpus := loadCanonicalCorpus(t)
	seed, err := hex.DecodeString(corpus.Seed)
	if err != nil || len(seed) != ed25519.SeedSize {
		t.Fatal("invalid synthetic seed")
	}
	key := ed25519.NewKeyFromSeed(seed)
	if hex.EncodeToString(key.Public().(ed25519.PublicKey)) != corpus.Public {
		t.Fatal("synthetic public key mismatch")
	}
	for _, v := range corpus.Proofs {
		t.Run(v.ID, func(t *testing.T) {
			var claims any
			var signature string
			switch v.Purpose {
			case "grant":
				var g application.Grant
				if err := json.Unmarshal([]byte(v.Claims), &g); err != nil {
					t.Fatal(err)
				}
				signed, err := application.SignGrant(g, key)
				if err != nil {
					t.Fatal(err)
				}
				claims, signature = g, signed.Signature
			case "evidence":
				var e application.Evidence
				if err := json.Unmarshal([]byte(v.Claims), &e); err != nil {
					t.Fatal(err)
				}
				signed, err := application.SignEvidence(e, key)
				if err != nil {
					t.Fatal(err)
				}
				claims, signature = e, signed.Signature
			default:
				t.Fatal("unknown proof purpose")
			}
			data, err := application.Canonical(claims)
			if err != nil {
				t.Fatal(err)
			}
			message := append([]byte("pesaro.ledger/"+v.Purpose+"/v1\n"), data...)
			checkCanonical(t, message, v.Message, v.SHA256)
			if signature != v.Signature {
				t.Fatal("domain-separated Ed25519 signature mismatch")
			}
		})
	}
}
