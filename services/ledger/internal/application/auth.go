package application

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

type Grant struct {
	ID            domain.ID     `json:"id"`
	Issuer        string        `json:"issuer"`
	BookID        domain.ID     `json:"book_id"`
	PaymentID     domain.ID     `json:"payment_id"`
	SubjectID     domain.ID     `json:"subject_id"`
	SourceID      domain.ID     `json:"source_id"`
	BeneficiaryID domain.ID     `json:"beneficiary_id"`
	Principal     domain.Amount `json:"principal"`
	Fee           domain.Amount `json:"fee"`
	PolicyID      domain.ID     `json:"policy_id"`
	QuoteID       domain.ID     `json:"quote_id"`
	Currency      string        `json:"currency"`
	SubjectEpoch  int64         `json:"subject_epoch,string"`
	AccountEpoch  int64         `json:"account_epoch,string"`
	NotBefore     time.Time     `json:"not_before"`
	ExpiresAt     time.Time     `json:"expires_at"`
}
type SignedGrant struct {
	Claims    Grant  `json:"claims"`
	Signature string `json:"signature"`
}
type Evidence struct {
	ID                domain.ID     `json:"id"`
	Issuer            string        `json:"issuer"`
	BookID            domain.ID     `json:"book_id"`
	PaymentID         domain.ID     `json:"payment_id"`
	HoldID            domain.ID     `json:"hold_id"`
	AttemptID         domain.ID     `json:"attempt_id"`
	ProviderAccountID domain.ID     `json:"provider_account_id"`
	BeneficiaryID     domain.ID     `json:"beneficiary_id"`
	Principal         domain.Amount `json:"principal"`
	Currency          string        `json:"currency"`
	CapabilityID      domain.ID     `json:"capability_id"`
	FinalState        string        `json:"final_state"`
	SubmissionFenced  bool          `json:"submission_fenced"`
	ObservedAt        time.Time     `json:"observed_at"`
	SourceDigest      string        `json:"source_digest"`
}
type SignedEvidence struct {
	Claims    Evidence `json:"claims"`
	Signature string   `json:"signature"`
}
type Trust struct {
	GrantKeys    map[string]ed25519.PublicKey
	EvidenceKeys map[string]ed25519.PublicKey
}

func Canonical(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return jsoncanonicalizer.Transform(data)
}
func hash(data []byte) string { s := sha256.Sum256(data); return hex.EncodeToString(s[:]) }
func proofMessage(purpose string, claims any) ([]byte, error) {
	canonical, err := Canonical(claims)
	if err != nil {
		return nil, err
	}
	return append([]byte("pesaro.ledger/"+purpose+"/v1\n"), canonical...), nil
}

// SignGrant and SignEvidence are used by authorized issuers/test harnesses, never
// called by the Ledger runtime to authorize its own customer spends.
func SignGrant(g Grant, key ed25519.PrivateKey) (SignedGrant, error) {
	m, e := proofMessage("grant", g)
	if e != nil {
		return SignedGrant{}, e
	}
	return SignedGrant{g, base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, m))}, nil
}
func SignEvidence(e Evidence, key ed25519.PrivateKey) (SignedEvidence, error) {
	m, err := proofMessage("evidence", e)
	if err != nil {
		return SignedEvidence{}, err
	}
	return SignedEvidence{e, base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, m))}, nil
}
func verifyProof(purpose string, claims any, signature string, key ed25519.PublicKey) bool {
	if len(key) != ed25519.PublicKeySize || len(signature) > 128 {
		return false
	}
	sig, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return false
	}
	m, err := proofMessage(purpose, claims)
	return err == nil && ed25519.Verify(key, m, sig)
}
func (t Trust) grantBinding(book domain.ID, terms Terms, owner domain.ID) bool {
	g := terms.Grant.Claims
	return g.ID.Valid() && verifyProof("grant", g, terms.Grant.Signature, t.GrantKeys[g.Issuer]) &&
		g.BookID == book && g.PaymentID == terms.PaymentID && g.SubjectID == owner && g.SourceID == terms.SourceID &&
		g.BeneficiaryID == terms.BeneficiaryID && g.Principal == terms.Principal && g.Fee == terms.Fee &&
		g.PolicyID == terms.PolicyID && g.QuoteID == terms.QuoteID && g.Currency == terms.Currency
}
func (t Trust) evidence(book domain.ID, h Hold, signed SignedEvidence, now time.Time) bool {
	e := signed.Claims
	digest, err := hex.DecodeString(e.SourceDigest)
	return e.ID.Valid() && verifyProof("evidence", e, signed.Signature, t.EvidenceKeys[e.Issuer]) &&
		e.BookID == book && e.PaymentID == h.Terms.PaymentID && e.HoldID == h.ID && e.AttemptID == h.AttemptID &&
		e.ProviderAccountID == h.ProviderAccountID && e.BeneficiaryID == h.Terms.BeneficiaryID &&
		e.Principal == h.Terms.Principal && e.Currency == h.Terms.Currency && e.CapabilityID == h.CapabilityID &&
		err == nil && len(digest) == 32 && !e.ObservedAt.IsZero() && !e.ObservedAt.After(now.Add(time.Minute)) &&
		(e.FinalState == "FINAL_SUCCESS" || (e.FinalState == "FINAL_FAILURE" && e.SubmissionFenced))
}
