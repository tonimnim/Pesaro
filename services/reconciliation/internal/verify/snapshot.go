// Package verify reconstructs accounting from the public snapshot artifact.
// It deliberately imports no Ledger domain, application or persistence code.
package verify

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

type Row map[string]any
type Snapshot struct {
	SchemaVersion string            `json:"schema_version"`
	Cut           string            `json:"cut"`
	BookID        string            `json:"book_id"`
	Tables        map[string][]Row  `json:"tables"`
	Digests       map[string]string `json:"digests"`
	Counts        map[string]int    `json:"counts"`
}
type Report struct {
	BookID           string   `json:"book_id"`
	Cut              string   `json:"cut"`
	Valid            bool     `json:"valid"`
	Accounts         int      `json:"accounts"`
	Journals         int      `json:"journals"`
	Operations       int      `json:"operations"`
	OutstandingHolds int      `json:"outstanding_holds"`
	Problems         []string `json:"problems"`
}
type checker struct {
	s      Snapshot
	report Report
}

func Decode(r io.Reader) (Snapshot, error) {
	data, err := io.ReadAll(io.LimitReader(r, (16<<20)+1))
	if err != nil || len(data) > 16<<20 {
		return Snapshot{}, errors.New("snapshot exceeds bounded export")
	}
	var s Snapshot
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err = d.Decode(&s); err != nil {
		return s, errors.New("invalid snapshot")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return s, errors.New("trailing snapshot data")
	}
	return s, nil
}
func (c *checker) bad(message string) {
	if len(c.report.Problems) < 100 {
		c.report.Problems = append(c.report.Problems, message)
	}
}
func str(r Row, key string) string { value, _ := r[key].(string); return value }
func obj(v any) Row {
	m, _ := v.(map[string]any)
	if m == nil {
		m, _ = v.(Row)
	}
	return m
}
func (c *checker) body(r Row, key string) Row {
	decoder := json.NewDecoder(strings.NewReader(str(r, key)))
	decoder.UseNumber()
	var result Row
	if decoder.Decode(&result) != nil || result == nil {
		c.bad("invalid JSON body in " + key)
		return Row{}
	}
	return result
}

var digits = regexp.MustCompile("^(0|[1-9][0-9]*)$")

func (c *checker) number(value string, maxDigits int) *big.Int {
	n, ok := new(big.Int).SetString(value, 10)
	if !ok || !digits.MatchString(value) || len(value) > maxDigits {
		c.bad("invalid exact integer")
		return new(big.Int)
	}
	return n
}
func integer(v any) int64 {
	switch x := v.(type) {
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return 0
}
func canonical(value any) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	b, err = jsoncanonicalizer.Transform(b)
	if err != nil {
		return nil
	}
	return b
}
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func equal(a, b any) bool    { return bytes.Equal(canonical(a), canonical(b)) }
func (c *checker) index(table string, keys ...string) map[string]Row {
	result := map[string]Row{}
	for _, r := range c.s.Tables[table] {
		var parts []string
		for _, key := range keys {
			value := str(r, key)
			if value == "" {
				c.bad("missing identity in " + table)
			}
			parts = append(parts, value)
		}
		id := strings.Join(parts, "/")
		if _, duplicate := result[id]; duplicate {
			c.bad("duplicate identity in " + table)
		}
		result[id] = r
	}
	return result
}

type totals struct{ debit, credit, held *big.Int }

func zero() totals { return totals{new(big.Int), new(big.Int), new(big.Int)} }
func add(m map[string]*big.Int, key string, n *big.Int) {
	if m[key] == nil {
		m[key] = new(big.Int)
	}
	m[key].Add(m[key], n)
}
func expect(m map[string]*big.Int, key string) *big.Int {
	if m[key] == nil {
		return new(big.Int)
	}
	return m[key]
}

// Verify checks the snapshot's internal accounting consistency. It does not prove
// provider settlement, signature issuance, snapshot authenticity or availability.
func Verify(s Snapshot) Report {
	c := &checker{s: s, report: Report{BookID: s.BookID, Cut: s.Cut, Problems: []string{}}}
	required := []string{"books", "accounts", "posting_policies", "account_balances", "spending_controls", "business_claims", "holds", "limit_usage", "journals", "journal_lines", "account_events", "control_events", "hold_events", "limit_events", "resolution_evidence", "financial_operations", "outbox_facts"}
	if s.SchemaVersion != "1" || s.BookID == "" || s.Cut == "" {
		c.bad("invalid snapshot identity")
	}
	for _, name := range required {
		rows, exists := s.Tables[name]
		if !exists || s.Counts[name] != len(rows) || digest(canonical(rows)) != s.Digests[name] {
			c.bad("table missing or manifest mismatch: " + name)
		}
		for _, row := range rows {
			if str(row, "book_id") != s.BookID {
				c.bad("cross-book row in " + name)
			}
		}
	}
	if len(s.Tables) != len(required) {
		c.bad("unexpected snapshot tables")
	}
	if len(s.Tables["books"]) != 1 {
		c.bad("expected exactly one book")
	} else {
		book := s.Tables["books"][0]
		if str(book, "currency") != "KES" || str(book, "scale") != "2" || str(book, "country") != "KE" || str(book, "synthetic") != "true" {
			c.bad("unsupported M1 book")
		}
	}
	accounts := c.index("accounts", "account_id")
	balances := c.index("account_balances", "account_id")
	journals := c.index("journals", "journal_id")
	ops := c.index("financial_operations", "operation_id")
	claims := c.index("business_claims", "payment_id")
	holds := c.index("holds", "hold_id")
	policies := c.index("posting_policies", "policy_id")
	c.report.Accounts = len(accounts)
	c.report.Journals = len(journals)
	c.report.Operations = len(ops)
	computed := map[string]totals{}
	for id, a := range accounts {
		if str(a, "owner_id") == "" {
			c.bad("account missing owner")
		}
		purpose := str(a, "purpose")
		if purpose != "WALLET" && purpose != "PROVIDER_POOL" && purpose != "FEE_INCOME" {
			c.bad("invalid account purpose")
		}
		computed[id] = zero()
	}
	receipts := map[string]Row{}
	materials := map[string]Row{}
	journalByOp := map[string]string{}
	for id, j := range journals {
		op := str(j, "operation_id")
		if journalByOp[op] != "" {
			c.bad("multiple journals for one financial operation")
		}
		journalByOp[op] = id
		if ops[op] == nil {
			c.bad("journal without operation")
		}
	}
	eventByOp := map[string]Row{}
	eventIDs := map[string]bool{}
	for _, event := range s.Tables["outbox_facts"] {
		op, id := str(event, "operation_id"), str(event, "event_id")
		if eventByOp[op] != nil || eventIDs[id] || id == "" {
			c.bad("duplicate outbox identity")
		}
		eventByOp[op] = event
		eventIDs[id] = true
		if ops[op] == nil || str(event, "event_type") != "LedgerOperationResolved" {
			c.bad("orphan or invalid financial event")
		}
	}
	for id, op := range ops {
		receipt := c.body(op, "receipt")
		receipts[id] = receipt
		b, err := hex.DecodeString(str(op, "canonical"))
		canonicalRequest, canonErr := jsoncanonicalizer.Transform(b)
		if err != nil || canonErr != nil || !bytes.Equal(b, canonicalRequest) || digest(b) != str(op, "request_hash") {
			c.bad("operation canonical hash mismatch")
		}
		decoder := json.NewDecoder(bytes.NewReader(b))
		decoder.UseNumber()
		var material Row
		if decoder.Decode(&material) != nil {
			c.bad("invalid material instruction")
		}
		materials[id] = material
		if str(material, "book_id") != s.BookID || str(material, "kind") != str(receipt, "kind") {
			c.bad("operation material scope/kind mismatch")
		}
		if str(receipt, "operation_id") != id || str(receipt, "book_id") != s.BookID || str(receipt, "request_hash") != str(op, "request_hash") || str(receipt, "outcome") != str(op, "outcome") {
			c.bad("operation receipt mismatch")
		}
		event := eventByOp[id]
		if event == nil || str(event, "event_id") != str(receipt, "event_id") || !equal(c.body(event, "body"), receipt) {
			c.bad("missing or mismatched financial outbox fact")
		}
		journal := str(receipt, "journal_id")
		if journal != journalByOp[id] {
			c.bad("receipt/journal association mismatch")
		}
		if journal != "" && (str(op, "outcome") != "APPLIED" || str(journals[journal], "template") != str(receipt, "kind")+"/v1") {
			c.bad("journal without matching applied template")
		}
		if str(op, "outcome") != "APPLIED" && str(op, "outcome") != "REJECTED" {
			c.bad("invalid financial outcome")
		}
		kind := str(receipt, "kind")
		body := obj(material["body"])
		terms := obj(body["terms"])
		if kind == "TRANSFER_INTERNAL" || kind == "RESERVE_PAYOUT" {
			payment := str(terms, "payment_id")
			claim := claims[payment]
			if claim == nil || str(receipt, "payment_id") != payment {
				c.bad("admission without business claim")
			}
			if str(op, "outcome") == "APPLIED" && str(claim, "admission_operation_id") != id {
				c.bad("duplicate applied business execution")
			}
		}
	}
	lines := map[string][]Row{}
	for _, line := range s.Tables["journal_lines"] {
		journal, account := str(line, "journal_id"), str(line, "account_id")
		if journals[journal] == nil || accounts[account] == nil {
			c.bad("orphan journal line")
			continue
		}
		n := c.number(str(line, "units"), 19)
		if n.Sign() <= 0 || !n.IsInt64() {
			c.bad("invalid journal line amount")
		}
		total := computed[account]
		switch str(line, "side") {
		case "DEBIT":
			total.debit.Add(total.debit, n)
		case "CREDIT":
			total.credit.Add(total.credit, n)
		default:
			c.bad("invalid journal direction")
		}
		lines[journal] = append(lines[journal], line)
	}
	for id, journal := range journals {
		rows := lines[id]
		sort.Slice(rows, func(i, j int) bool { return integer(rows[i]["ordinal"]) < integer(rows[j]["ordinal"]) })
		debit, credit := new(big.Int), new(big.Int)
		seen := map[string]bool{}
		text := s.BookID + "|KES|2\n"
		for i, line := range rows {
			account, side, units := str(line, "account_id"), str(line, "side"), str(line, "units")
			if integer(line["ordinal"]) != int64(i) || seen[account] {
				c.bad("invalid journal ordinal or duplicate account")
			}
			seen[account] = true
			n := c.number(units, 19)
			if side == "DEBIT" {
				debit.Add(debit, n)
			} else {
				credit.Add(credit, n)
			}
			text += account + "|" + side + "|" + units + "\n"
		}
		if len(rows) < 2 || len(rows) > 8 || integer(journal["line_count"]) != int64(len(rows)) || debit.Cmp(credit) != 0 || digest([]byte(text)) != str(journal, "digest") {
			c.bad("unbalanced, incomplete or altered journal")
		}
	}
	reserved, consumed := map[string]*big.Int{}, map[string]*big.Int{}
	for id, hold := range holds {
		h := c.body(hold, "terms")
		terms := obj(h["terms"])
		source, pool := str(terms, "source_id"), str(h, "pool_id")
		principal, fee := c.number(str(terms, "principal"), 19), c.number(str(terms, "fee"), 19)
		total := new(big.Int).Add(principal, fee)
		state := str(hold, "state")
		owner, bucket := str(h, "owner_id"), str(h, "bucket")
		if str(h, "id") != id || str(h, "attempt_id") != str(hold, "attempt_id") || str(terms, "payment_id") != str(hold, "payment_id") || str(accounts[source], "owner_id") != owner || str(accounts[source], "purpose") != "WALLET" || str(accounts[pool], "purpose") != "PROVIDER_POOL" {
			c.bad("hold identity/account mismatch")
		}
		if principal.Sign() <= 0 || !total.IsInt64() {
			c.bad("invalid hold amount")
		}
		policy := policies[str(terms, "policy_id")]
		if policy == nil || str(policy, "pool_id") != pool || str(policy, "fee_account_id") != str(h, "fee_account_id") || str(policy, "fee") != str(terms, "fee") {
			c.bad("hold policy mismatch")
		}
		claim := claims[str(hold, "payment_id")]
		if claim == nil || str(claim, "hold_id") != id || str(claim, "state") != state {
			c.bad("hold/business claim mismatch")
		}
		if state == "RESERVED" || state == "EXPOSED" {
			wallet, wok := computed[source]
			asset, pok := computed[pool]
			if wok && pok {
				wallet.held.Add(wallet.held, total)
				asset.held.Add(asset.held, principal)
			}
			add(reserved, owner+"/"+bucket, principal)
			c.report.OutstandingHolds++
		} else if state == "CAPTURED" {
			add(consumed, owner+"/"+bucket, principal)
		} else if state != "RELEASED" {
			c.bad("invalid hold state")
		}
	}
	zone, _ := time.LoadLocation("Africa/Nairobi")
	for id, op := range ops {
		receipt := receipts[id]
		if str(op, "outcome") == "APPLIED" && str(receipt, "kind") == "TRANSFER_INTERNAL" {
			terms := obj(obj(materials[id]["body"])["terms"])
			source := str(terms, "source_id")
			when, err := time.Parse(time.RFC3339Nano, str(receipt, "recorded_at"))
			if err != nil {
				c.bad("invalid transfer timestamp")
				continue
			}
			add(consumed, str(accounts[source], "owner_id")+"/"+when.In(zone).Format("2006-01-02"), c.number(str(terms, "principal"), 19))
		}
	}
	for _, claim := range claims {
		admission, result := str(claim, "admission_operation_id"), str(claim, "result_operation_id")
		if str(claim, "stage") != "EXECUTION" || ops[admission] == nil || ops[result] == nil || str(ops[admission], "outcome") != str(claim, "admission_outcome") || str(ops[admission], "request_hash") != str(claim, "material_hash") {
			c.bad("business claim lacks matching admission")
		}
		if str(receipts[admission], "payment_id") != str(claim, "payment_id") || str(receipts[result], "payment_id") != str(claim, "payment_id") {
			c.bad("business claim payment mismatch")
		}
	}
	for id, total := range computed {
		balance := balances[id]
		if balance == nil || str(balance, "purpose") != str(accounts[id], "purpose") || total.debit.Cmp(c.number(str(balance, "debits"), 38)) != 0 || total.credit.Cmp(c.number(str(balance, "credits"), 38)) != 0 || total.held.Cmp(c.number(str(balance, "held"), 38)) != 0 {
			c.bad("balance differs from journal/hold history")
		}
		posted := new(big.Int).Sub(total.credit, total.debit)
		if str(accounts[id], "purpose") == "PROVIDER_POOL" {
			posted.Sub(total.debit, total.credit)
		}
		if posted.Cmp(total.held) < 0 {
			c.bad("negative available funds")
		}
	}
	if len(balances) != len(accounts) {
		c.bad("orphan or missing balance")
	}
	usage := c.index("limit_usage", "owner_id", "bucket")
	for key, u := range usage {
		if expect(reserved, key).Cmp(c.number(str(u, "reserved"), 38)) != 0 || expect(consumed, key).Cmp(c.number(str(u, "consumed"), 38)) != 0 {
			c.bad("limit usage differs from admitted payments/holds")
		}
	}
	for key := range reserved {
		if usage[key] == nil {
			c.bad("missing reserved limit bucket")
		}
	}
	for key := range consumed {
		if usage[key] == nil {
			c.bad("missing consumed limit bucket")
		}
	}
	c.histories(ops, balances, holds, usage)
	c.templates(ops, receipts, materials, journals, lines, holds, accounts, policies)
	c.report.Valid = len(c.report.Problems) == 0
	return c.report
}

func (c *checker) histories(ops, balances, holds, usage map[string]Row) {
	// Validate contiguous, connected histories independently of mutable versions.
	specs := []struct {
		table   string
		keys    []string
		current map[string]Row
	}{
		{"account_events", []string{"account_id"}, balances},
		{"control_events", []string{"subject_kind", "subject_id"}, c.index("spending_controls", "subject_kind", "subject_id")},
		{"hold_events", []string{"hold_id"}, holds},
		{"limit_events", []string{"owner_id", "bucket"}, usage},
	}
	for _, spec := range specs {
		grouped := map[string][]Row{}
		for _, r := range c.s.Tables[spec.table] {
			var parts []string
			for _, k := range spec.keys {
				parts = append(parts, str(r, k))
			}
			id := strings.Join(parts, "/")
			if spec.current[id] == nil {
				c.bad("orphan history in " + spec.table)
			}
			if str(ops[str(r, "operation_id")], "outcome") != "APPLIED" {
				c.bad("history without applied operation")
			}
			grouped[id] = append(grouped[id], r)
		}
		for id, current := range spec.current {
			rows := grouped[id]
			sort.Slice(rows, func(i, j int) bool { return integer(rows[i]["version"]) < integer(rows[j]["version"]) })
			var prior Row
			for i, r := range rows {
				change := c.body(r, "body")
				before, after := obj(change["Before"]), obj(change["After"])
				if integer(r["version"]) != int64(i+1) || after == nil {
					c.bad("broken history sequence in " + spec.table)
				}
				if i == 0 && before != nil {
					c.bad("initial history has predecessor")
				}
				if i > 0 && !historyEqual(before, prior) {
					c.bad("disconnected history in " + spec.table)
				}
				version := integer(after["version"])
				if spec.table == "hold_events" {
					state := obj(after["state"])
					version = integer(state["Version"])
					next, old := str(state, "State"), str(obj(prior["state"]), "State")
					if i == 0 && next != "RESERVED" {
						c.bad("hold did not start reserved")
					}
					if i > 0 && !((old == "RESERVED" && (next == "EXPOSED" || next == "RELEASED")) || (old == "EXPOSED" && (next == "CAPTURED" || next == "RELEASED"))) {
						c.bad("invalid hold history transition")
					}
					if i > 0 {
						a, b := clone(after), clone(prior)
						delete(a, "state")
						delete(b, "state")
						if !equal(a, b) {
							c.bad("hold financial terms changed")
						}
					}
				}
				if version != int64(i+1) {
					c.bad("history body version mismatch")
				}
				prior = after
			}
			if len(rows) == 0 || integer(current["version"]) != int64(len(rows)) {
				c.bad("mutable version lacks history in " + spec.table)
				continue
			}
			switch spec.table {
			case "account_events":
				totals := obj(prior["totals"])
				if str(prior, "account_id") != str(current, "account_id") || str(totals, "Debits") != str(current, "debits") || str(totals, "Credits") != str(current, "credits") || str(totals, "Held") != str(current, "held") {
					c.bad("account history tip mismatch")
				}
			case "hold_events":
				if str(obj(prior["state"]), "State") != str(current, "state") {
					c.bad("hold history tip mismatch")
				}
				expected := c.body(current, "terms")
				tip := clone(prior)
				delete(tip, "state")
				if !equal(expected, tip) {
					c.bad("hold terms/history mismatch")
				}
			case "limit_events":
				values := obj(prior["values"])
				if str(values, "Reserved") != str(current, "reserved") || str(values, "Consumed") != str(current, "consumed") {
					c.bad("limit history tip mismatch")
				}
			case "control_events":
				if str(prior, "daily_cap") != str(current, "daily_cap") || str(prior, "epoch") != str(current, "epoch") || fmt.Sprint(prior["debit_frozen"]) != str(current, "debit_frozen") || fmt.Sprint(prior["credit_frozen"]) != str(current, "credit_frozen") {
					c.bad("control history tip mismatch")
				}
			}
		}
	}
}
func (c *checker) templates(ops, receipts, materials, journals map[string]Row, lines map[string][]Row, holds, accounts, policies map[string]Row) {
	evidence := c.index("resolution_evidence", "evidence_id")
	for _, r := range evidence {
		if digest(canonical(c.body(r, "body"))) != str(r, "digest") {
			c.bad("resolution evidence hash mismatch")
		}
	}
	for id, op := range ops {
		receipt, material := receipts[id], materials[id]
		if eid := str(receipt, "evidence_id"); eid != "" {
			proof := c.body(evidence[eid], "body")
			if evidence[eid] == nil || str(proof, "id") != eid || str(proof, "book_id") != c.s.BookID || str(proof, "payment_id") != str(receipt, "payment_id") || str(proof, "hold_id") != str(receipt, "hold_id") {
				c.bad("resolution receipt/evidence mismatch")
			}
		}
		if str(op, "outcome") != "APPLIED" {
			continue
		}
		journal := str(receipt, "journal_id")
		kind := str(receipt, "kind")
		expected := map[string]string{}
		entry := func(account, side string, units *big.Int) {
			if accounts[account] == nil {
				c.bad("posting template references missing account")
			}
			if units.Sign() > 0 {
				key := account + "/" + side
				if expected[key] != "" {
					c.bad("posting template aliases accounts")
				}
				expected[key] = units.String()
			}
		}
		switch kind {
		case "TRANSFER_INTERNAL":
			terms := obj(obj(material["body"])["terms"])
			principal, fee := c.number(str(terms, "principal"), 19), c.number(str(terms, "fee"), 19)
			policy := policies[str(terms, "policy_id")]
			if policy == nil || str(policy, "fee") != str(terms, "fee") || str(receipt, "policy_id") != str(terms, "policy_id") {
				c.bad("transfer fee policy mismatch")
			}
			entry(str(terms, "source_id"), "DEBIT", new(big.Int).Add(principal, fee))
			entry(str(terms, "beneficiary_id"), "CREDIT", principal)
			entry(str(policy, "fee_account_id"), "CREDIT", fee)
		case "CAPTURE_PAYOUT":
			h := holds[str(receipt, "hold_id")]
			fixed := c.body(h, "terms")
			terms := obj(fixed["terms"])
			proof := c.body(evidence[str(receipt, "evidence_id")], "body")
			if h == nil || str(h, "state") != "CAPTURED" || str(proof, "final_state") != "FINAL_SUCCESS" || str(proof, "attempt_id") != str(h, "attempt_id") || str(proof, "principal") != str(terms, "principal") || str(proof, "beneficiary_id") != str(terms, "beneficiary_id") || str(proof, "provider_account_id") != str(fixed, "provider_account_id") || str(proof, "capability_id") != str(fixed, "capability_id") {
				c.bad("capture lacks matching final evidence")
			}
			principal, fee := c.number(str(terms, "principal"), 19), c.number(str(terms, "fee"), 19)
			entry(str(terms, "source_id"), "DEBIT", new(big.Int).Add(principal, fee))
			entry(str(fixed, "pool_id"), "CREDIT", principal)
			entry(str(fixed, "fee_account_id"), "CREDIT", fee)
		case "FIXTURE_FUND":
			rows := lines[journal]
			units := str(material, "units")
			if len(rows) != 2 {
				c.bad("invalid synthetic funding template")
			}
			for _, line := range rows {
				purpose, side := str(accounts[str(line, "account_id")], "purpose"), str(line, "side")
				if str(line, "units") != units || !((purpose == "WALLET" && side == "CREDIT") || (purpose == "PROVIDER_POOL" && side == "DEBIT")) {
					c.bad("invalid synthetic funding posting")
				}
			}
			continue
		case "CREATE_ACCOUNT", "SET_ACCOUNT_CONTROL", "RESERVE_PAYOUT", "MARK_HOLD_EXPOSED", "RELEASE_PAYOUT":
			if journal != "" {
				c.bad("non-posting command created a journal")
			}
			continue
		default:
			c.bad("unknown operation kind")
			continue
		}
		if journal == "" || journals[journal] == nil || len(lines[journal]) != len(expected) {
			c.bad("missing or extra posting template lines")
			continue
		}
		for _, line := range lines[journal] {
			if expected[str(line, "account_id")+"/"+str(line, "side")] != str(line, "units") {
				c.bad("journal does not match authorized payment terms")
			}
		}
	}
}

func clone(r Row) Row {
	out := Row{}
	for k, v := range r {
		out[k] = v
	}
	return out
}
func historyEqual(a, b Row) bool {
	// SQL TIMESTAMPTZ persists microseconds; the original JSON can retain nanos.
	normalize := func(r Row) Row {
		out := clone(r)
		if state := obj(out["state"]); state != nil {
			state = clone(state)
			if when, err := time.Parse(time.RFC3339Nano, str(state, "ExpiresAt")); err == nil {
				state["ExpiresAt"] = when.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
			}
			out["state"] = state
		}
		return out
	}
	return equal(normalize(a), normalize(b))
}
