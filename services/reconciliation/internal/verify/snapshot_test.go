package verify

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func fixture(t *testing.T, name string) Snapshot {
	t.Helper()
	f, err := os.Open("testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s, err := Decode(f)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func refresh(s *Snapshot) {
	for table, rows := range s.Tables {
		s.Digests[table] = digest(canonical(rows))
		s.Counts[table] = len(rows)
	}
}
func TestIndependentRealDatabaseSnapshots(t *testing.T) {
	for _, name := range []string{"transfer", "capture", "release"} {
		t.Run(name, func(t *testing.T) {
			r := Verify(fixture(t, name))
			if !r.Valid {
				t.Fatal(r.Problems)
			}
		})
	}
}
func TestRejectCorruptionEvenWithRecomputedManifest(t *testing.T) {
	cases := map[string]func(*Snapshot){
		"changed balance":         func(s *Snapshot) { s.Tables["account_balances"][0]["held"] = "1" },
		"missing financial event": func(s *Snapshot) { s.Tables["outbox_facts"] = s.Tables["outbox_facts"][1:] },
		"missing journal line":    func(s *Snapshot) { s.Tables["journal_lines"] = s.Tables["journal_lines"][1:] },
		"balanced duplicate journal": func(s *Snapshot) {
			j := clone(s.Tables["journals"][0])
			old := str(j, "journal_id")
			j["journal_id"] = "11111111-1111-4111-8111-111111111111"
			s.Tables["journals"] = append(s.Tables["journals"], j)
			for _, line := range s.Tables["journal_lines"] {
				if str(line, "journal_id") == old {
					copy := clone(line)
					copy["journal_id"] = j["journal_id"]
					s.Tables["journal_lines"] = append(s.Tables["journal_lines"], copy)
				}
			}
		},
		"removed business claim": func(s *Snapshot) { s.Tables["business_claims"] = nil },
		"changed usage":          func(s *Snapshot) { s.Tables["limit_usage"][0]["consumed"] = "0" },
		"missing history":        func(s *Snapshot) { s.Tables["account_events"] = s.Tables["account_events"][1:] },
		"duplicate operation": func(s *Snapshot) {
			s.Tables["financial_operations"] = append(s.Tables["financial_operations"], clone(s.Tables["financial_operations"][0]))
		},
		"cross-book row":   func(s *Snapshot) { s.Tables["accounts"][0]["book_id"] = "another-book" },
		"altered evidence": func(s *Snapshot) { s.Tables["resolution_evidence"][0]["digest"] = strings.Repeat("0", 64) },
		"reopened hold":    func(s *Snapshot) { s.Tables["holds"][0]["state"] = "RESERVED" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := fixture(t, "capture")
			mutate(&s)
			refresh(&s)
			if r := Verify(s); r.Valid {
				t.Fatal("corruption accepted")
			}
		})
	}
}
func TestDecodeLimitsAndInvalidManifest(t *testing.T) {
	for _, data := range []string{`{} {}`, `{"schema_version":"1","unexpected":true}`, strings.Repeat(" ", (16<<20)+1)} {
		if _, err := Decode(strings.NewReader(data)); err == nil {
			t.Fatal("invalid artifact accepted")
		}
	}
	s := fixture(t, "transfer")
	s.Digests["accounts"] = strings.Repeat("0", 64)
	if Verify(s).Valid {
		t.Fatal("bad digest accepted")
	}
	s = fixture(t, "transfer")
	b, _ := json.Marshal(s)
	if _, err := Decode(strings.NewReader(string(b))); err != nil {
		t.Fatal(err)
	}
}
