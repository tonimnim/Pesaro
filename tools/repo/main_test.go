package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A Git checkout intentionally excludes the local Markdown documentation.
func TestCodeChecksWorkWithoutLocalDocumentation(t *testing.T) {
	t.Chdir(t.TempDir())
	files := map[string]string{
		"go.mod":                              "module example.com/fixture\n\ngo 1.26.5\n",
		"services/catalog.json":               `[{"id":"ledger","name":"Ledger","port":8105,"stage":"scaffold","implementation_phase":"M1","planned_dependencies":[]}]`,
		"services/ledger/cmd/ledger/main.go":  "package main\n\nfunc main() {}\n",
		"services/ledger/internal/app/app.go": "package app\n",
	}
	for path, content := range files {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := run([]string{"check"}); err != nil {
		t.Fatalf("code-only checkout failed: %v", err)
	}
	if err := run([]string{"check-docs"}); err == nil || !strings.Contains(err.Error(), "SPEC.md") {
		t.Fatalf("explicit local documentation check must require specs: %v", err)
	}
	// Local documentation must not affect the checks used on GitHub.
	if err := os.WriteFile("README.md", []byte("[local draft](missing.md)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"check"}); err != nil {
		t.Fatalf("local Markdown changed code check behavior: %v", err)
	}
}
