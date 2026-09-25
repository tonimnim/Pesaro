// ledger-verify reads an exported snapshot without Ledger database credentials.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/tonimnim/Pesaro/services/reconciliation/internal/verify"
	"os"
)

func main() {
	path := flag.String("snapshot", "", "path to complete snapshot JSON")
	flag.Parse()
	if *path == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: ledger-verify -snapshot=file.json")
		os.Exit(2)
	}
	file, err := os.Open(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot open snapshot")
		os.Exit(2)
	}
	defer file.Close()
	snapshot, err := verify.Decode(file)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	report := verify.Verify(snapshot)
	if json.NewEncoder(os.Stdout).Encode(report) != nil {
		os.Exit(2)
	}
	if !report.Valid {
		os.Exit(1)
	}
}
