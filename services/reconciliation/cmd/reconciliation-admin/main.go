// reconciliation-admin owns synthetic private inbox schema administration.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tonimnim/Pesaro/services/reconciliation/internal/inbox"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "-synthetic" {
		fmt.Fprintln(os.Stderr, "usage: reconciliation-admin -synthetic")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := inbox.Open(ctx, os.Getenv("PESAR_RECONCILIATION_ADMIN_URL"), true)
	if err == nil {
		defer store.Close()
		err = store.Migrate(ctx)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "synthetic reconciliation migration unavailable")
		os.Exit(1)
	}
	fmt.Println("Reconciliation inbox schema and restricted runtime grants ready.")
}
