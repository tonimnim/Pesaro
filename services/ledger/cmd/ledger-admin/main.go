// ledger-admin is a separate, synthetic-only migration/fixture authority.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/store/cockroach"
)

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("ledger-admin", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	synthetic := flags.Bool("synthetic", false, "acknowledge synthetic local-only administration")
	action := flags.String("action", "", "migrate or fixture (each fixture creates a fresh isolated book)")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !*synthetic || (*action != "migrate" && *action != "fixture") {
		return errors.New("usage: ledger-admin -synthetic -action=migrate|fixture")
	}
	dsn := os.Getenv("PESAR_LEDGER_ADMIN_URL")
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil || u.User.Username() != "root" || u.Path != "/pesaro_ledger" {
		return errors.New("synthetic admin database configuration required")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return errors.New("synthetic administration requires a loopback database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := cockroach.Open(ctx, dsn)
	if err != nil {
		return errors.New("synthetic admin database unavailable")
	}
	defer store.Close()
	if *action == "migrate" {
		if store.Migrate(ctx) != nil || store.GrantRuntime(ctx) != nil {
			return errors.New("synthetic migration or grants failed")
		}
		_, err = fmt.Fprintln(out, "Ledger schema and restricted runtime grants ready.")
		return err
	}
	if store.Ready(ctx) != nil {
		return errors.New("apply migrations before creating a fixture")
	}
	fixture, err := store.BootstrapSynthetic(ctx)
	if err != nil {
		return errors.New("synthetic fixture creation failed")
	}
	return json.NewEncoder(out).Encode(fixture)
}
func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
