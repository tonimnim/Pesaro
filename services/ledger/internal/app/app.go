// Package app is the private composition root for Ledger.
package app

import (
	"context"
	"os"

	"github.com/tonimnim/Pesaro/internal/platform/service"
)

// Run serves a configured synthetic Ledger, or the unconfigured health scaffold.
func Run(ctx context.Context) error {
	if path := os.Getenv("PESAR_LEDGER_CONFIG"); path != "" {
		return runConfigured(ctx, path, os.Getenv("PESAR_LEDGER_DATABASE_URL"))
	}
	return service.Run(ctx, service.Definition{Name: "ledger", DefaultPort: 8105})
}
