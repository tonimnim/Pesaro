// Package app is the private composition root for Reconciliation.
package app

import (
	"context"
	"os"

	"github.com/tonimnim/Pesaro/internal/platform/service"
)

// Run opts into the synthetic inbox only with explicit configuration.
func Run(ctx context.Context) error {
	if path := os.Getenv("PESAR_RECONCILIATION_CONFIG"); path != "" {
		return runEvents(ctx, path, os.Getenv("PESAR_RECONCILIATION_DATABASE_URL"))
	}
	return service.Run(ctx, service.Definition{Name: "reconciliation", DefaultPort: 8111})
}
