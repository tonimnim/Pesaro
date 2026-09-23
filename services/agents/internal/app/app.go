// Package app is the private composition root for Agents.
package app

import (
	"context"

	"github.com/tonimnim/Pesaro/internal/platform/service"
)

// Run currently serves operational scaffolding only; business readiness is false.
func Run(ctx context.Context) error {
	return service.Run(ctx, service.Definition{Name: "agents", DefaultPort: 8108})
}
