package main

import (
	"os"

	"github.com/tonimnim/Pesaro/internal/platform/service"
	"github.com/tonimnim/Pesaro/services/wallets/internal/app"
)

func main() {
	os.Exit(service.Process(app.Run))
}
