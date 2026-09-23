package main

import (
	"os"

	"github.com/tonimnim/Pesaro/internal/platform/service"
	"github.com/tonimnim/Pesaro/services/risk/internal/app"
)

func main() {
	os.Exit(service.Process(app.Run))
}
