package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mr-dadong/Xift/internal/daemon"
	"github.com/mr-dadong/Xift/internal/storage"
)

func main() {
	var (
		dataDir  string
		endpoint string
	)
	flag.StringVar(&dataDir, "data-dir", storage.DefaultDataDir(), "index data directory")
	flag.StringVar(&endpoint, "endpoint", "", "IPC endpoint override (default: per-user local endpoint)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := daemon.Run(ctx, daemon.Config{
		DataDir:  dataDir,
		Endpoint: endpoint,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "xift-daemon: %v\n", err)
		os.Exit(1)
	}
}
