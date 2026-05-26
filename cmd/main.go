package main

import (
	"log/slog"
	"os"

	"github.com/artyomstank/RWB_intern/internal/app"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	a, err := app.NewApp()
	if err != nil {
		slog.Error("failed to initialize app", "err", err)
		os.Exit(1)
	}

	if err := a.Run(); err != nil {
		slog.Error("service error", "err", err)
		os.Exit(1)
	}

	slog.Info("service stopped gracefully")
}
