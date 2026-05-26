package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/artyomstank/RWB_intern/internal/anomaly"
	"github.com/artyomstank/RWB_intern/internal/api"
	"github.com/artyomstank/RWB_intern/internal/config"
	"github.com/artyomstank/RWB_intern/internal/consumer"
	"github.com/artyomstank/RWB_intern/internal/metrics"
	"github.com/artyomstank/RWB_intern/internal/stoplist"
	"github.com/artyomstank/RWB_intern/internal/topn"
	"github.com/artyomstank/RWB_intern/internal/window"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := config.MustLoad()

	slog.Info("starting trending service",
		"http_addr", cfg.HTTP.Addr,
		"kafka_brokers", cfg.Kafka.Brokers,
		"kafka_topic", cfg.Kafka.Topic,
		"topn_size", cfg.TopN.N,
		"topn_refresh", cfg.TopN.RefreshTTL,
	)

	// ── Prometheus ────────────────────────────────────────────────────────────

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	m := metrics.New(reg)

	// ── Core components ───────────────────────────────────────────────────────

	win := window.New()
	detector := anomaly.New()

	sl, err := stoplist.New(cfg.StopList.DBPath)
	if err != nil {
		slog.Error("failed to open stoplist db", "err", err, "path", cfg.StopList.DBPath)
		os.Exit(1)
	}
	defer func() {
		if closeErr := sl.Close(); closeErr != nil {
			slog.Error("stoplist close error", "err", closeErr)
		}
	}()

	slog.Info("stoplist loaded", "words", len(sl.List()))
	m.StopListSize.Set(float64(len(sl.List())))

	// ── Top-N cache + background worker ──────────────────────────────────────

	cache := new(topn.Cache)
	worker := topn.NewWorker(win, sl, detector, cache, m, cfg.TopN.N, cfg.TopN.RefreshTTL)

	// ── Kafka consumer ────────────────────────────────────────────────────────

	cons, err := consumer.New(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
		cfg.Kafka.Group,
		win,
		m,
	)
	if err != nil {
		slog.Error("failed to create kafka consumer", "err", err)
		os.Exit(1)
	}
	defer cons.Close()

	// ── HTTP server ───────────────────────────────────────────────────────────

	promHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
	mux := api.NewMux(cache, sl, m, promHandler)
	srv := &http.Server{
		Addr:         cfg.HTTP.Addr,
		Handler:      mux,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
	}

	// ── Graceful shutdown ─────────────────────────────────────────────────────

	// signal.NotifyContext отменяет ctx при SIGINT / SIGTERM.
	// stop() возвращает дефолтное поведение сигналов — важно вызвать defer.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, gCtx := errgroup.WithContext(ctx)

	// HTTP: ListenAndServe блокирует до вызова Shutdown.
	g.Go(func() error {
		slog.Info("HTTP server listening", "addr", cfg.HTTP.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	// HTTP graceful shutdown: ждём отмены контекста, потом Shutdown.
	g.Go(func() error {
		<-gCtx.Done()
		slog.Info("shutting down HTTP server")
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	})

	// Kafka consumer: context.Canceled при штатной остановке не ошибка.
	g.Go(func() error {
		slog.Info("kafka consumer starting")
		if err := cons.Run(gCtx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	// Top-N worker: Run блокирует до отмены ctx, всегда возвращает nil.
	g.Go(func() error {
		slog.Info("top-N worker starting",
			"refresh", cfg.TopN.RefreshTTL,
			"size", cfg.TopN.N,
		)
		worker.Run(gCtx)
		return nil
	})

	if err := g.Wait(); err != nil {
		slog.Error("service error", "err", err)
		os.Exit(1)
	}

	slog.Info("service stopped gracefully")
}
