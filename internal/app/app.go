package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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

type App struct {
	cfg    config.Config
	srv    *http.Server
	cons   *consumer.Consumer
	sl     *stoplist.StopList
	worker *topn.Worker
	m      *metrics.Metrics
}

func NewApp() (*App, error) {
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
		return nil, fmt.Errorf("open stoplist db: %w", err)
	}

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
		sl.Close()
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

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

	return &App{
		cfg:    cfg,
		srv:    srv,
		cons:   cons,
		sl:     sl,
		worker: worker,
		m:      m,
	}, nil
}

func (a *App) Run() error {
	defer func() {
		a.cons.Close()
		if err := a.sl.Close(); err != nil {
			slog.Error("stoplist close error", "err", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		slog.Info("HTTP server listening", "addr", a.cfg.HTTP.Addr)
		if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	g.Go(func() error {
		<-gCtx.Done()
		slog.Info("shutting down HTTP server")
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return a.srv.Shutdown(shutCtx)
	})

	g.Go(func() error {
		slog.Info("kafka consumer starting")
		if err := a.cons.Run(gCtx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	g.Go(func() error {
		slog.Info("top-N worker starting",
			"refresh", a.cfg.TopN.RefreshTTL,
			"size", a.cfg.TopN.N,
		)
		a.worker.Run(gCtx)
		return nil
	})

	return g.Wait()
}
