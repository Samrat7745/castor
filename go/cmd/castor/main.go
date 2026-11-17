// Command castor is the Go implementation of the Castor tuple store service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Samrat7745/castor/go/internal/config"
	"github.com/Samrat7745/castor/go/internal/httpapi"
	"github.com/Samrat7745/castor/go/internal/model"
	"github.com/Samrat7745/castor/go/internal/objstore"
	"github.com/Samrat7745/castor/go/internal/peer"
	"github.com/Samrat7745/castor/go/internal/pgstore"
	"github.com/Samrat7745/castor/go/internal/rediscache"
	"github.com/Samrat7745/castor/go/internal/service"
	"github.com/Samrat7745/castor/go/internal/stomp"
	"github.com/redis/go-redis/v9"
)

// version is set at build time (-ldflags "-X main.version=...").
var version = "0.3.0"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	if err := run(); err != nil {
		slog.Error("castor terminated", "err", err)
		os.Exit(1)
	}
}

// healthcheck is used by the container HEALTHCHECK (the image has no curl).
func healthcheck() int {
	port := os.Getenv("CASTOR_SERVICE_PORT")
	if port == "" {
		port = "10100"
	}
	c := http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get("http://127.0.0.1:" + port + "/actuator/health")
	if err != nil || resp.StatusCode != 200 {
		return 1
	}
	return 0
}

func logger(level string) *slog.Logger {
	lv := slog.LevelInfo
	switch level {
	case "DEBUG", "TRACE":
		lv = slog.LevelDebug
	case "WARN":
		lv = slog.LevelWarn
	case "ERROR":
		lv = slog.LevelError
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lv}))
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	log := logger(cfg.LogLevel)
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pg, err := pgstore.Open(ctx, pgstore.DSN(cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName))
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pg.Close()

	rdb := redis.NewClient(&redis.Options{Addr: net.JoinHostPort(cfg.RedisHost, fmt.Sprint(cfg.RedisPort))})
	defer rdb.Close()
	cache := rediscache.New(rdb, cfg.ReservationStore, cfg.ConsumptionStore, cfg.TelemetryTTLSec)

	tuples, err := objstore.New(ctx, cfg.MinioEndpoint, cfg.MinioBucket, cfg.MinioAccessKey, cfg.MinioSecretKey)
	if err != nil {
		return fmt.Errorf("minio: %w", err)
	}

	svc := &service.Service{
		S: service.Settings{IsMaster: cfg.IsMaster, InitialFragmentSize: cfg.InitialFragmentSize,
			ReservationTimeout: time.Duration(cfg.ReservationTimeout) * time.Millisecond,
			RetryDelay:         time.Duration(cfg.RetryDelay) * time.Millisecond},
		PG: pg, Cache: cache, Tuples: tuples, Log: log,
	}
	if cfg.IsMaster && len(cfg.SlaveURIs) > 0 {
		// Java refuses to start a master without slaves; Go tolerates a standalone master.
		pc, err := peer.New(cfg.SlaveURIs, cfg.NoSSLValidation, cfg.TrustedCertificates, 2*time.Minute)
		if err != nil {
			return err
		}
		svc.Peers = pc
	} else if cfg.IsMaster {
		log.Warn("IS_MASTER=true but SLAVE_URIS is empty; running master without slaves")
		svc.Peers = noPeers{}
	}

	mux := http.NewServeMux()
	ws := stomp.NewServer(stomp.Config{MaxMessage: int(cfg.MessageBuffer),
		ServerSendMs: int(cfg.ServerHeartbeat), ServerReceiveMs: int(cfg.ClientHeartbeat)}, svc.HandleUpload, log)
	mux.Handle("/intra-vcp/ws", ws)

	h := httpapi.New(svc, []httpapi.HealthCheck{pg.Ping, cache.Ping}, httpapi.Info{
		Name: "castor-service", Description: "@project.description@", GroupID: "io.carbynestack", ArtifactID: "castor-service", Version: version}, mux, log)

	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Port), Handler: h, ReadHeaderTimeout: 30 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("castor listening", "port", cfg.Port, "master", cfg.IsMaster, "slaves", strings.Join(cfg.SlaveURIs, ","))
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		sc, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(sc)
	}
	return nil
}

// noPeers lets a slave-less master work (reservation sharing trivially succeeds).
type noPeers struct{}

func (noPeers) ShareReservation(context.Context, *model.Reservation) (bool, error) { return true, nil }

func (noPeers) UpdateReservationStatus(context.Context, string, model.ActivationStatus) error {
	return nil
}
