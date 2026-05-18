// sipmesh-mock — in-memory OperatorAPI gRPC server for frontend
// test stands (Playwright, contract tests, local dev without a full
// engine).
//
// Wire-protocol contract sync with the real sipmesh engine is the
// non-negotiable design goal:
//
//   - Both sides import `sipmesh-common/gen/go/.../OperatorAPIServer`,
//     so adding a new RPC to the proto without updating this mock
//     fails the mock's build at the var-assertion in server.go. CI
//     on sipmesh-common runs `go build ./...`, so drift is caught
//     before tagging a release.
//
//   - WriteConfig diagnostics come from `sipmesh-common/validate`,
//     the same package the real engine uses. New validation rules
//     land in both places in lock-step (next sipmesh-common bump
//     pulls the new rule into every consumer).
//
// Usage:
//
//   sipmesh-mock --addr :50051
//
// Frontend Playwright tests typically run this as a sidecar container
// in their compose / k8s test pod:
//
//   ghcr.io/rromenskyi/sipmesh-mock:v0.x.y
//
// To seed canned state before a scenario, an HTTP side-channel will
// be added in a follow-up (POST /__seed/operator-config + POST
// /__reset). The CLI flags below carry placeholders for that
// endpoint already.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/rromenskyi/sipmesh-common/validate"
)

func main() {
	addr := flag.String("addr", ":50051", "gRPC listen address")
	seedAddr := flag.String("seed-addr", ":50052", "HTTP listen address for the test-only seed/admin endpoint (empty disables)")
	logLevelStr := flag.String("log-level", "info", "log level: debug | info | warn | error")
	flag.Parse()

	log := newLogger(*logLevelStr)
	log.Info("sipmesh-mock starting",
		"grpc_addr", *addr,
		"seed_addr", *seedAddr,
		"validate_package", "github.com/rromenskyi/sipmesh-common/validate",
	)

	// Sanity check — confirm the validate package compiles under
	// this binary's go.mod pin (a misuse of `go work` or a stale
	// replace directive would surface as a runtime panic on first
	// WriteConfig otherwise).
	_ = validate.OperatorConfig(nil)

	srv := newServer(log)

	grpcServer := grpc.NewServer()
	srv.register(grpcServer)
	reflection.Register(grpcServer) // grpcurl works out of the box

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("listen failed", "addr", *addr, "err", err)
		os.Exit(1)
	}
	log.Info("gRPC listener bound", "addr", lis.Addr().String())

	// HTTP seed/admin endpoint (test-only). Optional — empty
	// --seed-addr disables it for production-like security scans.
	var httpSrv *http.Server
	if *seedAddr != "" {
		httpSrv = newSeedServer(*seedAddr, srv, log)
		// Inform the gRPC server which host:port the seed HTTP
		// listener is on. GetCallArtifactURL synthesises URLs
		// rooted at that host so the browser can fetch the
		// /__artifact/<key> blob directly. The bind addr may be
		// ":50052" (any iface) — we leave the host portion empty
		// in that case so the URL ends up scheme-relative-ish
		// (`http://:50052/...`), which browsers in a docker
		// network resolve via the container hostname they
		// reached the mock on. Callers that need an explicit
		// hostname can pass --seed-addr=host:port.
		srv.setSeedHostHint(*seedAddr)
		go func() {
			log.Info("seed HTTP listener bound", "addr", *seedAddr)
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("seed HTTP server error", "err", err)
			}
		}()
	}

	// Graceful shutdown on SIGINT/SIGTERM. gRPC waits for in-flight
	// RPCs (bounded by the caller's context), seed HTTP server gets
	// a 5s drain window.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("gRPC serve failed", "err", err)
		}
	}()

	<-stop
	log.Info("shutdown signal received; draining")

	if httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = httpSrv.Shutdown(ctx)
		cancel()
	}
	grpcServer.GracefulStop()
	log.Info("shutdown complete")
}

func newLogger(levelStr string) *slog.Logger {
	var level slog.Level
	switch levelStr {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}
