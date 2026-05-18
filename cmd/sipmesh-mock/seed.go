// seed.go — HTTP side-channel for test fixtures.
//
// Why HTTP and not gRPC: this is a test-only admin surface, not part
// of the OperatorAPI contract. Keeping it on a separate port + HTTP
// makes it visible/scrubable in deploy configs ("the mock has a
// test endpoint on :50052, never expose this in prod"). gRPC
// reflection on the main port stays clean of test-only RPCs that
// would otherwise pollute frontend tooling auto-discovery.
//
// Endpoints:
//
//   POST /__reset
//     Wipes all in-memory state. 204 No Content on success. Body
//     ignored. Use at the top of every Playwright scenario to
//     guarantee a known-empty starting point.
//
//   POST /__seed/operator-config
//     Body: protojson-encoded OperatorConfig. Wholesale-replaces the
//     default group's ConfigSet, bumps version, returns
//     `{"version": N, "etag": "..."}`. Skips validation so tests
//     CAN seed deliberately-invalid state if they need to exercise
//     downstream error paths. To validate the same payload BEFORE
//     seeding, the caller can POST to /__seed/dry-run-validate
//     first (returns ConfigDiagnostic list without storing).
//
//   POST /__seed/dry-run-validate
//     Body: protojson-encoded OperatorConfig. Returns the validator
//     diagnostics without storing anything. Useful when test code
//     wants to assert "this fixture would pass validation" as part
//     of a pre-flight check.

package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"

	sipmeshapiv1 "github.com/rromenskyi/sipmesh-common/gen/go/sipmesh/api/v1"
	"github.com/rromenskyi/sipmesh-common/validate"
)

func newSeedServer(addr string, s *server, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /__reset", func(w http.ResponseWriter, r *http.Request) {
		s.reset()
		log.Info("seed: state reset")
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /__seed/operator-config", func(w http.ResponseWriter, r *http.Request) {
		cfg, err := readOperatorConfig(r)
		if err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
		version, etag := s.seed(defaultGroup, cfg)
		log.Info("seed: operator-config replaced", "version", version)
		writeJSON(w, http.StatusOK, map[string]any{
			"version": version,
			"etag":    etag,
		})
	})

	mux.HandleFunc("POST /__seed/dry-run-validate", func(w http.ResponseWriter, r *http.Request) {
		cfg, err := readOperatorConfig(r)
		if err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
		diags := validate.OperatorConfig(cfg)
		out := make([]map[string]any, 0, len(diags))
		for _, d := range diags {
			out = append(out, map[string]any{
				"severity":   d.GetSeverity().String(),
				"message":    d.GetMessage(),
				"field_path": d.GetFieldPath(),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"diagnostics": out,
		})
	})

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}

func readOperatorConfig(r *http.Request) (*sipmeshapiv1.OperatorConfig, error) {
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 1<<20)) // 1 MiB cap
	if err != nil {
		return nil, err
	}
	var cfg sipmeshapiv1.OperatorConfig
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(body, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
