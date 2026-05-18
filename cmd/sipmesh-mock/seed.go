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
	"encoding/base64"
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

	mux.HandleFunc("POST /__seed/calls", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Calls []json.RawMessage `json:"calls"`
		}
		if err := decodeJSON(r, &body); err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
		calls := make([]*sipmeshapiv1.CallSummary, 0, len(body.Calls))
		for _, raw := range body.Calls {
			var c sipmeshapiv1.CallSummary
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, &c); err != nil {
				httpError(w, http.StatusBadRequest, "call entry: "+err.Error())
				return
			}
			calls = append(calls, &c)
		}
		count := s.seedCalls(calls)
		log.Info("seed: calls replaced", "count", count)
		writeJSON(w, http.StatusOK, map[string]any{"seeded": count})
	})

	mux.HandleFunc("POST /__seed/workers", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Workers []json.RawMessage `json:"workers"`
		}
		if err := decodeJSON(r, &body); err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
		workers := make([]*sipmeshapiv1.WorkerSummaryV2, 0, len(body.Workers))
		for _, raw := range body.Workers {
			var x sipmeshapiv1.WorkerSummaryV2
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, &x); err != nil {
				httpError(w, http.StatusBadRequest, "worker entry: "+err.Error())
				return
			}
			workers = append(workers, &x)
		}
		count := s.seedWorkers(workers)
		log.Info("seed: workers replaced", "count", count)
		writeJSON(w, http.StatusOK, map[string]any{"seeded": count})
	})

	mux.HandleFunc("POST /__seed/ai-workers", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Workers []json.RawMessage `json:"workers"`
		}
		if err := decodeJSON(r, &body); err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
		pools := make([]*sipmeshapiv1.AIWorkerCapability, 0, len(body.Workers))
		for _, raw := range body.Workers {
			var p sipmeshapiv1.AIWorkerCapability
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, &p); err != nil {
				httpError(w, http.StatusBadRequest, "ai-worker entry: "+err.Error())
				return
			}
			pools = append(pools, &p)
		}
		count := s.seedAIWorkers(pools)
		log.Info("seed: ai-workers replaced", "count", count)
		writeJSON(w, http.StatusOK, map[string]any{"seeded": count})
	})

	mux.HandleFunc("POST /__seed/events", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []json.RawMessage `json:"events"`
		}
		if err := decodeJSON(r, &body); err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
		events := make([]*sipmeshapiv1.Event, 0, len(body.Events))
		for _, raw := range body.Events {
			var e sipmeshapiv1.Event
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, &e); err != nil {
				httpError(w, http.StatusBadRequest, "event entry: "+err.Error())
				return
			}
			events = append(events, &e)
		}
		count := s.seedEvents(events)
		log.Info("seed: events replaced", "count", count)
		writeJSON(w, http.StatusOK, map[string]any{"seeded": count})
	})

	mux.HandleFunc("POST /__seed/call-archive", func(w http.ResponseWriter, r *http.Request) {
		// Body shape:
		//   {
		//     "calls": [ CallArchiveSummary, ... ],
		//     "artifacts": {
		//       "<call_id>:<kind-enum-name>": {
		//         "body_base64": "...",
		//         "content_type": "audio/wav"
		//       }
		//     }
		//   }
		// kind-enum-name is the proto string form
		// ("CALL_ARTIFACT_RECORDING" etc), matching what
		// artifactKey() emits at runtime so tests can pre-compute
		// the URL the mock will return.
		var body struct {
			Calls     []json.RawMessage `json:"calls"`
			Artifacts map[string]struct {
				BodyBase64  string `json:"body_base64"`
				ContentType string `json:"content_type"`
			} `json:"artifacts"`
		}
		if err := decodeJSON(r, &body); err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
		rows := make([]*sipmeshapiv1.CallArchiveSummary, 0, len(body.Calls))
		for _, raw := range body.Calls {
			var c sipmeshapiv1.CallArchiveSummary
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, &c); err != nil {
				httpError(w, http.StatusBadRequest, "archive entry: "+err.Error())
				return
			}
			rows = append(rows, &c)
		}
		artifacts := make(map[string]archiveArtifact, len(body.Artifacts))
		for key, a := range body.Artifacts {
			decoded, err := base64.StdEncoding.DecodeString(a.BodyBase64)
			if err != nil {
				httpError(w, http.StatusBadRequest, "artifact "+key+": invalid base64: "+err.Error())
				return
			}
			artifacts[key] = archiveArtifact{
				body:        decoded,
				contentType: a.ContentType,
			}
		}
		archiveCount, artifactCount := s.seedCallArchive(rows, artifacts)
		log.Info("seed: call-archive replaced",
			"archive_rows", archiveCount,
			"artifacts", artifactCount)
		writeJSON(w, http.StatusOK, map[string]any{
			"archive_rows": archiveCount,
			"artifacts":    artifactCount,
		})
	})

	// /__artifact/<key> — serves the raw bytes seeded via
	// /__seed/call-archive. Key shape: "<call_id>:<KIND_ENUM_NAME>".
	// The URL is what GetCallArtifactURL returns; <audio> tags and
	// curl alike fetch this directly.
	mux.HandleFunc("GET /__artifact/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		body, ct, ok := s.getArtifact(key)
		if !ok {
			httpError(w, http.StatusNotFound, "artifact "+key+" not seeded")
			return
		}
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
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

// decodeJSON reads at most 1 MiB of request body and decodes it into
// dst. Used by the live-state seed endpoints which carry envelopes of
// `{"calls": [...], ...}` rather than a single top-level proto
// message. (operator-config seeds protojson directly via
// readOperatorConfig.)
func decodeJSON(r *http.Request, dst any) error {
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 1<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, dst)
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
