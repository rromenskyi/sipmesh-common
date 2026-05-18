// Package main — sipmesh-mock OperatorAPI gRPC server.
//
// This file implements the OperatorAPIServer interface backed by an
// in-memory store. Wire-protocol sync with the real sipmesh engine is
// guaranteed by:
//
//   1. Compile-time: the `var _ sipmeshapiv1.OperatorAPIServer = (*server)(nil)`
//      assertion at the bottom fails to build when a new RPC lands in
//      sipmesh-common without a corresponding handler here. CI on the
//      sipmesh-common repo runs `go build ./...`, so adding a proto
//      RPC without updating the mock breaks the build.
//
//   2. Behaviour-time: WriteConfig calls into the shared
//      sipmesh-common/validate package — the same function the real
//      engine uses. When validation rules tighten/relax, the mock
//      tracks in lock-step.
//
// What the mock does NOT cover (intentionally):
//
//   - Live state: ListCalls / ListWorkers / SubscribeEvents return
//     empty / Unimplemented today. A future companion HTTP seed
//     endpoint (see main.go's --seed-port flag, TODO) lets tests
//     populate canned data before each scenario.
//   - Side effects: there is no edge / proxy / Redis. Apply lands in
//     an in-memory ConfigSet and stays there until process exit (or
//     /__reset on the seed endpoint).
//   - Single-tenant operating model: `group` field on requests is
//     honoured (separate stores per group), but the mock has no
//     authn/authz; any caller can write to any group.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	sipmeshapiv1 "github.com/rromenskyi/sipmesh-common/gen/go/sipmesh/api/v1"
	"github.com/rromenskyi/sipmesh-common/validate"
)

// defaultGroup — single-tenant deploys use "" as the group key on
// WriteConfig/ImportConfig. The per-resource read RPCs
// (GetOperatorConfig / ListPipelines / ...) have no group field on
// the proto today; they implicitly target the default group.
const defaultGroup = ""

// configSet captures one ConfigSet revision in memory. version is
// monotonic per group; etag is a sha256 of the marshalled config body
// so HTTP-cache-style consumers can dedupe trivially.
type configSet struct {
	version uint64
	etag    string
	config  *sipmeshapiv1.OperatorConfig
}

// server is the in-memory OperatorAPI implementation. One server
// instance serves all groups; per-group state lives in `groups`
// keyed by group name (empty string for single-tenant).
//
// Live state (calls / workers / ai-workers) lives outside the group
// shape because the proto's per-resource read RPCs are flat — they
// have no group field today. Tests seed these via the HTTP side-
// channel before each scenario.
type server struct {
	sipmeshapiv1.UnimplementedOperatorAPIServer

	mu     sync.RWMutex
	groups map[string]*configSet

	// calls is keyed by CallSummary.internal_call_id. Order of
	// insertion is preserved via callOrder for stable ListCalls
	// output (tests assert specific ordering).
	calls     map[string]*sipmeshapiv1.CallSummary
	callOrder []string

	workers   []*sipmeshapiv1.WorkerSummaryV2
	aiWorkers []*sipmeshapiv1.AIWorkerCapability

	// events is the canned event sequence the next SubscribeEvents
	// stream emits. Frontend seeds the slice, mock yields each entry
	// in order then closes the stream (emit-then-EOF semantics —
	// enough for SSE-end-to-end tests that just assert delivery).
	events []*sipmeshapiv1.Event

	// archive is the canned call-archive list ListCallArchive
	// returns; artifacts is a (call_id, kind) → blob store the
	// `/__artifact/{key}` HTTP handler serves on GetCallArtifactURL
	// requests. Mock synthesises the URL pointing back at its own
	// seed HTTP server so the frontend can fetch the artefact
	// without S3 wiring.
	archive   []*sipmeshapiv1.CallArchiveSummary
	artifacts map[string]archiveArtifact

	// seedHostHint is the host:port main.go bound the HTTP seed
	// listener to (e.g. "127.0.0.1:50052"). GetCallArtifactURL
	// rewrites this into the response URL so the browser fetches
	// artefacts straight from the mock's HTTP port. Empty string
	// = HTTP seed server disabled at startup; GetCallArtifactURL
	// then returns FailedPrecondition (mirrors engine behaviour
	// when S3 archive isn't configured).
	seedHostHint string

	log *slog.Logger
}

// archiveArtifact pairs the raw bytes with the content type so the
// `/__artifact/{key}` HTTP handler can emit the right Content-Type
// header (browser cares for `<audio>` playback).
type archiveArtifact struct {
	body        []byte
	contentType string
}

// newServer constructs a mock with empty state. All groups start at
// version 0 with a zero-value OperatorConfig; first WriteConfig with
// a matching parent_version=0 advances to version 1.
func newServer(log *slog.Logger) *server {
	return &server{
		groups:    make(map[string]*configSet),
		calls:     make(map[string]*sipmeshapiv1.CallSummary),
		artifacts: make(map[string]archiveArtifact),
		log:       log,
	}
}

// setSeedHostHint records the host:port the HTTP seed server bound
// to. GetCallArtifactURL uses it as the host portion of the returned
// URL so the browser can fetch the seeded blob via the mock's own
// /__artifact handler. Pass an empty string when --seed-addr was
// disabled — the RPC then errors FailedPrecondition.
func (s *server) setSeedHostHint(hostPort string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seedHostHint = hostPort
}

// reset wipes all in-memory state. Exposed for the seed/admin HTTP
// endpoint so frontend Playwright tests can start each scenario with
// a known-empty backend.
func (s *server) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups = make(map[string]*configSet)
	s.calls = make(map[string]*sipmeshapiv1.CallSummary)
	s.callOrder = nil
	s.workers = nil
	s.aiWorkers = nil
	s.events = nil
	s.archive = nil
	s.artifacts = make(map[string]archiveArtifact)
}

// seedEvents replaces the canned SubscribeEvents queue.
func (s *server) seedEvents(events []*sipmeshapiv1.Event) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = nil
	for _, e := range events {
		s.events = append(s.events, proto.Clone(e).(*sipmeshapiv1.Event))
	}
	return len(s.events)
}

// seedCallArchive replaces the canned ListCallArchive page + the
// per-(call_id, kind) artifact blob store. The artifacts map's key
// shape is artifactKey(call_id, kind); each entry pairs the bytes
// the seed HTTP server returns with the content-type
// GetCallArtifactURL announces.
func (s *server) seedCallArchive(rows []*sipmeshapiv1.CallArchiveSummary, artifacts map[string]archiveArtifact) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.archive = nil
	for _, r := range rows {
		s.archive = append(s.archive, proto.Clone(r).(*sipmeshapiv1.CallArchiveSummary))
	}
	s.artifacts = make(map[string]archiveArtifact, len(artifacts))
	for k, a := range artifacts {
		s.artifacts[k] = archiveArtifact{
			body:        append([]byte(nil), a.body...),
			contentType: a.contentType,
		}
	}
	return len(s.archive), len(s.artifacts)
}

// artifactKey is the lookup key used by both the seed-time store
// and the per-request GetCallArtifactURL → /__artifact/<key> path.
// Stable encoding so tests can pre-compute the URL the mock will
// emit.
func artifactKey(callID string, kind sipmeshapiv1.CallArtifactKind) string {
	return callID + ":" + kind.String()
}

// getArtifact returns the seeded artifact for (call_id, kind), or
// (nil, "", false) when no such entry exists. Used by the HTTP
// /__artifact handler.
func (s *server) getArtifact(key string) ([]byte, string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.artifacts[key]
	if !ok {
		return nil, "", false
	}
	return append([]byte(nil), a.body...), a.contentType, true
}

// seedCalls wholesale-replaces the live calls fixture. Empty input
// = clear. Each call MUST have a non-empty internal_call_id; the
// helper drops entries violating that contract and returns the
// accepted count.
func (s *server) seedCalls(calls []*sipmeshapiv1.CallSummary) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = make(map[string]*sipmeshapiv1.CallSummary, len(calls))
	s.callOrder = s.callOrder[:0]
	for _, c := range calls {
		if c.GetInternalCallId() == "" {
			continue
		}
		s.calls[c.GetInternalCallId()] = proto.Clone(c).(*sipmeshapiv1.CallSummary)
		s.callOrder = append(s.callOrder, c.GetInternalCallId())
	}
	return len(s.callOrder)
}

// seedWorkers replaces the workers fixture.
func (s *server) seedWorkers(workers []*sipmeshapiv1.WorkerSummaryV2) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers = nil
	for _, w := range workers {
		s.workers = append(s.workers, proto.Clone(w).(*sipmeshapiv1.WorkerSummaryV2))
	}
	return len(s.workers)
}

// seedAIWorkers replaces the AI worker pools fixture.
func (s *server) seedAIWorkers(pools []*sipmeshapiv1.AIWorkerCapability) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aiWorkers = nil
	for _, p := range pools {
		s.aiWorkers = append(s.aiWorkers, proto.Clone(p).(*sipmeshapiv1.AIWorkerCapability))
	}
	return len(s.aiWorkers)
}

// seed wholesale-replaces a group's ConfigSet with the supplied
// config, bumping version and re-stamping etag. Skips validation so
// tests can also exercise "what happens when invalid state leaks
// past the validator" without the seed itself failing.
func (s *server) seed(group string, cfg *sipmeshapiv1.OperatorConfig) (uint64, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cs, ok := s.groups[group]
	if !ok {
		cs = &configSet{}
		s.groups[group] = cs
	}
	cs.version++
	cs.config = proto.Clone(cfg).(*sipmeshapiv1.OperatorConfig)
	cs.etag = computeETag(cs.config)
	return cs.version, cs.etag
}

func computeETag(cfg *sipmeshapiv1.OperatorConfig) string {
	if cfg == nil {
		return ""
	}
	// Deterministic marshal — proto's wire format is stable enough
	// for ETag purposes (within one process; consumers shouldn't
	// rely on cross-process stability since field ordering can
	// drift if message types are mutated).
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(cfg)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// GetOperatorConfig returns the current state of `group`. Empty
// group (single-tenant default) is the most common shape frontend
// tests hit.
func (s *server) GetOperatorConfig(ctx context.Context, req *sipmeshapiv1.GetOperatorConfigRequest) (*sipmeshapiv1.OperatorConfigResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cs, ok := s.groups[defaultGroup]
	if !ok {
		return &sipmeshapiv1.OperatorConfigResponse{
			Config: &sipmeshapiv1.OperatorConfig{
				Version: 0,
			},
		}, nil
	}
	return &sipmeshapiv1.OperatorConfigResponse{
		Config: cs.proto(),
	}, nil
}

// proto returns a cloned snapshot with version + applied_at stamped
// onto the OperatorConfig envelope — mirrors the engine's read
// behaviour.
func (cs *configSet) proto() *sipmeshapiv1.OperatorConfig {
	if cs == nil || cs.config == nil {
		return &sipmeshapiv1.OperatorConfig{}
	}
	out := proto.Clone(cs.config).(*sipmeshapiv1.OperatorConfig)
	out.Version = cs.version
	if out.AppliedAtIso == "" {
		out.AppliedAtIso = time.Now().UTC().Format(time.RFC3339)
	}
	return out
}

// WriteConfig applies a batch of ConfigOps atomically:
//
//   1. Clone current config (or empty if group has none yet)
//   2. Apply each op in order to the clone
//   3. Validate the merged result via validate.OperatorConfig
//   4. If any ERROR-severity diag → return InvalidArgument
//   5. If dry_run → return version=current + diagnostics + diff
//   6. Else store, bump version, return new state
//
// OCC: parent_version must match current group version. The engine
// rejects parent_version=0 for routine writes (use ImportConfig);
// this mock follows the same rule.
func (s *server) WriteConfig(ctx context.Context, req *sipmeshapiv1.WriteConfigRequest) (*sipmeshapiv1.WriteConfigResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group := req.GetGroup()
	cur, ok := s.groups[group]
	curVersion := uint64(0)
	if ok {
		curVersion = cur.version
	}

	if req.GetParentVersion() == 0 && curVersion != 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"parent_version=0 is rejected for routine writes; use ImportConfig for bootstrap")
	}
	if req.GetParentVersion() != curVersion {
		return &sipmeshapiv1.WriteConfigResponse{
			Version: curVersion,
		}, status.Errorf(codes.Aborted,
			"OCC: parent_version=%d does not match current=%d", req.GetParentVersion(), curVersion)
	}

	// Snapshot before for diff + no-op detection. Two clones — one
	// frozen for the diff baseline (before), one that applyOps can
	// mutate freely (the eventual merged result). Sharing the same
	// pointer would let in-place ops fool computeDiff into reporting
	// no changes.
	var before, working *sipmeshapiv1.OperatorConfig
	if cur != nil && cur.config != nil {
		before = proto.Clone(cur.config).(*sipmeshapiv1.OperatorConfig)
		working = proto.Clone(cur.config).(*sipmeshapiv1.OperatorConfig)
	} else {
		before = &sipmeshapiv1.OperatorConfig{}
		working = &sipmeshapiv1.OperatorConfig{}
	}

	merged, opErr := applyOps(working, req.GetOps())
	if opErr != nil {
		return nil, status.Errorf(codes.InvalidArgument, "apply ops: %v", opErr)
	}

	diags := validate.OperatorConfig(merged)
	hasError := false
	for _, d := range diags {
		if d.GetSeverity() == sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR {
			hasError = true
			break
		}
	}
	if hasError {
		return &sipmeshapiv1.WriteConfigResponse{
			Version:     curVersion,
			Etag:        etagOrEmpty(cur),
			Diagnostics: diags,
		}, status.Errorf(codes.InvalidArgument, "validation failed (%d diagnostics); first: %s @ %s",
			len(diags), diags[0].GetMessage(), diags[0].GetFieldPath())
	}

	diff := computeDiff(before, merged)
	noOp := len(diff.GetChanges()) == 0

	if req.GetDryRun() {
		return &sipmeshapiv1.WriteConfigResponse{
			Version:     curVersion,
			Etag:        etagOrEmpty(cur),
			DryRun:      true,
			NoOp:        noOp,
			Diagnostics: diags,
			Diff:        diff,
		}, nil
	}
	if noOp {
		return &sipmeshapiv1.WriteConfigResponse{
			Version:     curVersion,
			Etag:        etagOrEmpty(cur),
			NoOp:        true,
			Diagnostics: diags,
			Diff:        diff,
		}, nil
	}

	if cur == nil {
		cur = &configSet{}
		s.groups[group] = cur
	}
	cur.version++
	cur.config = merged
	cur.etag = computeETag(merged)

	return &sipmeshapiv1.WriteConfigResponse{
		Version:     cur.version,
		Etag:        cur.etag,
		Diagnostics: diags,
		Diff:        diff,
	}, nil
}

func etagOrEmpty(cs *configSet) string {
	if cs == nil {
		return ""
	}
	return cs.etag
}

// applyOps walks the ConfigOp slice and applies each to `cfg` in
// place. Returns the mutated cfg and any structural error (unknown
// op kind, missing required field). Resource semantics:
//
//   - upsert_pipeline: replace by name, append if absent
//   - upsert_trunk: replace by id, append if absent
//   - replace_routes: wholesale swap of routes list
//   - delete_pipeline / delete_trunk: filter out by name/id
func applyOps(cfg *sipmeshapiv1.OperatorConfig, ops []*sipmeshapiv1.ConfigOp) (*sipmeshapiv1.OperatorConfig, error) {
	for i, op := range ops {
		if op == nil {
			return nil, fmt.Errorf("op[%d] is nil", i)
		}
		switch v := op.GetOp().(type) {
		case *sipmeshapiv1.ConfigOp_UpsertPipeline:
			p := v.UpsertPipeline
			if p == nil || p.GetName() == "" {
				return nil, fmt.Errorf("op[%d] upsert_pipeline requires name", i)
			}
			replaced := false
			for j, existing := range cfg.GetPipelines() {
				if existing.GetName() == p.GetName() {
					cfg.Pipelines[j] = proto.Clone(p).(*sipmeshapiv1.Pipeline)
					replaced = true
					break
				}
			}
			if !replaced {
				cfg.Pipelines = append(cfg.Pipelines, proto.Clone(p).(*sipmeshapiv1.Pipeline))
			}
		case *sipmeshapiv1.ConfigOp_UpsertTrunk:
			t := v.UpsertTrunk
			if t == nil || t.GetId() == "" {
				return nil, fmt.Errorf("op[%d] upsert_trunk requires id", i)
			}
			replaced := false
			for j, existing := range cfg.GetTrunks() {
				if existing.GetId() == t.GetId() {
					cfg.Trunks[j] = proto.Clone(t).(*sipmeshapiv1.Trunk)
					replaced = true
					break
				}
			}
			if !replaced {
				cfg.Trunks = append(cfg.Trunks, proto.Clone(t).(*sipmeshapiv1.Trunk))
			}
		case *sipmeshapiv1.ConfigOp_ReplaceRoutes:
			if v.ReplaceRoutes == nil {
				cfg.Routes = nil
				continue
			}
			cfg.Routes = nil
			for _, r := range v.ReplaceRoutes.GetRoutes() {
				cfg.Routes = append(cfg.Routes, proto.Clone(r).(*sipmeshapiv1.Route))
			}
		case *sipmeshapiv1.ConfigOp_DeletePipeline:
			name := v.DeletePipeline
			cfg.Pipelines = filterPipelines(cfg.GetPipelines(), name)
		case *sipmeshapiv1.ConfigOp_DeleteTrunk:
			id := v.DeleteTrunk
			cfg.Trunks = filterTrunks(cfg.GetTrunks(), id)
		default:
			return nil, fmt.Errorf("op[%d] unknown variant %T (the mock probably needs an update to match a newer sipmesh-common revision)", i, v)
		}
	}
	return cfg, nil
}

func filterPipelines(in []*sipmeshapiv1.Pipeline, name string) []*sipmeshapiv1.Pipeline {
	if name == "" {
		return in
	}
	out := in[:0]
	for _, p := range in {
		if p.GetName() != name {
			out = append(out, p)
		}
	}
	return out
}

func filterTrunks(in []*sipmeshapiv1.Trunk, id string) []*sipmeshapiv1.Trunk {
	if id == "" {
		return in
	}
	out := in[:0]
	for _, t := range in {
		if t.GetId() != id {
			out = append(out, t)
		}
	}
	return out
}

// computeDiff produces a per-resource summary of changes between two
// OperatorConfigs. Used by WriteConfig responses so the frontend can
// render "what just happened" without a follow-up read.
func computeDiff(before, after *sipmeshapiv1.OperatorConfig) *sipmeshapiv1.ConfigDiff {
	out := &sipmeshapiv1.ConfigDiff{}

	beforePipes := map[string]*sipmeshapiv1.Pipeline{}
	for _, p := range before.GetPipelines() {
		beforePipes[p.GetName()] = p
	}
	afterPipes := map[string]*sipmeshapiv1.Pipeline{}
	for _, p := range after.GetPipelines() {
		afterPipes[p.GetName()] = p
	}
	for name, p := range afterPipes {
		old, ok := beforePipes[name]
		if !ok {
			out.Changes = append(out.Changes, &sipmeshapiv1.ResourceChange{
				ResourceType: "pipeline",
				Key:          name,
				Kind:         sipmeshapiv1.ResourceChange_CHANGE_KIND_ADDED,
			})
			continue
		}
		if !proto.Equal(old, p) {
			out.Changes = append(out.Changes, &sipmeshapiv1.ResourceChange{
				ResourceType: "pipeline",
				Key:          name,
				Kind:         sipmeshapiv1.ResourceChange_CHANGE_KIND_MODIFIED,
			})
		}
	}
	for name := range beforePipes {
		if _, ok := afterPipes[name]; !ok {
			out.Changes = append(out.Changes, &sipmeshapiv1.ResourceChange{
				ResourceType: "pipeline",
				Key:          name,
				Kind:         sipmeshapiv1.ResourceChange_CHANGE_KIND_REMOVED,
			})
		}
	}

	beforeTrunks := map[string]*sipmeshapiv1.Trunk{}
	for _, t := range before.GetTrunks() {
		beforeTrunks[t.GetId()] = t
	}
	afterTrunks := map[string]*sipmeshapiv1.Trunk{}
	for _, t := range after.GetTrunks() {
		afterTrunks[t.GetId()] = t
	}
	for id, t := range afterTrunks {
		old, ok := beforeTrunks[id]
		if !ok {
			out.Changes = append(out.Changes, &sipmeshapiv1.ResourceChange{
				ResourceType: "trunk",
				Key:          id,
				Kind:         sipmeshapiv1.ResourceChange_CHANGE_KIND_ADDED,
			})
			continue
		}
		if !proto.Equal(old, t) {
			out.Changes = append(out.Changes, &sipmeshapiv1.ResourceChange{
				ResourceType: "trunk",
				Key:          id,
				Kind:         sipmeshapiv1.ResourceChange_CHANGE_KIND_MODIFIED,
			})
		}
	}
	for id := range beforeTrunks {
		if _, ok := afterTrunks[id]; !ok {
			out.Changes = append(out.Changes, &sipmeshapiv1.ResourceChange{
				ResourceType: "trunk",
				Key:          id,
				Kind:         sipmeshapiv1.ResourceChange_CHANGE_KIND_REMOVED,
			})
		}
	}

	// Routes: list-level rather than per-route diff (routes have no
	// stable key — replace_routes is wholesale).
	if !proto.Equal(asRouteList(before.GetRoutes()), asRouteList(after.GetRoutes())) {
		out.Changes = append(out.Changes, &sipmeshapiv1.ResourceChange{
			ResourceType: "route",
			Kind:         sipmeshapiv1.ResourceChange_CHANGE_KIND_MODIFIED,
		})
	}

	sort.SliceStable(out.Changes, func(i, j int) bool {
		a, b := out.Changes[i], out.Changes[j]
		if a.ResourceType != b.ResourceType {
			return a.ResourceType < b.ResourceType
		}
		return a.Key < b.Key
	})
	return out
}

func asRouteList(in []*sipmeshapiv1.Route) *sipmeshapiv1.RouteList {
	return &sipmeshapiv1.RouteList{Routes: in}
}

// ImportConfig wholesale-replaces the group's ConfigSet. Forces
// validation but skips OCC when allow_force=true.
func (s *server) ImportConfig(ctx context.Context, req *sipmeshapiv1.ImportConfigRequest) (*sipmeshapiv1.ImportConfigResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group := req.GetGroup()
	cur, ok := s.groups[group]
	curVersion := uint64(0)
	if ok {
		curVersion = cur.version
	}

	if !req.GetAllowForce() && req.GetParentVersion() != curVersion {
		return &sipmeshapiv1.ImportConfigResponse{
			Version: curVersion,
		}, status.Errorf(codes.Aborted, "OCC: parent_version=%d does not match current=%d (or pass allow_force=true)",
			req.GetParentVersion(), curVersion)
	}

	cfg := req.GetConfig()
	if cfg == nil {
		cfg = &sipmeshapiv1.OperatorConfig{}
	}
	diags := validate.OperatorConfig(cfg)
	hasError := false
	for _, d := range diags {
		if d.GetSeverity() == sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR {
			hasError = true
			break
		}
	}
	if hasError {
		return &sipmeshapiv1.ImportConfigResponse{
			Version:     curVersion,
			Diagnostics: diags,
		}, status.Errorf(codes.InvalidArgument, "validation failed (%d diagnostics); first: %s @ %s",
			len(diags), diags[0].GetMessage(), diags[0].GetFieldPath())
	}

	var before *sipmeshapiv1.OperatorConfig
	if cur != nil {
		before = cur.config
	}
	diff := computeDiff(before, cfg)

	if req.GetDryRun() {
		return &sipmeshapiv1.ImportConfigResponse{
			Version:     curVersion,
			DryRun:      true,
			Diagnostics: diags,
			Diff:        diff,
		}, nil
	}

	if cur == nil {
		cur = &configSet{}
		s.groups[group] = cur
	}
	cur.version++
	cur.config = proto.Clone(cfg).(*sipmeshapiv1.OperatorConfig)
	cur.etag = computeETag(cur.config)

	return &sipmeshapiv1.ImportConfigResponse{
		Version:     cur.version,
		Etag:        cur.etag,
		Diagnostics: diags,
		Diff:        diff,
	}, nil
}

// -- Read views (per-resource projections) ----------------------------

func (s *server) ListPipelines(ctx context.Context, req *sipmeshapiv1.ListPipelinesRequest) (*sipmeshapiv1.ListPipelinesResponse, error) {
	cfg := s.snapshot(defaultGroup)
	return &sipmeshapiv1.ListPipelinesResponse{
		Pipelines: cfg.GetPipelines(),
	}, nil
}

func (s *server) GetPipeline(ctx context.Context, req *sipmeshapiv1.GetPipelineRequest) (*sipmeshapiv1.Pipeline, error) {
	cfg := s.snapshot(defaultGroup)
	for _, p := range cfg.GetPipelines() {
		if p.GetName() == req.GetName() {
			return p, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "pipeline %q not found", req.GetName())
}

func (s *server) ListTrunks(ctx context.Context, req *sipmeshapiv1.ListTrunksRequest) (*sipmeshapiv1.ListTrunksResponse, error) {
	cfg := s.snapshot(defaultGroup)
	return &sipmeshapiv1.ListTrunksResponse{
		Trunks: cfg.GetTrunks(),
	}, nil
}

func (s *server) GetTrunk(ctx context.Context, req *sipmeshapiv1.GetTrunkRequest) (*sipmeshapiv1.Trunk, error) {
	cfg := s.snapshot(defaultGroup)
	for _, t := range cfg.GetTrunks() {
		if t.GetId() == req.GetId() {
			return t, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "trunk %q not found", req.GetId())
}

func (s *server) ListRoutes(ctx context.Context, req *sipmeshapiv1.ListRoutesRequest) (*sipmeshapiv1.ListRoutesResponse, error) {
	cfg := s.snapshot(defaultGroup)
	return &sipmeshapiv1.ListRoutesResponse{
		Routes: cfg.GetRoutes(),
	}, nil
}

// snapshot returns a cheap read-lock-protected reference to the
// group's current config. Returns an empty OperatorConfig if the
// group has never been written to.
func (s *server) snapshot(group string) *sipmeshapiv1.OperatorConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cs, ok := s.groups[group]
	if !ok || cs.config == nil {
		return &sipmeshapiv1.OperatorConfig{}
	}
	return cs.proto()
}

// -- Live state (calls / workers / ai-workers) ----------------------
//
// All four RPCs read from in-memory fixtures populated by the HTTP
// seed endpoint (POST /__seed/calls, /__seed/workers,
// /__seed/ai-workers). When no fixture is seeded, responses are
// empty slices — frontend tests that depend on specific call /
// worker rows are expected to seed them explicitly per scenario.
//
// HangupCall + DrainWorker are mutators: they delete the call or
// drop the worker from the in-memory state so the next ListCalls /
// ListWorkers reflects the change, matching the real engine's
// observable behaviour. This lets the frontend's "hangup this call"
// flow be tested end-to-end against the mock.

func (s *server) ListCalls(ctx context.Context, req *sipmeshapiv1.ListCallsRequest) (*sipmeshapiv1.ListCallsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	worker := req.GetWorker()
	trunk := req.GetTrunk()
	flow := req.GetFlow()
	limit := int(req.GetLimit())

	out := make([]*sipmeshapiv1.CallSummary, 0, len(s.callOrder))
	for _, id := range s.callOrder {
		c := s.calls[id]
		if c == nil {
			continue
		}
		if worker != "" && c.GetWorker() != worker {
			continue
		}
		if trunk != "" && c.GetTrunk() != trunk {
			continue
		}
		if flow != "" && c.GetFlow() != flow {
			continue
		}
		out = append(out, proto.Clone(c).(*sipmeshapiv1.CallSummary))
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return &sipmeshapiv1.ListCallsResponse{Calls: out}, nil
}

func (s *server) GetCall(ctx context.Context, req *sipmeshapiv1.GetCallRequest) (*sipmeshapiv1.CallDetail, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.calls[req.GetInternalCallId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "call %q not found", req.GetInternalCallId())
	}
	return &sipmeshapiv1.CallDetail{
		Summary: proto.Clone(c).(*sipmeshapiv1.CallSummary),
	}, nil
}

func (s *server) HangupCall(ctx context.Context, req *sipmeshapiv1.HangupCallRequest) (*sipmeshapiv1.HangupCallResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.calls[req.GetInternalCallId()]
	if !ok {
		return &sipmeshapiv1.HangupCallResponse{Found: false}, nil
	}
	prior := c.GetWorker()
	delete(s.calls, req.GetInternalCallId())
	// Compact callOrder.
	out := s.callOrder[:0]
	for _, id := range s.callOrder {
		if id != req.GetInternalCallId() {
			out = append(out, id)
		}
	}
	s.callOrder = out
	return &sipmeshapiv1.HangupCallResponse{
		Found:       true,
		PriorWorker: prior,
	}, nil
}

func (s *server) ListWorkers(ctx context.Context, req *sipmeshapiv1.ListWorkersRequest) (*sipmeshapiv1.ListWorkersResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*sipmeshapiv1.WorkerSummaryV2, 0, len(s.workers))
	for _, w := range s.workers {
		out = append(out, proto.Clone(w).(*sipmeshapiv1.WorkerSummaryV2))
	}
	return &sipmeshapiv1.ListWorkersResponse{Workers: out}, nil
}

func (s *server) GetWorker(ctx context.Context, req *sipmeshapiv1.GetWorkerRequest) (*sipmeshapiv1.WorkerDetail, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, w := range s.workers {
		if w.GetId() == req.GetId() {
			return &sipmeshapiv1.WorkerDetail{
				Summary: proto.Clone(w).(*sipmeshapiv1.WorkerSummaryV2),
			}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "worker %q not found", req.GetId())
}

func (s *server) DrainWorker(ctx context.Context, req *sipmeshapiv1.DrainWorkerRequest) (*sipmeshapiv1.DrainWorkerResponse, error) {
	if !req.GetConfirm() {
		return nil, status.Errorf(codes.FailedPrecondition, "drain requires confirm=true")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.workers[:0]
	dropped := false
	for _, w := range s.workers {
		if w.GetId() == req.GetId() {
			dropped = true
			continue
		}
		out = append(out, w)
	}
	s.workers = out
	if !dropped {
		return &sipmeshapiv1.DrainWorkerResponse{Ok: false}, nil
	}
	return &sipmeshapiv1.DrainWorkerResponse{Ok: true}, nil
}

func (s *server) ListAIWorkers(ctx context.Context, req *sipmeshapiv1.ListAIWorkersRequest) (*sipmeshapiv1.ListAIWorkersResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*sipmeshapiv1.AIWorkerCapability, 0, len(s.aiWorkers))
	for _, p := range s.aiWorkers {
		out = append(out, proto.Clone(p).(*sipmeshapiv1.AIWorkerCapability))
	}
	return &sipmeshapiv1.ListAIWorkersResponse{Workers: out}, nil
}

// -- Streaming events + call archive --------------------------------

// SubscribeEvents emits the canned event queue then returns. No
// keep-alive, no live event delivery — frontend tests assert
// SSE-end-to-end against this one-shot drain. Topics filter applies
// as AND on Event.topic (empty topics list = no filter).
func (s *server) SubscribeEvents(req *sipmeshapiv1.SubscribeEventsRequest, stream grpc.ServerStreamingServer[sipmeshapiv1.Event]) error {
	s.mu.RLock()
	queue := make([]*sipmeshapiv1.Event, 0, len(s.events))
	for _, e := range s.events {
		queue = append(queue, proto.Clone(e).(*sipmeshapiv1.Event))
	}
	s.mu.RUnlock()

	topics := req.GetTopics()
	want := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		want[t] = struct{}{}
	}
	for _, e := range queue {
		if len(want) > 0 {
			if _, ok := want[e.GetTopic()]; !ok {
				continue
			}
		}
		if err := stream.Send(e); err != nil {
			return err
		}
	}
	return nil
}

// ListCallArchive paginates the seeded archive list. page_size=0
// returns everything (matches the engine's "small archives don't
// need paging" shape). The page_token field is honoured for
// completeness — empty in → first page, empty out → done.
func (s *server) ListCallArchive(ctx context.Context, req *sipmeshapiv1.ListCallArchiveRequest) (*sipmeshapiv1.ListCallArchiveResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*sipmeshapiv1.CallArchiveSummary, 0, len(s.archive))
	for _, r := range s.archive {
		out = append(out, proto.Clone(r).(*sipmeshapiv1.CallArchiveSummary))
	}
	limit := int(req.GetPageSize())
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return &sipmeshapiv1.ListCallArchiveResponse{Calls: out}, nil
}

// GetCallArtifactURL synthesises a URL pointing back at the mock's
// own HTTP seed server (`/__artifact/<key>`). The key is the same
// shape seedCallArchive used so frontend test fixtures can predict
// it offline if they need to assert URL shape directly.
//
// `seedAddr` from main flows in via the seedHostHint field set by
// newSeedServer when the HTTP listener binds; when the seed port
// is disabled (--seed-addr=""), this RPC returns FailedPrecondition
// the same way the real engine does when S3 archive isn't wired.
func (s *server) GetCallArtifactURL(ctx context.Context, req *sipmeshapiv1.GetCallArtifactURLRequest) (*sipmeshapiv1.GetCallArtifactURLResponse, error) {
	key := artifactKey(req.GetCallId(), req.GetKind())
	s.mu.RLock()
	a, ok := s.artifacts[key]
	hint := s.seedHostHint
	s.mu.RUnlock()
	if hint == "" {
		return nil, status.Error(codes.FailedPrecondition,
			"call archive not wired in this mock (--seed-addr was disabled, so no /__artifact handler exists)")
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound,
			"no seeded artifact for call %q kind %s", req.GetCallId(), req.GetKind().String())
	}
	ttl := int(req.GetTtlSeconds())
	if ttl == 0 {
		ttl = 15 * 60
	}
	if ttl > 60*60 {
		ttl = 60 * 60
	}
	return &sipmeshapiv1.GetCallArtifactURLResponse{
		Url:         "http://" + hint + "/__artifact/" + key,
		ExpiresAt:   time.Now().UTC().Add(time.Duration(ttl) * time.Second).Format(time.RFC3339),
		ContentType: a.contentType,
	}, nil
}

// -- Stubs (Unimplemented unless callers prove they need them) -------
//
// Remaining RPCs (StreamSipTrace) fall through to
// UnimplementedOperatorAPIServer's Unimplemented errors. Add per-
// test seed plumbing when frontend lands a test that needs it.

// register binds the server's RPC handlers to the gRPC server.
func (s *server) register(g *grpc.Server) {
	sipmeshapiv1.RegisterOperatorAPIServer(g, s)
}

// Compile-time assertion: server MUST implement OperatorAPIServer.
// If a new RPC lands in sipmesh-common's OperatorAPI service, this
// build fails until the mock either implements the method or relies
// on UnimplementedOperatorAPIServer to stub it. That's the wire-
// protocol drift guard described in the package doc.
var _ sipmeshapiv1.OperatorAPIServer = (*server)(nil)
