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
type server struct {
	sipmeshapiv1.UnimplementedOperatorAPIServer

	mu     sync.RWMutex
	groups map[string]*configSet

	log *slog.Logger
}

// newServer constructs a mock with empty state. All groups start at
// version 0 with a zero-value OperatorConfig; first WriteConfig with
// a matching parent_version=0 advances to version 1.
func newServer(log *slog.Logger) *server {
	return &server{
		groups: make(map[string]*configSet),
		log:    log,
	}
}

// reset wipes all in-memory state. Exposed for the seed/admin HTTP
// endpoint so frontend Playwright tests can start each scenario with
// a known-empty backend.
func (s *server) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups = make(map[string]*configSet)
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

// -- Stubs (Unimplemented unless callers prove they need them) -------
//
// Calls / Workers / Archive / streaming RPCs return either empty
// canned responses (where empty is a meaningful state) or fall through
// to UnimplementedOperatorAPIServer's auto-generated Unimplemented
// gRPC errors. Frontend tests that need richer fixtures will get
// them via the seed HTTP endpoint (see main.go TODO).

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
