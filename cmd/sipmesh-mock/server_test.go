package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	sipmeshapiv1 "github.com/rromenskyi/sipmesh-common/gen/go/sipmesh/api/v1"
)

// silentLogger discards log output so go test -v stays clean.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startBufconn boots a server on an in-process bufconn listener so
// the test doesn't need a real TCP port. Returns a ready-to-dial
// gRPC connection + a teardown function.
func startBufconn(t *testing.T) (sipmeshapiv1.OperatorAPIClient, *server, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := newServer(silentLogger())
	g := grpc.NewServer()
	s.register(g)
	go func() { _ = g.Serve(lis) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = ctx
	return sipmeshapiv1.NewOperatorAPIClient(conn), s, func() {
		_ = conn.Close()
		g.GracefulStop()
		_ = lis.Close()
	}
}

func TestGetOperatorConfig_EmptyStateReturnsZeroVersion(t *testing.T) {
	t.Parallel()
	cli, _, stop := startBufconn(t)
	defer stop()

	resp, err := cli.GetOperatorConfig(context.Background(), &sipmeshapiv1.GetOperatorConfigRequest{})
	if err != nil {
		t.Fatalf("GetOperatorConfig: %v", err)
	}
	if resp.GetConfig().GetVersion() != 0 {
		t.Errorf("empty-state version=%d, want 0", resp.GetConfig().GetVersion())
	}
	if len(resp.GetConfig().GetPipelines()) != 0 {
		t.Errorf("empty-state pipelines=%d, want 0", len(resp.GetConfig().GetPipelines()))
	}
}

func TestWriteConfig_UpsertPipelineThenRead(t *testing.T) {
	t.Parallel()
	cli, _, stop := startBufconn(t)
	defer stop()

	wresp, err := cli.WriteConfig(context.Background(), &sipmeshapiv1.WriteConfigRequest{
		ParentVersion: 0, // empty-state bootstrap
		Ops: []*sipmeshapiv1.ConfigOp{
			{Op: &sipmeshapiv1.ConfigOp_UpsertPipeline{
				UpsertPipeline: &sipmeshapiv1.Pipeline{
					Name: "smoke",
					Steps: []*sipmeshapiv1.PipelineStep{
						{Say: &sipmeshapiv1.SayStep{
							TextByLanguage: map[string]string{"en": "hello"},
						}},
						{Hangup: &sipmeshapiv1.HangupStep{Reason: "done"}},
					},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("WriteConfig: %v", err)
	}
	if wresp.GetVersion() != 1 {
		t.Errorf("version after first apply = %d, want 1", wresp.GetVersion())
	}
	if wresp.GetEtag() == "" {
		t.Error("etag empty after successful write")
	}
	if got := len(wresp.GetDiff().GetChanges()); got != 1 {
		t.Errorf("diff changes=%d, want 1", got)
	}

	resp, err := cli.GetOperatorConfig(context.Background(), &sipmeshapiv1.GetOperatorConfigRequest{})
	if err != nil {
		t.Fatalf("GetOperatorConfig: %v", err)
	}
	if resp.GetConfig().GetVersion() != 1 {
		t.Errorf("read-back version=%d, want 1", resp.GetConfig().GetVersion())
	}
	if len(resp.GetConfig().GetPipelines()) != 1 {
		t.Fatalf("read-back pipelines=%d, want 1", len(resp.GetConfig().GetPipelines()))
	}
	if resp.GetConfig().GetPipelines()[0].GetName() != "smoke" {
		t.Errorf("read-back pipeline name=%q, want \"smoke\"", resp.GetConfig().GetPipelines()[0].GetName())
	}
}

func TestWriteConfig_ValidationFailsOnEmptyPipelineSteps(t *testing.T) {
	t.Parallel()
	cli, _, stop := startBufconn(t)
	defer stop()

	_, err := cli.WriteConfig(context.Background(), &sipmeshapiv1.WriteConfigRequest{
		ParentVersion: 0,
		Ops: []*sipmeshapiv1.ConfigOp{
			{Op: &sipmeshapiv1.ConfigOp_UpsertPipeline{
				UpsertPipeline: &sipmeshapiv1.Pipeline{
					Name: "no-steps",
				},
			}},
		},
	})
	if err == nil {
		t.Fatal("expected validation error on empty-steps pipeline")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("status code=%s, want InvalidArgument", st.Code())
	}
	if !strings.Contains(err.Error(), "has no steps") {
		t.Errorf("expected 'has no steps' in error, got: %v", err)
	}
}

func TestWriteConfig_OCCRejectsStaleParentVersion(t *testing.T) {
	t.Parallel()
	cli, _, stop := startBufconn(t)
	defer stop()

	// First write succeeds (bootstrap).
	if _, err := cli.WriteConfig(context.Background(), &sipmeshapiv1.WriteConfigRequest{
		ParentVersion: 0,
		Ops: []*sipmeshapiv1.ConfigOp{
			{Op: &sipmeshapiv1.ConfigOp_UpsertPipeline{
				UpsertPipeline: validPipeline("a"),
			}},
		},
	}); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// Second write with parent_version=0 must be rejected (current is 1).
	_, err := cli.WriteConfig(context.Background(), &sipmeshapiv1.WriteConfigRequest{
		ParentVersion: 0,
		Ops: []*sipmeshapiv1.ConfigOp{
			{Op: &sipmeshapiv1.ConfigOp_UpsertPipeline{
				UpsertPipeline: validPipeline("b"),
			}},
		},
	})
	if err == nil {
		t.Fatal("expected OCC reject; got nil error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument && st.Code() != codes.Aborted {
		t.Errorf("status code=%s, want InvalidArgument or Aborted", st.Code())
	}
}

func TestWriteConfig_DryRunDoesNotPersist(t *testing.T) {
	t.Parallel()
	cli, _, stop := startBufconn(t)
	defer stop()

	dresp, err := cli.WriteConfig(context.Background(), &sipmeshapiv1.WriteConfigRequest{
		ParentVersion: 0,
		DryRun:        true,
		Ops: []*sipmeshapiv1.ConfigOp{
			{Op: &sipmeshapiv1.ConfigOp_UpsertPipeline{
				UpsertPipeline: validPipeline("dry"),
			}},
		},
	})
	if err != nil {
		t.Fatalf("dry-run write: %v", err)
	}
	if !dresp.GetDryRun() {
		t.Error("response.dry_run not set")
	}
	if dresp.GetVersion() != 0 {
		t.Errorf("dry-run version=%d, want 0 (unchanged)", dresp.GetVersion())
	}
	// State must still be empty.
	resp, _ := cli.GetOperatorConfig(context.Background(), &sipmeshapiv1.GetOperatorConfigRequest{})
	if resp.GetConfig().GetVersion() != 0 {
		t.Errorf("post-dry-run version=%d, want 0", resp.GetConfig().GetVersion())
	}
}

func TestSeed_WipesAndReplacesState(t *testing.T) {
	t.Parallel()
	_, s, stop := startBufconn(t)
	defer stop()

	// Write something via WriteConfig path.
	cfg := &sipmeshapiv1.OperatorConfig{
		Pipelines: []*sipmeshapiv1.Pipeline{validPipeline("seeded")},
	}
	v1, etag1 := s.seed(defaultGroup, cfg)
	if v1 != 1 {
		t.Errorf("seed version=%d, want 1", v1)
	}
	if etag1 == "" {
		t.Error("seed etag empty")
	}

	// Re-seed bumps version.
	v2, _ := s.seed(defaultGroup, cfg)
	if v2 != 2 {
		t.Errorf("re-seed version=%d, want 2", v2)
	}

	// Reset wipes to zero.
	s.reset()
	if got := s.snapshot(defaultGroup); got.GetVersion() != 0 || len(got.GetPipelines()) != 0 {
		t.Errorf("after reset: version=%d pipelines=%d, want 0/0",
			got.GetVersion(), len(got.GetPipelines()))
	}
}

func TestComputeETag_DeterministicAcrossClones(t *testing.T) {
	t.Parallel()
	cfg := &sipmeshapiv1.OperatorConfig{
		Pipelines: []*sipmeshapiv1.Pipeline{validPipeline("a")},
	}
	cloned := proto.Clone(cfg).(*sipmeshapiv1.OperatorConfig)
	if computeETag(cfg) != computeETag(cloned) {
		t.Error("etag differs across proto.Clone — must be deterministic")
	}
}

// validPipeline returns a minimal pipeline that passes
// validate.OperatorConfig. Two steps to satisfy the "no steps"
// check; both step kinds carry their required fields.
func validPipeline(name string) *sipmeshapiv1.Pipeline {
	return &sipmeshapiv1.Pipeline{
		Name: name,
		Steps: []*sipmeshapiv1.PipelineStep{
			{Say: &sipmeshapiv1.SayStep{
				TextByLanguage: map[string]string{"en": "hello"},
			}},
			{Hangup: &sipmeshapiv1.HangupStep{Reason: "done"}},
		},
	}
}
