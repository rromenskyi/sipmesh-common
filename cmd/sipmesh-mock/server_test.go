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

// -- Live-state coverage -------------------------------------------

func TestListCalls_EmptyBeforeSeed(t *testing.T) {
	t.Parallel()
	cli, _, stop := startBufconn(t)
	defer stop()

	resp, err := cli.ListCalls(context.Background(), &sipmeshapiv1.ListCallsRequest{})
	if err != nil {
		t.Fatalf("ListCalls: %v", err)
	}
	if len(resp.GetCalls()) != 0 {
		t.Errorf("ListCalls before seed: got %d calls, want 0", len(resp.GetCalls()))
	}
}

func TestSeedCalls_ListAndFilterAndGet(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	seeded := s.seedCalls([]*sipmeshapiv1.CallSummary{
		{InternalCallId: "call-1", Worker: "w-a", Trunk: "t-1", Flow: "voicebot", State: "answered"},
		{InternalCallId: "call-2", Worker: "w-b", Trunk: "t-1", Flow: "voicebot", State: "ringing"},
		{InternalCallId: "call-3", Worker: "w-a", Trunk: "t-2", Flow: "answering_machine", State: "answered"},
		{InternalCallId: "", Worker: "should-be-dropped"}, // missing id → drop
	})
	if seeded != 3 {
		t.Fatalf("seedCalls accepted=%d, want 3 (4th has empty id)", seeded)
	}

	// Unfiltered.
	resp, err := cli.ListCalls(context.Background(), &sipmeshapiv1.ListCallsRequest{})
	if err != nil {
		t.Fatalf("ListCalls: %v", err)
	}
	if len(resp.GetCalls()) != 3 {
		t.Fatalf("unfiltered ListCalls got %d, want 3", len(resp.GetCalls()))
	}

	// Filter by worker.
	resp, _ = cli.ListCalls(context.Background(), &sipmeshapiv1.ListCallsRequest{Worker: "w-a"})
	if got := len(resp.GetCalls()); got != 2 {
		t.Errorf("worker=w-a got %d, want 2", got)
	}

	// Filter by trunk + limit.
	resp, _ = cli.ListCalls(context.Background(), &sipmeshapiv1.ListCallsRequest{Trunk: "t-1", Limit: 1})
	if got := len(resp.GetCalls()); got != 1 {
		t.Errorf("trunk=t-1 limit=1 got %d, want 1", got)
	}

	// GetCall hits.
	detail, err := cli.GetCall(context.Background(), &sipmeshapiv1.GetCallRequest{InternalCallId: "call-2"})
	if err != nil {
		t.Fatalf("GetCall call-2: %v", err)
	}
	if detail.GetSummary().GetWorker() != "w-b" {
		t.Errorf("call-2 worker=%q, want w-b", detail.GetSummary().GetWorker())
	}

	// GetCall miss.
	_, err = cli.GetCall(context.Background(), &sipmeshapiv1.GetCallRequest{InternalCallId: "ghost"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetCall ghost: got code %s, want NotFound", status.Code(err))
	}
}

func TestHangupCall_RemovesFromList(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	s.seedCalls([]*sipmeshapiv1.CallSummary{
		{InternalCallId: "call-1", Worker: "w-a"},
		{InternalCallId: "call-2", Worker: "w-b"},
	})

	resp, err := cli.HangupCall(context.Background(), &sipmeshapiv1.HangupCallRequest{
		InternalCallId: "call-1", Reason: "test",
	})
	if err != nil {
		t.Fatalf("HangupCall: %v", err)
	}
	if !resp.GetFound() {
		t.Error("HangupCall.Found=false, want true")
	}
	if resp.GetPriorWorker() != "w-a" {
		t.Errorf("PriorWorker=%q, want w-a", resp.GetPriorWorker())
	}

	// Now ListCalls should show only call-2.
	lresp, _ := cli.ListCalls(context.Background(), &sipmeshapiv1.ListCallsRequest{})
	if got := len(lresp.GetCalls()); got != 1 || lresp.GetCalls()[0].GetInternalCallId() != "call-2" {
		t.Errorf("post-hangup list=%v, want exactly call-2", lresp.GetCalls())
	}

	// Hangup of missing call → Found=false.
	resp, _ = cli.HangupCall(context.Background(), &sipmeshapiv1.HangupCallRequest{InternalCallId: "ghost"})
	if resp.GetFound() {
		t.Error("HangupCall(ghost).Found=true, want false")
	}
}

func TestWorkers_SeedListGetDrain(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	s.seedWorkers([]*sipmeshapiv1.WorkerSummaryV2{
		{Id: "edge-a", GrpcAddr: "10.0.0.1:50050", MaxConcurrent: 100, ActiveCalls: 7},
		{Id: "edge-b", GrpcAddr: "10.0.0.2:50050", MaxConcurrent: 100, ActiveCalls: 3},
	})

	resp, err := cli.ListWorkers(context.Background(), &sipmeshapiv1.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(resp.GetWorkers()) != 2 {
		t.Errorf("ListWorkers got %d, want 2", len(resp.GetWorkers()))
	}

	d, err := cli.GetWorker(context.Background(), &sipmeshapiv1.GetWorkerRequest{Id: "edge-b"})
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if d.GetSummary().GetActiveCalls() != 3 {
		t.Errorf("active_calls=%d, want 3", d.GetSummary().GetActiveCalls())
	}

	// Drain requires confirm=true.
	if _, err := cli.DrainWorker(context.Background(), &sipmeshapiv1.DrainWorkerRequest{Id: "edge-a"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("DrainWorker without confirm: got %s, want FailedPrecondition", status.Code(err))
	}

	dr, err := cli.DrainWorker(context.Background(), &sipmeshapiv1.DrainWorkerRequest{Id: "edge-a", Confirm: true})
	if err != nil {
		t.Fatalf("DrainWorker: %v", err)
	}
	if !dr.GetOk() {
		t.Error("DrainWorker.Ok=false, want true")
	}

	// edge-a gone.
	resp, _ = cli.ListWorkers(context.Background(), &sipmeshapiv1.ListWorkersRequest{})
	if len(resp.GetWorkers()) != 1 || resp.GetWorkers()[0].GetId() != "edge-b" {
		t.Errorf("post-drain workers=%v, want only edge-b", resp.GetWorkers())
	}
}

func TestAIWorkers_SeedAndList(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	s.seedAIWorkers([]*sipmeshapiv1.AIWorkerCapability{
		{
			PoolLabel: "cloud-premium",
			Voices: []*sipmeshapiv1.VoiceInfo{
				{Id: "en-Neural2-F", Language: "en-US", Gender: "F", Tier: "Neural2"},
			},
			LlmModels:     []string{"gemini-1.5-pro"},
			MaxConcurrent: 50,
			ActiveWorkers: 2,
		},
		{
			PoolLabel:     "local-standard",
			LlmModels:     []string{"qwen3.5:9b"},
			MaxConcurrent: 10,
			ActiveWorkers: 1,
		},
	})

	resp, err := cli.ListAIWorkers(context.Background(), &sipmeshapiv1.ListAIWorkersRequest{})
	if err != nil {
		t.Fatalf("ListAIWorkers: %v", err)
	}
	if len(resp.GetWorkers()) != 2 {
		t.Fatalf("ListAIWorkers got %d pools, want 2", len(resp.GetWorkers()))
	}
	if resp.GetWorkers()[0].GetPoolLabel() != "cloud-premium" {
		t.Errorf("first pool=%q, want cloud-premium", resp.GetWorkers()[0].GetPoolLabel())
	}
}

func TestReset_WipesLiveState(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	s.seedCalls([]*sipmeshapiv1.CallSummary{{InternalCallId: "x", Worker: "w"}})
	s.seedWorkers([]*sipmeshapiv1.WorkerSummaryV2{{Id: "w"}})
	s.seedAIWorkers([]*sipmeshapiv1.AIWorkerCapability{{PoolLabel: "p"}})

	s.reset()

	if lr, _ := cli.ListCalls(context.Background(), &sipmeshapiv1.ListCallsRequest{}); len(lr.GetCalls()) != 0 {
		t.Error("post-reset calls non-empty")
	}
	if wr, _ := cli.ListWorkers(context.Background(), &sipmeshapiv1.ListWorkersRequest{}); len(wr.GetWorkers()) != 0 {
		t.Error("post-reset workers non-empty")
	}
	if ar, _ := cli.ListAIWorkers(context.Background(), &sipmeshapiv1.ListAIWorkersRequest{}); len(ar.GetWorkers()) != 0 {
		t.Error("post-reset ai-workers non-empty")
	}
}

// -- SubscribeEvents + ListCallArchive coverage --------------------

func TestSubscribeEvents_EmitsSeededQueueThenEOF(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	s.seedEvents([]*sipmeshapiv1.Event{
		{Topic: "calls", Action: "started", SubjectId: "call-1", DetailJson: `{"caller":"+1"}`},
		{Topic: "config", Action: "applied", SubjectId: "42"},
		{Topic: "calls", Action: "ended", SubjectId: "call-1"},
	})

	stream, err := cli.SubscribeEvents(context.Background(), &sipmeshapiv1.SubscribeEventsRequest{})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}

	var got []*sipmeshapiv1.Event
	for {
		e, err := stream.Recv()
		if err != nil {
			break // EOF expected after the seeded queue drains
		}
		got = append(got, e)
	}
	if len(got) != 3 {
		t.Fatalf("received %d events, want 3", len(got))
	}
	if got[0].GetTopic() != "calls" || got[0].GetAction() != "started" {
		t.Errorf("event[0]={%s,%s}, want {calls,started}", got[0].GetTopic(), got[0].GetAction())
	}
	if got[2].GetSubjectId() != "call-1" {
		t.Errorf("event[2] subject_id=%q, want call-1", got[2].GetSubjectId())
	}
}

func TestSubscribeEvents_TopicFilterANDsOnTopic(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	s.seedEvents([]*sipmeshapiv1.Event{
		{Topic: "calls", Action: "started"},
		{Topic: "config", Action: "applied"},
		{Topic: "trunks", Action: "registered"},
		{Topic: "calls", Action: "ended"},
	})

	stream, _ := cli.SubscribeEvents(context.Background(), &sipmeshapiv1.SubscribeEventsRequest{
		Topics: []string{"calls"},
	})
	var got []string
	for {
		e, err := stream.Recv()
		if err != nil {
			break
		}
		got = append(got, e.GetTopic()+":"+e.GetAction())
	}
	if len(got) != 2 || got[0] != "calls:started" || got[1] != "calls:ended" {
		t.Errorf("filtered stream=%v, want [calls:started calls:ended]", got)
	}
}

func TestListCallArchive_SeedListAndPageSize(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	rows := []*sipmeshapiv1.CallArchiveSummary{
		{CallId: "c-1", StartedAt: "2026-05-18T10:00:00Z", HasRecording: true},
		{CallId: "c-2", StartedAt: "2026-05-18T10:05:00Z"},
		{CallId: "c-3", StartedAt: "2026-05-18T10:10:00Z", HasRecording: true, HasSipTrace: true},
	}
	if got, _ := s.seedCallArchive(rows, nil); got != 3 {
		t.Fatalf("seedCallArchive accepted=%d, want 3", got)
	}

	// Unlimited.
	resp, err := cli.ListCallArchive(context.Background(), &sipmeshapiv1.ListCallArchiveRequest{})
	if err != nil {
		t.Fatalf("ListCallArchive: %v", err)
	}
	if len(resp.GetCalls()) != 3 {
		t.Errorf("unlimited list got %d, want 3", len(resp.GetCalls()))
	}

	// page_size=2 returns first 2.
	resp, _ = cli.ListCallArchive(context.Background(), &sipmeshapiv1.ListCallArchiveRequest{PageSize: 2})
	if len(resp.GetCalls()) != 2 || resp.GetCalls()[0].GetCallId() != "c-1" {
		t.Errorf("page_size=2 got %v, want [c-1, c-2]", resp.GetCalls())
	}
}

func TestGetCallArtifactURL_SeedAndSynthesise(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	s.setSeedHostHint("127.0.0.1:50052")
	s.seedCallArchive(
		[]*sipmeshapiv1.CallArchiveSummary{{CallId: "c-1", HasRecording: true}},
		map[string]archiveArtifact{
			artifactKey("c-1", sipmeshapiv1.CallArtifactKind_CALL_ARTIFACT_RECORDING): {
				body:        []byte("fake-wav-bytes"),
				contentType: "audio/wav",
			},
		},
	)

	resp, err := cli.GetCallArtifactURL(context.Background(), &sipmeshapiv1.GetCallArtifactURLRequest{
		CallId:     "c-1",
		Kind:       sipmeshapiv1.CallArtifactKind_CALL_ARTIFACT_RECORDING,
		StartedAt:  "2026-05-18T10:00:00Z",
		TtlSeconds: 60,
	})
	if err != nil {
		t.Fatalf("GetCallArtifactURL: %v", err)
	}
	wantSuffix := "/__artifact/c-1:CALL_ARTIFACT_RECORDING"
	if !strings.HasSuffix(resp.GetUrl(), wantSuffix) {
		t.Errorf("URL=%q, want suffix %q", resp.GetUrl(), wantSuffix)
	}
	if !strings.Contains(resp.GetUrl(), "127.0.0.1:50052") {
		t.Errorf("URL=%q should contain seed-host hint", resp.GetUrl())
	}
	if resp.GetContentType() != "audio/wav" {
		t.Errorf("ContentType=%q, want audio/wav", resp.GetContentType())
	}
	if resp.GetExpiresAt() == "" {
		t.Error("ExpiresAt empty")
	}

	// Direct getArtifact roundtrip — the HTTP /__artifact handler
	// reads the same store, so this proves the same bytes will be
	// served.
	body, ct, ok := s.getArtifact(artifactKey("c-1", sipmeshapiv1.CallArtifactKind_CALL_ARTIFACT_RECORDING))
	if !ok || string(body) != "fake-wav-bytes" || ct != "audio/wav" {
		t.Errorf("getArtifact mismatch: ok=%v body=%q ct=%q", ok, body, ct)
	}
}

func TestGetCallArtifactURL_FailedPreconditionWhenSeedHostDisabled(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	// Don't call setSeedHostHint — mimics --seed-addr="".
	s.seedCallArchive(
		[]*sipmeshapiv1.CallArchiveSummary{{CallId: "c-1"}},
		map[string]archiveArtifact{
			artifactKey("c-1", sipmeshapiv1.CallArtifactKind_CALL_ARTIFACT_RECORDING): {
				body: []byte("x"), contentType: "audio/wav",
			},
		},
	)

	_, err := cli.GetCallArtifactURL(context.Background(), &sipmeshapiv1.GetCallArtifactURLRequest{
		CallId: "c-1",
		Kind:   sipmeshapiv1.CallArtifactKind_CALL_ARTIFACT_RECORDING,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("got code %s, want FailedPrecondition", status.Code(err))
	}
}

func TestGetCallArtifactURL_NotFoundForUnseededKey(t *testing.T) {
	t.Parallel()
	cli, s, stop := startBufconn(t)
	defer stop()

	s.setSeedHostHint("127.0.0.1:50052")
	// Don't seed anything.
	_, err := cli.GetCallArtifactURL(context.Background(), &sipmeshapiv1.GetCallArtifactURLRequest{
		CallId: "ghost",
		Kind:   sipmeshapiv1.CallArtifactKind_CALL_ARTIFACT_RECORDING,
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("got code %s, want NotFound", status.Code(err))
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
