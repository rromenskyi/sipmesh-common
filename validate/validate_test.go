package validate

import (
	"strings"
	"testing"

	sipmeshapiv1 "github.com/rromenskyi/sipmesh-common/gen/go/sipmesh/api/v1"
)

// TestOperatorConfig_EmptyConfigOK — empty config produces zero
// diagnostics. Establishes the baseline "valid means empty slice"
// contract callers rely on.
func TestOperatorConfig_EmptyConfigOK(t *testing.T) {
	if diags := OperatorConfig(&sipmeshapiv1.OperatorConfig{}); len(diags) != 0 {
		t.Errorf("empty config emitted %d diagnostics, want 0:\n%v", len(diags), diags)
	}
}

// TestOperatorConfig_CatchesEmptyPipelineSteps — primary positive
// shape from the engine's TestValidateOperatorConfig coverage.
func TestOperatorConfig_CatchesEmptyPipelineSteps(t *testing.T) {
	cfg := &sipmeshapiv1.OperatorConfig{
		Pipelines: []*sipmeshapiv1.Pipeline{
			{Name: "broken"}, // no steps
		},
	}
	diags := OperatorConfig(cfg)
	if len(diags) == 0 {
		t.Fatal("expected diagnostics for pipeline with no steps")
	}
	var found bool
	for _, d := range diags {
		if strings.Contains(d.GetMessage(), "has no steps") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected 'has no steps' diag, got:\n%+v", diags)
	}
}

// TestOperatorConfig_AcceptsValidReceptionistShape — the canonical
// happy path: routes, trunks, pipeline with whisper + on_peer + branch
// + custom-field matcher + dial caller_id. This is the receptionist
// demo (PRs #403/404/405/406/407) shape — emits zero diagnostics.
func TestOperatorConfig_AcceptsValidReceptionistShape(t *testing.T) {
	cfg := &sipmeshapiv1.OperatorConfig{
		Trunks: []*sipmeshapiv1.Trunk{
			{
				Id:   "zadarma-in",
				Kind: sipmeshapiv1.Trunk_KIND_REGISTER_OUTBOUND,
				Host: "sip.zadarma.com",
				User: "453425",
			},
		},
		Pipelines: []*sipmeshapiv1.Pipeline{
			{
				Name: "receptionist",
				Steps: []*sipmeshapiv1.PipelineStep{
					{Say: &sipmeshapiv1.SayStep{
						TextByLanguage: map[string]string{"en": "Hello"},
					}},
					{Converse: &sipmeshapiv1.ConverseStep{
						SystemByLanguage:       map[string]string{"en": "You are X."},
						FallbackTextByLanguage: map[string]string{"en": "Sorry?"},
					}},
					{Hold: &sipmeshapiv1.HoldStep{Mode: sipmeshapiv1.HoldStep_MODE_MOH}},
					{Branch: &sipmeshapiv1.BranchStep{
						Cases: []*sipmeshapiv1.BranchCase{
							{
								When: map[string]string{"custom.route": "marketing"},
								Steps: []*sipmeshapiv1.PipelineStep{
									{Dial: &sipmeshapiv1.DialStep{
										Trunk:  "zadarma-in",
										Callee: "+34662239300",
										CallerId: &sipmeshapiv1.CallerID{
											User: "+13855180204",
										},
									}},
								},
							},
						},
						DefaultSteps: []*sipmeshapiv1.PipelineStep{
							{Dial: &sipmeshapiv1.DialStep{
								Trunk:  "zadarma-in",
								Callee: "+19254445028",
								CallerId: &sipmeshapiv1.CallerID{
									User: "+13855180204",
								},
							}},
						},
					}},
					{OnPeer: &sipmeshapiv1.OnPeerStep{
						Steps: []*sipmeshapiv1.PipelineStep{
							{Whisper: &sipmeshapiv1.WhisperStep{
								TextByLanguage: map[string]string{"en": "Call from caller"},
							}},
						},
					}},
					{Unhold: &sipmeshapiv1.UnholdStep{}},
					{Branch: &sipmeshapiv1.BranchStep{
						Cases: []*sipmeshapiv1.BranchCase{
							{
								When: map[string]string{"custom.last_whisper_outcome": "accept"},
								Steps: []*sipmeshapiv1.PipelineStep{
									{Bridge: &sipmeshapiv1.BridgeStep{}},
								},
							},
						},
						DefaultSteps: []*sipmeshapiv1.PipelineStep{
							{Hangup: &sipmeshapiv1.HangupStep{Reason: "voicemail"}},
						},
					}},
					{Hangup: &sipmeshapiv1.HangupStep{Reason: "done"}},
				},
			},
		},
	}
	diags := OperatorConfig(cfg)
	if len(diags) != 0 {
		for i, d := range diags {
			t.Errorf("[%d] %s @ %s", i, d.GetMessage(), d.GetFieldPath())
		}
	}
}

// TestOperatorConfig_CatchesUnknownStepKind — protective check
// for the validator's discriminator coverage. An empty PipelineStep
// envelope (no inner kind set) must error, otherwise the silent-drop
// failure mode operators tripped on with whisper/on_peer comes back.
func TestOperatorConfig_CatchesUnknownStepKind(t *testing.T) {
	cfg := &sipmeshapiv1.OperatorConfig{
		Pipelines: []*sipmeshapiv1.Pipeline{
			{
				Name: "broken",
				Steps: []*sipmeshapiv1.PipelineStep{
					{}, // no inner kind
				},
			},
		},
	}
	diags := OperatorConfig(cfg)
	var found bool
	for _, d := range diags {
		if strings.Contains(d.GetMessage(), "no inner kind set") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected 'no inner kind set' diag, got:\n%+v", diags)
	}
}

// TestOperatorConfig_BranchCaseCustomFieldAccepted — the `custom.<field>`
// branch predicate prefix (sipmesh#405) is the operator-facing
// encoding for matching CallState.CustomFields[<field>]. Validator
// must accept the prefixed key without flagging it as a typo.
func TestOperatorConfig_BranchCaseCustomFieldAccepted(t *testing.T) {
	cfg := &sipmeshapiv1.OperatorConfig{
		Pipelines: []*sipmeshapiv1.Pipeline{
			{
				Name: "ok",
				Steps: []*sipmeshapiv1.PipelineStep{
					{Branch: &sipmeshapiv1.BranchStep{
						Cases: []*sipmeshapiv1.BranchCase{
							{
								When: map[string]string{"custom.last_whisper_outcome": "accept"},
								Steps: []*sipmeshapiv1.PipelineStep{
									{Hangup: &sipmeshapiv1.HangupStep{}},
								},
							},
						},
					}},
				},
			},
		},
	}
	for _, d := range OperatorConfig(cfg) {
		if strings.Contains(d.GetMessage(), "unknown predicate \"custom.last_whisper_outcome\"") {
			t.Errorf("custom.<field> predicate flagged as unknown: %s", d.GetMessage())
		}
	}
}

// TestOperatorConfig_DialTrunkPlusCalleeAccepted — `trunk` + `callee`
// is a legitimate pair (sipmesh#406). Only operator_group_trunk is
// mutually exclusive with the other two.
func TestOperatorConfig_DialTrunkPlusCalleeAccepted(t *testing.T) {
	cfg := &sipmeshapiv1.OperatorConfig{
		Trunks: []*sipmeshapiv1.Trunk{
			{Id: "z", Kind: sipmeshapiv1.Trunk_KIND_REGISTER_OUTBOUND, Host: "h", User: "u"},
		},
		Pipelines: []*sipmeshapiv1.Pipeline{
			{
				Name: "p",
				Steps: []*sipmeshapiv1.PipelineStep{
					{Dial: &sipmeshapiv1.DialStep{Trunk: "z", Callee: "+12025550100"}},
				},
			},
		},
	}
	for _, d := range OperatorConfig(cfg) {
		if strings.Contains(d.GetMessage(), "multiple targets") ||
			strings.Contains(d.GetMessage(), "no target") {
			t.Errorf("trunk+callee must be accepted: %s @ %s", d.GetMessage(), d.GetFieldPath())
		}
	}
}
