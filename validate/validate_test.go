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
				Id:   "telco-out",
				Kind: sipmeshapiv1.Trunk_KIND_REGISTER_OUTBOUND,
				// sip.example.com / generic user — placeholders only.
				// Real test stands wire their own trunk/host/user
				// through the seed endpoint or import.
				Host: "sip.example.com",
				User: "operator-test",
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
								// NANP fictional range (+1 555 555 01xx) keeps
								// the test fixture obviously non-routable.
								When: map[string]string{"custom.route": "marketing"},
								Steps: []*sipmeshapiv1.PipelineStep{
									{Dial: &sipmeshapiv1.DialStep{
										Trunk:  "telco-out",
										Callee: "+15555550101",
										CallerId: &sipmeshapiv1.CallerID{
											User: "+15555550100",
										},
									}},
								},
							},
						},
						DefaultSteps: []*sipmeshapiv1.PipelineStep{
							{Dial: &sipmeshapiv1.DialStep{
								Trunk:  "telco-out",
								Callee: "+15555550102",
								CallerId: &sipmeshapiv1.CallerID{
									User: "+15555550100",
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

// TestOperatorConfig_WaitForGreeting — the step body is all-optional
// (zero = engine default), so only a result_field that can't be
// addressed as `custom.<key>` / `${custom.<key>}` is rejected.
func TestOperatorConfig_WaitForGreeting(t *testing.T) {
	cases := []struct {
		name    string
		step    *sipmeshapiv1.WaitForGreetingStep
		wantErr string // substring of the expected diagnostic; "" = valid
	}{
		{name: "empty body uses defaults", step: &sipmeshapiv1.WaitForGreetingStep{}},
		{name: "fully specified", step: &sipmeshapiv1.WaitForGreetingStep{
			MinSpeechMs:       150,
			EndpointSilenceMs: 600,
			NoSpeechTimeoutMs: 2500,
			MaxGreetingMs:     4000,
			ResultField:       "greeting_v2",
		}},
		{name: "result_field with a space", step: &sipmeshapiv1.WaitForGreetingStep{ResultField: "greeting outcome"}, wantErr: "wait_for_greeting step result_field"},
		{name: "result_field with a dot", step: &sipmeshapiv1.WaitForGreetingStep{ResultField: "custom.greeting"}, wantErr: "wait_for_greeting step result_field"},
		{name: "result_field starting with a digit", step: &sipmeshapiv1.WaitForGreetingStep{ResultField: "1st"}, wantErr: "wait_for_greeting step result_field"},
		{name: "result_field with a template brace", step: &sipmeshapiv1.WaitForGreetingStep{ResultField: "x}"}, wantErr: "wait_for_greeting step result_field"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &sipmeshapiv1.OperatorConfig{
				Pipelines: []*sipmeshapiv1.Pipeline{{
					Name: "outbound",
					Steps: []*sipmeshapiv1.PipelineStep{
						{WaitForGreeting: tc.step},
						{Hangup: &sipmeshapiv1.HangupStep{}},
					},
				}},
			}
			diags := OperatorConfig(cfg)
			if tc.wantErr == "" {
				for _, d := range diags {
					t.Errorf("unexpected diag: %s @ %s", d.GetMessage(), d.GetFieldPath())
				}
				return
			}
			var found bool
			for _, d := range diags {
				if strings.Contains(d.GetMessage(), tc.wantErr) {
					found = true
					if d.GetFieldPath() != "/pipelines/0/steps/0/wait_for_greeting/result_field" {
						t.Errorf("field path = %q", d.GetFieldPath())
					}
				}
			}
			if !found {
				t.Errorf("expected %q diag, got:\n%+v", tc.wantErr, diags)
			}
		})
	}
}

// TestOperatorConfig_NoKindMessageNamesWaitForGreeting — the "exactly
// one of" list is what authors read when a step lost its kind; a new
// kind missing from it reads as unsupported.
func TestOperatorConfig_NoKindMessageNamesWaitForGreeting(t *testing.T) {
	cfg := &sipmeshapiv1.OperatorConfig{
		Pipelines: []*sipmeshapiv1.Pipeline{{
			Name:  "broken",
			Steps: []*sipmeshapiv1.PipelineStep{{}},
		}},
	}
	for _, d := range OperatorConfig(cfg) {
		if strings.Contains(d.GetMessage(), "no inner kind set") {
			if !strings.Contains(d.GetMessage(), "/wait_for_greeting") {
				t.Errorf("no-kind message does not name wait_for_greeting: %s", d.GetMessage())
			}
			return
		}
	}
	t.Error("expected 'no inner kind set' diag")
}

// TestOperatorConfig_WaitForGreetingCountsAsAKind — a step carrying
// only wait_for_greeting has a kind; paired with another kind it is
// named in the "multiple inner kinds" diagnostic.
func TestOperatorConfig_WaitForGreetingCountsAsAKind(t *testing.T) {
	cfg := &sipmeshapiv1.OperatorConfig{
		Pipelines: []*sipmeshapiv1.Pipeline{{
			Name: "p",
			Steps: []*sipmeshapiv1.PipelineStep{
				{WaitForGreeting: &sipmeshapiv1.WaitForGreetingStep{}, Hangup: &sipmeshapiv1.HangupStep{}},
			},
		}},
	}
	var found bool
	for _, d := range OperatorConfig(cfg) {
		if strings.Contains(d.GetMessage(), "multiple inner kinds set (hangup, wait_for_greeting)") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected multiple-kinds diag naming wait_for_greeting, got:\n%+v", OperatorConfig(cfg))
	}
}
