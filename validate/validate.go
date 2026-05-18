// Package validate carries the pure-function ConfigSet validator
// shared by the sipmesh engine and any sister tool that wants to
// produce the same diagnostics without depending on the engine
// (mocks, frontend test stands, CLI dry-runs, CI gates).
//
// Wire-shape sync between sipmesh engine and consumers (sipmesh-mock,
// future tools) is enforced by both sides importing this single
// validate.OperatorConfig — when a new validation rule lands here, it
// fires on every consumer in lock-step. Same goes for relaxations:
// the validator IS the contract for ConfigSet apply semantics.
//
// API design notes:
//   - Single public entry point: OperatorConfig(cfg). Sub-validators
//     are package-private. Engine-side scoped validators (per-pipeline,
//     per-trunk, per-route) synthesise a minimal OperatorConfig with
//     placeholder cross-references, call OperatorConfig, then filter
//     the resulting diagnostics by FieldPath prefix. This keeps the
//     public surface minimal and the validator's internal coupling
//     contained to one function-call boundary.
//   - Output is always a []*sipmeshapiv1.ConfigDiagnostic — the proto
//     type carrying Severity + Message + FieldPath. Empty slice means
//     "valid".
//   - Pure function: no I/O, no globals mutated, no cluster lookups.
//     Safe to call from any process in any order.
package validate

import (
	"net/netip"
	"regexp"
	"sort"
	"strings"
	"time"

	sipmeshapiv1 "github.com/rromenskyi/sipmesh-common/gen/go/sipmesh/api/v1"
)

// OperatorConfig walks the graph and produces diagnostics for every
// cross-reference miss, required-field omission, or other schema
// violation. Does NOT touch the cluster — pure-function validation
// suitable for dry_run.
func OperatorConfig(cfg *sipmeshapiv1.OperatorConfig) []*sipmeshapiv1.ConfigDiagnostic {
	var out []*sipmeshapiv1.ConfigDiagnostic

	// Build name sets for cross-reference lookups.
	trunkIDs := make(map[string]int, len(cfg.GetTrunks()))
	for i, t := range cfg.GetTrunks() {
		if t.GetId() == "" {
			out = append(out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "trunk has empty id",
				FieldPath: jsonPointer("trunks", i, "id"),
			})
			continue
		}
		if prev, ok := trunkIDs[t.GetId()]; ok {
			out = append(out, &sipmeshapiv1.ConfigDiagnostic{
				Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message: "duplicate trunk id " + t.GetId() + " (also at trunks[" +
					itoa(prev) + "])",
				FieldPath: jsonPointer("trunks", i, "id"),
			})
			continue
		}
		trunkIDs[t.GetId()] = i
	}

	pipelineNames := make(map[string]struct{}, len(cfg.GetPipelines()))
	for i, p := range cfg.GetPipelines() {
		if p.GetName() == "" {
			out = append(out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "pipeline has empty name",
				FieldPath: jsonPointer("pipelines", i, "name"),
			})
			continue
		}
		if _, ok := pipelineNames[p.GetName()]; ok {
			out = append(out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "duplicate pipeline name " + p.GetName(),
				FieldPath: jsonPointer("pipelines", i, "name"),
			})
			continue
		}
		pipelineNames[p.GetName()] = struct{}{}

		if len(p.GetSteps()) == 0 {
			out = append(out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "pipeline " + p.GetName() + " has no steps",
				FieldPath: jsonPointer("pipelines", i, "steps"),
			})
		}

		if d := p.GetMaxCallDuration(); d != "" {
			if _, err := time.ParseDuration(d); err != nil {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message: "max_call_duration " + d + " is not a valid Go duration: " +
						err.Error(),
					FieldPath: jsonPointer("pipelines", i, "max_call_duration"),
				})
			}
		}
	}

	for i, r := range cfg.GetRoutes() {
		if m := r.GetMatch(); m != nil && m.GetTrunk() != "" {
			if _, ok := trunkIDs[m.GetTrunk()]; !ok {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "route references unknown trunk " + m.GetTrunk(),
					FieldPath: jsonPointer("routes", i, "match", "trunk"),
				})
			}
		}
		if r.GetPipeline() != "" {
			if _, ok := pipelineNames[r.GetPipeline()]; !ok {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "route references unknown pipeline " + r.GetPipeline(),
					FieldPath: jsonPointer("routes", i, "pipeline"),
				})
			}
		}
		groups := 0
		if r.GetFlow() != "" || r.GetPipeline() != "" {
			groups++
		}
		if r.GetExtension() != nil {
			groups++
		}
		if r.GetPeer() != nil {
			groups++
		}
		if r.GetForward() != nil {
			groups++
		}
		if r.GetTransit() != nil {
			groups++
		}
		if groups == 0 {
			out = append(out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "route has no target (exactly one of flow [+pipeline] / extension / peer / forward / transit must be set)",
				FieldPath: jsonPointer("routes", i),
			})
		} else if groups > 1 {
			out = append(out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "route has multiple targets set (exactly one of flow / extension / peer / forward / transit allowed; pipeline is a sub-selector for flow=ai_voice_bot)",
				FieldPath: jsonPointer("routes", i),
			})
		}
	}

	for i, t := range cfg.GetTrunks() {
		switch t.GetKind() {
		case sipmeshapiv1.Trunk_KIND_REGISTER_OUTBOUND, sipmeshapiv1.Trunk_KIND_UNSPECIFIED:
			if t.GetHost() == "" {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "register_outbound trunk requires host",
					FieldPath: jsonPointer("trunks", i, "host"),
				})
			}
			if t.GetUser() == "" {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "register_outbound trunk requires user",
					FieldPath: jsonPointer("trunks", i, "user"),
				})
			}
			if tr := t.GetTransport(); tr != "" && tr != "udp" && tr != "tcp" && tr != "tls" {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "transport must be udp/tcp/tls, got " + tr,
					FieldPath: jsonPointer("trunks", i, "transport"),
				})
			}
			if len(t.GetTargets()) > 0 {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "targets is only valid on kind=peer; use host on register_outbound",
					FieldPath: jsonPointer("trunks", i, "targets"),
				})
			}
			if t.GetAuth() != nil {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "auth is only valid on kind=peer; use user/pass on register_outbound",
					FieldPath: jsonPointer("trunks", i, "auth"),
				})
			}
		case sipmeshapiv1.Trunk_KIND_PEER:
			if len(t.GetTargets()) == 0 && len(t.GetAllowedIps()) == 0 {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "peer trunk requires at least one target or allowed_ips entry",
					FieldPath: jsonPointer("trunks", i, "targets"),
				})
			}
			switch t.GetDirection() {
			case sipmeshapiv1.Trunk_DIRECTION_UNSPECIFIED,
				sipmeshapiv1.Trunk_DIRECTION_IN,
				sipmeshapiv1.Trunk_DIRECTION_OUT,
				sipmeshapiv1.Trunk_DIRECTION_BOTH:
				// ok
			default:
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "direction must be DIRECTION_IN / DIRECTION_OUT / DIRECTION_BOTH",
					FieldPath: jsonPointer("trunks", i, "direction"),
				})
			}
			if t.GetHost() != "" || t.GetUser() != "" {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "host/user/pass not allowed on kind=peer; use targets + auth",
					FieldPath: jsonPointer("trunks", i),
				})
			}
			for k, tgt := range t.GetTargets() {
				if tgt.GetHost() == "" {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message:   "targets[].host required",
						FieldPath: jsonPointer("trunks", i, "targets", k, "host"),
					})
				}
				if tr := tgt.GetTransport(); tr != "" && tr != "udp" && tr != "tcp" && tr != "tls" {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message:   "targets[].transport must be udp/tcp/tls, got " + tr,
						FieldPath: jsonPointer("trunks", i, "targets", k, "transport"),
					})
				}
			}
			if a := t.GetAuth(); a != nil && a.GetUser() == "" {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "auth.user required when auth block is set",
					FieldPath: jsonPointer("trunks", i, "auth", "user"),
				})
			}
		case sipmeshapiv1.Trunk_KIND_REGISTER_INBOUND:
			if t.GetRealm() == "" {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "register_inbound trunk requires realm",
					FieldPath: jsonPointer("trunks", i, "realm"),
				})
			}
			ib := t.GetInboundAuth()
			if ib == nil {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "register_inbound trunk requires inbound_auth",
					FieldPath: jsonPointer("trunks", i, "inbound_auth"),
				})
			} else if len(ib.GetUsers()) == 0 {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "inbound_auth.users requires at least one entry",
					FieldPath: jsonPointer("trunks", i, "inbound_auth", "users"),
				})
			}
		}

		if rw := t.GetOutboundDialedRewrite(); rw != nil && rw.GetMatch() != "" {
			if _, err := regexp.Compile(rw.GetMatch()); err != nil {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "outbound_dialed_rewrite.match is not valid RE2: " + err.Error(),
					FieldPath: jsonPointer("trunks", i, "outbound_dialed_rewrite", "match"),
				})
			}
		}

		for j, or := range t.GetOutboundRoutes() {
			if or.GetTrunk() != "" {
				if _, ok := trunkIDs[or.GetTrunk()]; !ok {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message:   "outbound_routes references unknown trunk " + or.GetTrunk(),
						FieldPath: jsonPointer("trunks", i, "outbound_routes", j, "trunk"),
					})
				}
			}
			if m := or.GetMatch(); m != nil {
				if d := m.GetDialed(); d != nil && d.GetRegex() != "" {
					if _, err := regexp.Compile(d.GetRegex()); err != nil {
						out = append(out, &sipmeshapiv1.ConfigDiagnostic{
							Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
							Message:   "outbound_routes match.dialed.regex is not valid RE2: " + err.Error(),
							FieldPath: jsonPointer("trunks", i, "outbound_routes", j, "match", "dialed", "regex"),
						})
					}
				}
			}
			if rw := or.GetRewriteDialed(); rw != nil && rw.GetMatch() != "" {
				if _, err := regexp.Compile(rw.GetMatch()); err != nil {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message:   "outbound_routes rewrite_dialed.match is not valid RE2: " + err.Error(),
						FieldPath: jsonPointer("trunks", i, "outbound_routes", j, "rewrite_dialed", "match"),
					})
				}
			}
		}

		if allowed := t.GetCodecAllowed(); len(allowed) > 0 {
			allowedSet := make(map[string]struct{}, len(allowed))
			for _, c := range allowed {
				allowedSet[c] = struct{}{}
			}
			for k, c := range t.GetCodecPreference() {
				if _, ok := allowedSet[c]; !ok {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message: "codec_preference[" + itoa(k) + "]=" + c +
							" is not in codec_allowed (would be silently dropped at the allowlist; either add to codec_allowed or remove from codec_preference)",
						FieldPath: jsonPointer("trunks", i, "codec_preference", k),
					})
				}
			}
		}

		for k, raw := range t.GetAllowedIps() {
			if raw == "" {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "allowed_ips entry is empty",
					FieldPath: jsonPointer("trunks", i, "allowed_ips", k),
				})
				continue
			}
			if strings.Contains(raw, "/") {
				if _, err := netip.ParsePrefix(raw); err != nil {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message:   "allowed_ips " + raw + " is not a valid CIDR: " + err.Error(),
						FieldPath: jsonPointer("trunks", i, "allowed_ips", k),
					})
				}
			} else if _, err := netip.ParseAddr(raw); err != nil {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "allowed_ips " + raw + " is not a valid IP literal: " + err.Error(),
					FieldPath: jsonPointer("trunks", i, "allowed_ips", k),
				})
			}
		}

		for k, fam := range t.GetOutboundFamilyPreference() {
			if fam != "v4" && fam != "v6" {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message: "outbound_family_preference entry must be \"v4\" or \"v6\", got " +
						fam,
					FieldPath: jsonPointer("trunks", i, "outbound_family_preference", k),
				})
			}
		}

		for k, f := range t.GetInboundFilters() {
			fp := func(field string) string {
				return jsonPointer("trunks", i, "inbound_filters", k, field)
			}
			switch f.GetPolicy() {
			case sipmeshapiv1.InboundFilter_POLICY_BLOCK,
				sipmeshapiv1.InboundFilter_POLICY_ALLOW:
				// ok
			default:
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "inbound_filters policy must be BLOCK or ALLOW",
					FieldPath: fp("policy"),
				})
			}
			if f.GetMatchValue() == "" {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "inbound_filters match_value required",
					FieldPath: fp("match_value"),
				})
			}
			switch f.GetMatchKind() {
			case sipmeshapiv1.InboundFilter_MATCH_E164_EXACT,
				sipmeshapiv1.InboundFilter_MATCH_E164_PREFIX:
				// ok
			case sipmeshapiv1.InboundFilter_MATCH_REGEX:
				if v := f.GetMatchValue(); v != "" {
					if _, err := regexp.Compile(v); err != nil {
						out = append(out, &sipmeshapiv1.ConfigDiagnostic{
							Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
							Message: "inbound_filters match_value is not valid RE2: " +
								err.Error(),
							FieldPath: fp("match_value"),
						})
					}
				}
			default:
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "inbound_filters match_kind must be E164_EXACT, E164_PREFIX, or REGEX",
					FieldPath: fp("match_kind"),
				})
			}
			if c := f.GetRejectCode(); c != 0 && (c < 400 || c > 699) {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "inbound_filters reject_code must be 0 (engine default) or in 4xx/5xx/6xx range",
					FieldPath: fp("reject_code"),
				})
			}
		}

		if ib := t.GetInboundAuth(); ib != nil {
			seenUser := map[string]int{}
			for k, u := range ib.GetUsers() {
				if u.GetUser() == "" {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message:   "inbound_auth.users[].user empty",
						FieldPath: jsonPointer("trunks", i, "inbound_auth", "users", k, "user"),
					})
					continue
				}
				if prev, ok := seenUser[u.GetUser()]; ok {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message: "duplicate inbound_auth user " + u.GetUser() + " (also at " +
							itoa(prev) + ")",
						FieldPath: jsonPointer("trunks", i, "inbound_auth", "users", k, "user"),
					})
					continue
				}
				seenUser[u.GetUser()] = k
			}
			for k, name := range t.GetOperatorGroup() {
				if _, ok := seenUser[name]; !ok {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message: "operator_group references unknown user " + name +
							" (must appear in inbound_auth.users[])",
						FieldPath: jsonPointer("trunks", i, "operator_group", k),
					})
				}
			}
		}
	}

	for i, r := range cfg.GetRoutes() {
		if m := r.GetMatch(); m != nil {
			if cr := m.GetCallerRegex(); cr != "" {
				if _, err := regexp.Compile(cr); err != nil {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message:   "match.caller_regex is not valid RE2: " + err.Error(),
						FieldPath: jsonPointer("routes", i, "match", "caller_regex"),
					})
				}
			}
			if d := m.GetDialed(); d != nil && d.GetRegex() != "" {
				if _, err := regexp.Compile(d.GetRegex()); err != nil {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message:   "match.dialed.regex is not valid RE2: " + err.Error(),
						FieldPath: jsonPointer("routes", i, "match", "dialed", "regex"),
					})
				}
			}
			for k, allowed := range m.GetAllowedTrunks() {
				if _, ok := trunkIDs[allowed]; !ok {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message:   "match.allowed_trunks references unknown trunk " + allowed,
						FieldPath: jsonPointer("routes", i, "match", "allowed_trunks", k),
					})
				}
			}
		}
		if e := r.GetExtension(); e != nil && e.GetTargetTrunk() != "" {
			if idx, ok := trunkIDs[e.GetTargetTrunk()]; ok {
				if cfg.GetTrunks()[idx].GetKind() != sipmeshapiv1.Trunk_KIND_REGISTER_INBOUND {
					out = append(out, &sipmeshapiv1.ConfigDiagnostic{
						Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
						Message: "extension.target_trunk " + e.GetTargetTrunk() +
							" is not a register_inbound trunk",
						FieldPath: jsonPointer("routes", i, "extension", "target_trunk"),
					})
				}
			} else {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "extension.target_trunk references unknown trunk " + e.GetTargetTrunk(),
					FieldPath: jsonPointer("routes", i, "extension", "target_trunk"),
				})
			}
		}
		if f := r.GetForward(); f != nil && f.GetTrunk() != "" {
			if _, ok := trunkIDs[f.GetTrunk()]; !ok {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "forward.trunk references unknown trunk " + f.GetTrunk(),
					FieldPath: jsonPointer("routes", i, "forward", "trunk"),
				})
			}
		}
		if tr := r.GetTransit(); tr != nil && tr.GetTargetTrunk() != "" {
			if _, ok := trunkIDs[tr.GetTargetTrunk()]; !ok {
				out = append(out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "transit.target_trunk references unknown trunk " + tr.GetTargetTrunk(),
					FieldPath: jsonPointer("routes", i, "transit", "target_trunk"),
				})
			}
		}
	}

	for i, p := range cfg.GetPipelines() {
		for j, s := range p.GetSteps() {
			if d := s.GetDial(); d != nil {
				if og := d.GetOperatorGroupTrunk(); og != "" {
					if idx, ok := trunkIDs[og]; ok {
						if cfg.GetTrunks()[idx].GetKind() != sipmeshapiv1.Trunk_KIND_REGISTER_INBOUND {
							out = append(out, &sipmeshapiv1.ConfigDiagnostic{
								Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
								Message: "dial.operator_group_trunk " + og +
									" is not a register_inbound trunk",
								FieldPath: jsonPointer("pipelines", i, "steps", j, "dial", "operator_group_trunk"),
							})
						}
					} else {
						out = append(out, &sipmeshapiv1.ConfigDiagnostic{
							Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
							Message:   "dial.operator_group_trunk references unknown trunk " + og,
							FieldPath: jsonPointer("pipelines", i, "steps", j, "dial", "operator_group_trunk"),
						})
					}
				}
				if dt := d.GetTrunk(); dt != "" {
					if _, ok := trunkIDs[dt]; !ok {
						out = append(out, &sipmeshapiv1.ConfigDiagnostic{
							Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
							Message:   "dial.trunk references unknown trunk " + dt,
							FieldPath: jsonPointer("pipelines", i, "steps", j, "dial", "trunk"),
						})
					}
				}
			}
		}

		for j, s := range p.GetSteps() {
			validateStepDiscriminators(s, jsonPointer("pipelines", i, "steps", j), &out)
		}

		validatePipelineLabelScope(p.GetSteps(), jsonPointer("pipelines", i, "steps"), &out)
		validateSubPipelineRefs(p.GetSteps(), pipelineNames, jsonPointer("pipelines", i, "steps"), &out)
	}

	return out
}

// validatePipelineLabelScope mirrors flowconfig.validateLabelScope:
// LabelStep names unique within each scope; every GotoStep target
// resolves to a LabelStep in that same scope. BranchStep cases /
// default_steps each have their own independent scope (recurses).
func validatePipelineLabelScope(steps []*sipmeshapiv1.PipelineStep, basePath string, out *[]*sipmeshapiv1.ConfigDiagnostic) {
	labels := map[string]int{}
	for i, s := range steps {
		if l := s.GetLabel(); l != nil {
			name := l.GetName()
			if name == "" {
				continue
			}
			if prev, dup := labels[name]; dup {
				*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
					Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message: "duplicate label \"" + name + "\" in scope (also at step " +
						itoa(prev) + ")",
					FieldPath: basePath + "/" + itoa(i) + "/label/name",
				})
				continue
			}
			labels[name] = i
		}
	}
	for i, s := range steps {
		if g := s.GetGoto(); g != nil {
			label := g.GetLabel()
			if label == "" {
				*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "goto step requires label",
					FieldPath: basePath + "/" + itoa(i) + "/goto/label",
				})
				continue
			}
			if _, ok := labels[label]; !ok {
				*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "goto targets unknown label \"" + label + "\" (must exist in same scope)",
					FieldPath: basePath + "/" + itoa(i) + "/goto/label",
				})
			}
		}
	}
	for i, s := range steps {
		if b := s.GetBranch(); b != nil {
			for k, c := range b.GetCases() {
				validatePipelineLabelScope(c.GetSteps(),
					basePath+"/"+itoa(i)+"/branch/cases/"+itoa(k)+"/steps", out)
			}
			if def := b.GetDefaultSteps(); len(def) > 0 {
				validatePipelineLabelScope(def,
					basePath+"/"+itoa(i)+"/branch/default_steps", out)
			}
		}
	}
}

// validateSubPipelineRefs walks the step tree (including branch
// nesting) and asserts every SubPipelineStep.pipeline_name resolves
// in knownPipelines. Self-reference is allowed.
func validateSubPipelineRefs(steps []*sipmeshapiv1.PipelineStep, knownPipelines map[string]struct{}, basePath string, out *[]*sipmeshapiv1.ConfigDiagnostic) {
	for i, s := range steps {
		if sp := s.GetSubPipeline(); sp != nil {
			name := sp.GetName()
			if name == "" {
				*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "sub_pipeline step requires name",
					FieldPath: basePath + "/" + itoa(i) + "/sub_pipeline/name",
				})
			} else if _, ok := knownPipelines[name]; !ok {
				*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "sub_pipeline targets unknown pipeline \"" + name + "\"",
					FieldPath: basePath + "/" + itoa(i) + "/sub_pipeline/name",
				})
			}
		}
		if b := s.GetBranch(); b != nil {
			for k, c := range b.GetCases() {
				validateSubPipelineRefs(c.GetSteps(), knownPipelines,
					basePath+"/"+itoa(i)+"/branch/cases/"+itoa(k)+"/steps", out)
			}
			if def := b.GetDefaultSteps(); len(def) > 0 {
				validateSubPipelineRefs(def, knownPipelines,
					basePath+"/"+itoa(i)+"/branch/default_steps", out)
			}
		}
	}
}

// validateStepDiscriminators enforces the discriminated-union rules
// proto can't express directly: exactly one inner step kind on the
// envelope, the two-mode DialStep targeting check, per-step-kind
// required fields, and recursive walks into BranchStep / OnPeerStep.
func validateStepDiscriminators(s *sipmeshapiv1.PipelineStep, path string, out *[]*sipmeshapiv1.ConfigDiagnostic) {
	if s == nil {
		*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
			Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
			Message:   "pipeline step is nil",
			FieldPath: path,
		})
		return
	}
	kinds := []struct {
		set  bool
		name string
	}{
		{s.GetSay() != nil, "say"},
		{s.GetListen() != nil, "listen"},
		{s.GetConverse() != nil, "converse"},
		{s.GetDial() != nil, "dial"},
		{s.GetBridge() != nil, "bridge"},
		{s.GetHangup() != nil, "hangup"},
		{s.GetBranch() != nil, "branch"},
		{s.GetDtmf() != nil, "dtmf"},
		{s.GetSetCustomField() != nil, "set_custom_field"},
		{s.GetPause() != nil, "pause"},
		{s.GetHold() != nil, "hold"},
		{s.GetUnhold() != nil, "unhold"},
		{s.GetDtmfCollect() != nil, "dtmf_collect"},
		{s.GetReadyForBridge() != nil, "ready_for_bridge"},
		{s.GetAnswer() != nil, "answer"},
		{s.GetPlayFile() != nil, "play_file"},
		{s.GetHttpCallback() != nil, "http_callback"},
		{s.GetLabel() != nil, "label"},
		{s.GetGoto() != nil, "goto"},
		{s.GetRecord() != nil, "record"},
		{s.GetSubPipeline() != nil, "sub_pipeline"},
		{s.GetTransfer() != nil, "transfer"},
		{s.GetQueue() != nil, "queue"},
		{s.GetWhisper() != nil, "whisper"},
		{s.GetOnPeer() != nil, "on_peer"},
	}
	var setNames []string
	for _, k := range kinds {
		if k.set {
			setNames = append(setNames, k.name)
		}
	}
	switch len(setNames) {
	case 0:
		*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
			Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
			Message:   "pipeline step has no inner kind set (exactly one of say/listen/converse/dial/bridge/hangup/branch/dtmf/dtmf_collect/set_custom_field/pause/hold/unhold/ready_for_bridge/answer/play_file/http_callback/label/goto/record/sub_pipeline/transfer/queue/whisper/on_peer required)",
			FieldPath: path,
		})
	case 1:
		// fine
	default:
		*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
			Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
			Message:   "pipeline step has multiple inner kinds set (" + strings.Join(setNames, ", ") + "); exactly one allowed",
			FieldPath: path,
		})
	}

	if d := s.GetDial(); d != nil {
		// Two target modes, not three alternatives:
		//   operator_group_trunk set → callee/trunk MUST be empty
		//   operator_group_trunk empty → callee REQUIRED, trunk OPTIONAL
		if d.GetOperatorGroupTrunk() != "" {
			var clash []string
			if d.GetCallee() != "" {
				clash = append(clash, "callee")
			}
			if d.GetTrunk() != "" {
				clash = append(clash, "trunk")
			}
			if len(clash) > 0 {
				*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "dial step operator_group_trunk is mutually exclusive with " + strings.Join(clash, "/") + " (group fan-out resolves its targets from the trunk's registered bindings)",
					FieldPath: path + "/dial",
				})
			}
		} else if d.GetCallee() == "" {
			*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "dial step has no target (callee required, or set operator_group_trunk for hunt-group fan-out)",
				FieldPath: path + "/dial",
			})
		}
		if w := d.GetWaiting(); w != nil &&
			w.GetMode() == sipmeshapiv1.WaitingPolicy_MODE_MUSIC &&
			w.GetMohClip() == "" {
			*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "dial step waiting.mode=MODE_MUSIC requires moh_clip",
				FieldPath: path + "/dial/waiting/moh_clip",
			})
		}
	}

	if say := s.GetSay(); say != nil && len(say.GetTextByLanguage()) == 0 {
		*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
			Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
			Message:   "say step requires text_by_language (at least one entry)",
			FieldPath: path + "/say/text_by_language",
		})
	}
	if c := s.GetConverse(); c != nil {
		if len(c.GetSystemByLanguage()) == 0 {
			*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "converse step requires system_by_language (at least one entry)",
				FieldPath: path + "/converse/system_by_language",
			})
		}
		if len(c.GetFallbackTextByLanguage()) == 0 {
			*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "converse step requires fallback_text_by_language (the bot must always have something to say on Chat failure)",
				FieldPath: path + "/converse/fallback_text_by_language",
			})
		}
	}
	if scf := s.GetSetCustomField(); scf != nil && scf.GetKey() == "" {
		*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
			Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
			Message:   "set_custom_field step requires key",
			FieldPath: path + "/set_custom_field/key",
		})
	}
	if hc := s.GetHttpCallback(); hc != nil && hc.GetUrl() == "" {
		*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
			Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
			Message:   "http_callback step requires url",
			FieldPath: path + "/http_callback/url",
		})
	}
	if dc := s.GetDtmfCollect(); dc != nil {
		if t := dc.GetTerminator(); t != "" && len(t) != 1 {
			*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
				Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message: "dtmf_collect step terminator must be a single digit (got \"" +
					t + "\")",
				FieldPath: path + "/dtmf_collect/terminator",
			})
		}
	}
	if l := s.GetLabel(); l != nil && l.GetName() == "" {
		*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
			Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
			Message:   "label step requires name",
			FieldPath: path + "/label/name",
		})
	}
	if r := s.GetRecord(); r != nil && r.GetMaxDurationMs() == 0 {
		*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
			Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
			Message:   "record step requires max_duration_ms (zero would be unbounded)",
			FieldPath: path + "/record/max_duration_ms",
		})
	}
	if tr := s.GetTransfer(); tr != nil {
		if tr.GetTargetUri() == "" {
			*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "transfer step requires target_uri",
				FieldPath: path + "/transfer/target_uri",
			})
		}
		if tr.GetMode() == sipmeshapiv1.TransferStep_MODE_ATTENDED &&
			tr.GetPeerInternalCallId() == "" {
			*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
				Severity: sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message: "transfer step in MODE_ATTENDED requires peer_internal_call_id " +
					"(typically a ${...} reference to a prior DialStep's result)",
				FieldPath: path + "/transfer/peer_internal_call_id",
			})
		}
	}

	if b := s.GetBranch(); b != nil {
		for k, c := range b.GetCases() {
			validateBranchCasePredicates(c, path+"/branch/cases/"+itoa(k), out)
			for m, sub := range c.GetSteps() {
				validateStepDiscriminators(sub, path+"/branch/cases/"+itoa(k)+"/steps/"+itoa(m), out)
			}
		}
		for k, sub := range b.GetDefaultSteps() {
			validateStepDiscriminators(sub, path+"/branch/default_steps/"+itoa(k), out)
		}
	}

	if op := s.GetOnPeer(); op != nil {
		for k, sub := range op.GetSteps() {
			validateStepDiscriminators(sub, path+"/on_peer/steps/"+itoa(k), out)
		}
	}
}

// branchPredicateKeys is the closed set of typed predicate field names
// branchPredicateSchema accepts. `custom.<field>` keys are matched
// separately via the prefix check in validateBranchCasePredicates.
var branchPredicateKeys = map[string]struct{}{
	"caller_number":     {},
	"called_number":     {},
	"trunk_id":          {},
	"detected_language": {},
	"last_transcript":   {},
	"last_dtmf":         {},
	"last_dtmf_collect": {},
	"last_dial_codec":   {},
	"last_dial_status":  {},
	"time_window":       {},
}

// validateBranchCasePredicates rejects empty when{} (use default_steps)
// and unknown predicate keys (typo trap — yaml decoder silently
// drops unknowns). `custom.<field>` is the operator-facing encoding
// for matching CallState.CustomFields[<field>].
func validateBranchCasePredicates(c *sipmeshapiv1.BranchCase, path string, out *[]*sipmeshapiv1.ConfigDiagnostic) {
	when := c.GetWhen()
	if len(when) == 0 {
		*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
			Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
			Message:   "branch case.when must have at least one predicate (use default_steps for the always-fires fallback)",
			FieldPath: path + "/when",
		})
		return
	}
	keys := make([]string, 0, len(when))
	for k := range when {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if rest, ok := strings.CutPrefix(k, "custom."); ok {
			if rest == "" {
				*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
					Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
					Message:   "branch case.when has empty custom-field suffix (use `custom.<field>: <value>`)",
					FieldPath: path + "/when/" + k,
				})
			}
			continue
		}
		if _, ok := branchPredicateKeys[k]; !ok {
			*out = append(*out, &sipmeshapiv1.ConfigDiagnostic{
				Severity:  sipmeshapiv1.ConfigDiagnostic_SEVERITY_ERROR,
				Message:   "branch case.when has unknown predicate \"" + k + "\" (typo trap — yaml parser silently drops it; use one of caller_number/called_number/trunk_id/detected_language/last_transcript/last_dtmf/last_dtmf_collect/last_dial_codec/last_dial_status/time_window, or `custom.<field>` for CallState CustomFields matching)",
				FieldPath: path + "/when/" + k,
			})
		}
	}
}

// jsonPointer assembles a slash-prefixed RFC 6901 JSON pointer from
// a sequence of segments (strings or ints). Frontends use it to
// highlight the offending field in the constructor UI.
func jsonPointer(segments ...interface{}) string {
	out := ""
	for _, s := range segments {
		switch v := s.(type) {
		case string:
			out += "/" + v
		case int:
			out += "/" + itoa(v)
		}
	}
	return out
}

// itoa converts a small non-negative int to its decimal string. Used
// to avoid pulling strconv just for path building.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
