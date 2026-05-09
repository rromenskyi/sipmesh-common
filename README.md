# sipmesh-common

Shared API contracts between [sipmesh](https://github.com/rromenskyi/sipmesh)
(server) and its clients (sipmesh-frontend, programmatic
integrations, future SDKs).

**Public** repo by design: proto type shapes describe an API, not
implementation. Making the contract surface public eliminates auth
ceremony for every consumer's CI — `go get` / `npm install` /
future `pip install` Just Works without PATs, deploy keys, GitHub
Apps, or BSR subscriptions. Implementation stays in
[rromenskyi/sipmesh](https://github.com/rromenskyi/sipmesh).

## Contents

```
proto/                          .proto sources — source of truth
├── sipmesh/api/v1/operatorapi.proto    public OperatorAPI surface
└── sipmesh/v1/sipmesh.proto            internal services + messages
                                        OperatorAPI projects from
gen/go/                         pre-generated Go bindings
├── sipmesh/api/v1/                     module path:
│                                       github.com/rromenskyi/sipmesh-common/gen/go/sipmesh/api/v1
└── sipmesh/v1/
```

Future generators (TypeScript, Python, etc) land at
`gen/<lang>/` siblings without disturbing existing imports.

## Consuming from Go

```bash
go get github.com/rromenskyi/sipmesh-common@vX.Y.Z
```

```go
import (
    sipmeshapiv1 "github.com/rromenskyi/sipmesh-common/gen/go/sipmesh/api/v1"
    sipmeshv1   "github.com/rromenskyi/sipmesh-common/gen/go/sipmesh/v1"
)
```

Pin to a tag in `go.mod`. No GOPRIVATE or auth setup — public repo.

## Versioning

Semver-tagged on every release.

- `v0.x` — wire shape still settling; minor bumps may carry
  additive proto changes (new field numbers on existing
  messages). Operators on v0.x should bump intentionally and
  re-build clients.
- `v1.0` once `OperatorAPI.ApplyOperatorConfig` and the
  boot-pull `SipmeshConfigSource.PullConfigSet` contracts are
  stable.
- Patch bumps for additive changes within a minor.
- Minor bumps for new messages or RPCs.
- Major bumps for breaking renames / removals.

Reserved field numbers in proto messages (e.g. `ExtensionRoute`
fields 3-7) are left for future extensions; see
[sipmesh/docs/IDEAS.md](https://github.com/rromenskyi/sipmesh/blob/main/docs/IDEAS.md)
for what they're earmarked for.

## Wire-path versioning

Service is `sipmesh.api.v1.OperatorAPI` — the `v1` is in the proto
package, hence in the wire path
(`/sipmesh.api.v1.OperatorAPI/<Method>`). When `v2` lands, it
ships as `gen/go/sipmesh/api/v2/` alongside v1 with side-by-side
server registration on meshctl + a 2-minor-release deprecation
window for v1. Existing v1 imports don't move.

## Regen workflow (this repo)

After editing a `.proto`:

```bash
make proto      # regen Go bindings into gen/go/
go build ./...  # verify the generated code compiles
make tidy       # update go.sum if dependency surface moved
git add proto/ gen/go/ go.{mod,sum}
git commit -m "feat: ..."
git tag vX.Y.Z
git push origin main vX.Y.Z
```

Consumers bump their pin to the new tag.

## Authority

`.proto` files in `proto/` are the canonical schema. The
`gen/go/` output is mechanically derived — never hand-edit. CI
on this repo will regenerate + diff to enforce that on PRs (TODO:
add the workflow).

## Implementation lives elsewhere

Server code, internal types, infrastructure — all in
[rromenskyi/sipmesh](https://github.com/rromenskyi/sipmesh).
This repo intentionally has nothing executable beyond the proto
toolchain shim.
