.PHONY: proto tidy clean hooks

# Regenerate Go bindings from .proto sources. Output goes to
# gen/go/<package-path>/. Requires `protoc` + `protoc-gen-go` +
# `protoc-gen-go-grpc` on PATH (`go install google.golang.org/{protobuf/cmd/protoc-gen-go,grpc/cmd/protoc-gen-go-grpc}@latest`).
proto:
	protoc \
		--go_out=gen/go --go_opt=module=github.com/rromenskyi/sipmesh-common/gen/go \
		--go-grpc_out=gen/go --go-grpc_opt=module=github.com/rromenskyi/sipmesh-common/gen/go \
		-I proto \
		proto/sipmesh/api/v1/operatorapi.proto \
		proto/sipmesh/v1/sipmesh.proto

tidy:
	go mod tidy

clean:
	rm -rf gen/go/

# One-shot: point this clone's git hooks at .githooks/. Run after
# the first clone so the commit-msg cyrillic-check gate is active
# locally. Idempotent. Mirrors the sipmesh repo's hooks target.
hooks:
	git config core.hooksPath .githooks
	@echo "git hooks: core.hooksPath = .githooks (active)"
