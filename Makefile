.PHONY: proto tidy clean

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
