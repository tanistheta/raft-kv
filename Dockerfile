# syntax=docker/dockerfile:1

# Builder: full Go toolchain, only used to produce the static binary -
# never shipped. Pinned to 1.26 to match go.mod's `go 1.26.4` directive;
# bump both together if the module version changes.
FROM golang:1.26 AS builder
WORKDIR /src

# go.mod/go.sum copied and downloaded before the rest of the source so
# `docker build` can cache this layer across rebuilds that only touch .go
# files - the common case while iterating.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0: nothing in this module needs cgo (grpc-go and the
# protobuf runtime are pure Go), and a static binary is what lets the
# runtime stage below be distroless/static instead of needing glibc.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/node ./cmd/node

# Runtime: distroless static - no shell, no package manager, nothing
# beyond libc-free runtime deps. Root cause of "kill a container" (Phase
# 4G) needing to be a clean process-death test, not a shell exiting: this
# image can't accidentally have a shell process wrapping node's PID 1.
FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/node /node
ENTRYPOINT ["/node"]
