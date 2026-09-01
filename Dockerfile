# Build stage: Alpine-based Go toolchain producing a fully static binary.
FROM golang:1.27-alpine AS builder

ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# Dependencies first, so a source-only change reuses the module cache layer.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/tesira2mqtt ./cmd/tesira2mqtt

# Runtime stage: nothing but the binary and CA roots.
FROM scratch

# Needed only for TLS MQTT brokers (ssl:// / wss://); harmless otherwise.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/tesira2mqtt /tesira2mqtt

USER 65534:65534

ENTRYPOINT ["/tesira2mqtt"]
CMD ["-config", "/config.yaml"]
