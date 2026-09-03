FROM golang:1.27-alpine AS builder

ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/tesira2mqtt ./cmd/tesira2mqtt

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/tesira2mqtt /tesira2mqtt

USER 65534:65534

ENTRYPOINT ["/tesira2mqtt"]
CMD ["-config", "/config.yaml"]
