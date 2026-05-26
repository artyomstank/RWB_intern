FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/trending \
    ./cmd

RUN mkdir -p /data

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/trending /trending

COPY --from=builder --chown=65532:65532 /data /data

VOLUME ["/data"]

EXPOSE 8080

ENTRYPOINT ["/trending"]