# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

# git нужен go mod download для VCS-зависимостей
# ca-certificates нужны если franz-go делает TLS к брокеру
RUN apk add --no-cache git ca-certificates

WORKDIR /build

# Зависимости в отдельном слое — не пересобираются при изменении кода
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

# TARGETARCH пробрасывается docker buildx автоматически.
# При обычном docker build — берётся архитектура хоста.
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/trending \
    ./cmd

# Создаём директорию для stoplist.db
RUN mkdir -p /data

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# distroless/static: нет shell, нет пакетного менеджера.
# nonroot: процесс запускается под UID 65532 — не root.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/trending /trending

# Копируем /data с правами nonroot пользователя (UID 65532)
COPY --from=builder --chown=65532:65532 /data /data

VOLUME ["/data"]

EXPOSE 8080

ENTRYPOINT ["/trending"]