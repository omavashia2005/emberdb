FROM golang:1.25-alpine AS builder

ARG RACE=0

WORKDIR /app

COPY go.mod go.sum ./
COPY utils/go-resp/go.mod ./utils/go-resp/go.mod
RUN go mod download

COPY . .

# ponytail: RACE=1 needs cgo+gcc for the race detector; skipped for bench builds, it's ~10x slower.
RUN if [ "$RACE" = "1" ]; then \
      apk add --no-cache build-base && CGO_ENABLED=1 go build -race -o emberdb . ; \
    else \
      go build -o emberdb . ; \
    fi


FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/emberdb .

EXPOSE 6379
EXPOSE 16379

ENTRYPOINT ["./emberdb"]
