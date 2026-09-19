FROM golang:1.25-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /app
COPY . .

RUN CGO_ENABLED=1 go build -race -o emberdb .

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/emberdb .

ENTRYPOINT ["./emberdb"]
