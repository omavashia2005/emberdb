FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o emberdb .


FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/emberdb .

EXPOSE 6379
EXPOSE 16379

ENTRYPOINT ["./emberdb"]
