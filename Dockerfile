FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o go-redis ./cmd

FROM alpine:3.23
RUN apk add --no-cache redis
COPY --from=builder /app/go-redis /go-redis
EXPOSE 6379
CMD ["/go-redis"]
