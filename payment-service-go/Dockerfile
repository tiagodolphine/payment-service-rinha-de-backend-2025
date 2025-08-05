# Build stage
FROM golang:alpine AS builder
WORKDIR /app
RUN apk --no-cache add git
COPY . .
RUN go build -ldflags="-s -w" -o payment-service-go server.go

# Run stage
FROM alpine:latest
WORKDIR /app
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/payment-service-go .
ENV GOMAXPROCS=4
EXPOSE 8080
CMD ["./payment-service-go"]