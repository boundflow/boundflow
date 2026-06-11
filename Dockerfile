FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /convergeplane ./cmd/convergeplane

FROM alpine:3.21
RUN apk --no-cache add ca-certificates
COPY --from=builder /convergeplane /convergeplane
ENTRYPOINT ["/convergeplane"]
