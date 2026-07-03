FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# -s -w strips the symbol table and DWARF debug info: smaller binary, and the
# easy strings/objdump reverse-engineering path gives up much less.
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /boundflow ./cmd/boundflow

FROM alpine:3.21
RUN apk --no-cache add ca-certificates
COPY --from=builder /boundflow /boundflow
# Carry the license in the image.
COPY LICENSE /LICENSE
ENTRYPOINT ["/boundflow"]
