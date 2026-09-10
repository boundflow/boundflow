# The builder runs on the build machine's own architecture and cross-compiles for
# the target, so an arm64 image doesn't compile Go under emulation.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder
ARG TARGETOS TARGETARCH
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# -s -w strips the symbol table and DWARF debug info: smaller binary, and the
# easy strings/objdump reverse-engineering path gives up much less.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /boundflow ./cmd/boundflow

FROM alpine:3.21
# Copied, not installed: nothing in this stage runs, so building arm64 needs no
# emulation. The bundle is plain text and the same on every architecture.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /boundflow /boundflow
# Carry the license in the image.
COPY LICENSE /LICENSE
ENTRYPOINT ["/boundflow"]
