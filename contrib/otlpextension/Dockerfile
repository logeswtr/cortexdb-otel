# otlpextension as a container image.
#
#   docker build -t otlpextension:local contrib/otlpextension
#
# The build context is contrib/otlpextension itself: it is a separate Go module
# and imports nothing from the rest of the repository. The result is a static
# binary on Alpine (no CGO), following deploy/docker/Dockerfile.

# The build stage always runs on the builder's own platform and cross-compiles
# (pure Go, no CGO), so `--platform linux/amd64` on an arm64 laptop does not
# run the compiler under emulation.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/otlpextension .

FROM alpine:3.20
# ca-certificates only because BRIDGE_CORTEXDB_URL may be https.
RUN apk add --no-cache ca-certificates \
 && adduser -D -u 10001 bridge \
 && mkdir -p /data && chown bridge /data
COPY --from=build /out/otlpextension /usr/local/bin/otlpextension
VOLUME /data
USER bridge

# Defaults, all overridable at run time. The listeners bind 0.0.0.0 so that
# -p works; what they are reachable from is decided by the publish flag.
# The tenant switches are kept on the /data volume so a restart does not turn
# a customer off.
ENV BRIDGE_GRPC_ADDR=0.0.0.0:4317 \
    BRIDGE_HTTP_ADDR=0.0.0.0:4318 \
    BRIDGE_ADMIN_ADDR=0.0.0.0:4319 \
    BRIDGE_STATE_FILE=/data/tenants.json
EXPOSE 4317 4318 4319

# The binary is its own probe; it follows BRIDGE_ADMIN_ADDR's port.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD ["otlpextension", "-health"]

ENTRYPOINT ["otlpextension"]
