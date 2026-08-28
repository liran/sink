# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.27.0-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

ARG TARGETOS=linux
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/sink-quickstart ./examples/quickstart/client

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/sink-quickstart /usr/local/bin/sink-quickstart
ENTRYPOINT ["/usr/local/bin/sink-quickstart"]
