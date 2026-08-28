# syntax=docker/dockerfile:1.7

FROM golang:1.27.0-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/sink ./cmd/sink

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/sink /usr/local/bin/sink
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/sink"]
