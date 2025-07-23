FROM golang:1.24-bullseye AS build

WORKDIR /code

ENV CGO_ENABLED=1
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod go mod download -x
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build make

FROM debian:bullseye
LABEL org.opencontainers.image.source=https://github.com/agnosticeng/objstr

COPY --from=build /code/bin/* /

ENTRYPOINT ["/objstr"]
