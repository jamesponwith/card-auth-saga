# The worker image for the Fly deployment (ADR 0007). VERSION is the release
# tag without its "v", as goreleaser stamps it, so /healthz reports the release.
# The toolchain go.mod pins, so the binary links a patched standard library.
FROM golang:1.26.10 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-s -w -X main.Version=${VERSION}" -o /card-auth-saga .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /card-auth-saga /card-auth-saga
EXPOSE 8080
ENTRYPOINT ["/card-auth-saga"]
