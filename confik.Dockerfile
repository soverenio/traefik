# BUILD
FROM golang:1.16-alpine as gobuild

RUN apk --update upgrade \
    && apk --no-cache --no-progress add git mercurial bash gcc musl-dev curl tar ca-certificates tzdata \
    && update-ca-certificates \
    && rm -rf /var/cache/apk/*

RUN mkdir -p /usr/local/bin \
    && curl -fsSL -o /usr/local/bin/go-bindata https://github.com/containous/go-bindata/releases/download/v1.0.0/go-bindata \
    && chmod +x /usr/local/bin/go-bindata

WORKDIR /go/src/github.com/traefik/traefik

# Download go modules
COPY go.mod .
COPY go.sum .
RUN GO111MODULE=on GOPROXY=https://proxy.golang.org go mod download

COPY . /go/src/github.com/traefik/traefik

RUN ./script/make.sh generate confik

## IMAGE
FROM alpine:3.10

RUN apk --no-cache --no-progress add bash curl ca-certificates tzdata \
    && update-ca-certificates \
    && rm -rf /var/cache/apk/*

COPY --from=gobuild /go/src/github.com/traefik/traefik/dist/confik /

EXPOSE 80
VOLUME ["/tmp"]

ENTRYPOINT ["/confik"]