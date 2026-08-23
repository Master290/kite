# syntax=docker/dockerfile:1
FROM golang:1.26.6-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/kite ./cmd/kite

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && addgroup -S kite && adduser -S -G kite kite
COPY --from=build /out/kite /usr/local/bin/kite
USER kite
WORKDIR /var/lib/kite
EXPOSE 8000/tcp 8443/tcp 8443/udp 9090/tcp
ENTRYPOINT ["kite"]
CMD ["serve", "-config", "/var/lib/kite/kite.yaml"]
