FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server && \
    CGO_ENABLED=0 go build -o /out/client ./cmd/client && \
    CGO_ENABLED=0 go build -o /out/harness ./cmd/harness

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /out/ /usr/local/bin/
COPY testdata/iot/ testdata/iot/
USER 65532:65532
CMD ["server"]
