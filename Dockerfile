FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/partforge ./cmd/partforge

FROM ubuntu:24.04
ARG DEBIAN_FRONTEND=noninteractive
ARG CLICKHOUSE_VERSION=26.3.10.60

RUN apt-get update \
    && apt-get install -y --no-install-recommends apt-transport-https ca-certificates curl gnupg tzdata \
    && curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg \
    && arch="$(dpkg --print-architecture)" \
    && echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=${arch}] https://packages.clickhouse.com/deb stable main" > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        clickhouse-common-static=${CLICKHOUSE_VERSION} \
        clickhouse-server=${CLICKHOUSE_VERSION} \
        clickhouse-client=${CLICKHOUSE_VERSION} \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/partforge /usr/local/bin/partforge
RUN chmod 0755 /usr/local/bin/partforge
USER clickhouse
ENTRYPOINT ["/usr/local/bin/partforge"]
CMD ["worker"]
