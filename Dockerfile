FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS build
WORKDIR /src
ARG TARGETARCH
COPY go.mod go.sum* ./
RUN go mod download
RUN mkdir -p /tmp/s5cmd-build /out \
    && cd /tmp/s5cmd-build \
    && go mod init example.com/s5cmd-build \
    && go get github.com/peak/s5cmd/v2@v2.3.0 \
    && CGO_ENABLED=0 GOARCH=${TARGETARCH} go build -o /out/s5cmd github.com/peak/s5cmd/v2
COPY . .
RUN CGO_ENABLED=0 GOARCH=${TARGETARCH} go build -o /out/partforge ./cmd/partforge

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
COPY --from=build /out/s5cmd /usr/local/bin/s5cmd
RUN chmod 0755 /usr/local/bin/partforge /usr/local/bin/s5cmd
USER clickhouse
ENTRYPOINT ["/usr/local/bin/partforge"]
CMD ["worker"]
