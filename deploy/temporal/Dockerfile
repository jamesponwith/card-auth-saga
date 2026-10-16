# Temporal for the Fly deployment (ADR 0007): the CLI's single-process server,
# the same version the drills and load test run, persisted to SQLite on /data.
FROM alpine:3.20
ARG TEMPORAL_CLI_VERSION=1.9.1
ARG TEMPORAL_CLI_SHA256=09a0326a51db84d02735e53542b9ebd8c4758daf47482a9ab0abce15844e60d5
RUN wget -q -O /tmp/cli.tgz "https://github.com/temporalio/cli/releases/download/v${TEMPORAL_CLI_VERSION}/temporal_cli_${TEMPORAL_CLI_VERSION}_linux_amd64.tar.gz" \
 && echo "${TEMPORAL_CLI_SHA256}  /tmp/cli.tgz" | sha256sum -c - \
 && tar -xzf /tmp/cli.tgz -C /usr/local/bin temporal \
 && rm /tmp/cli.tgz
EXPOSE 7233
# --ip :: listens on Fly's private IPv6 network; binding one IPv6 address
# breaks the server's internal services, which dial 127.0.0.1.
ENTRYPOINT ["temporal", "server", "start-dev", "--ip", "::", "--db-filename", "/data/temporal.db", \
  "--headless", "--namespace", "default", "--search-attribute", "CardID=Keyword", "--log-level", "warn"]
