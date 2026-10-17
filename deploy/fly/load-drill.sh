#!/usr/bin/env bash
# Load drill (ADR 0008): measure the deployed stack, not a laptop. Stand up a
# throwaway copy of production with deploy.sh (same images, machine sizes and
# region, fresh volumes), run TestLoad against it through flyctl proxy, then
# destroy it. Production is never touched: its ledger stays free of load cards.
#
#   FLY_API_TOKEN              an org token: the drill creates apps
#   VERSION                    optional; the worker version to build (default: git describe)
#   FLY_APP_PREFIX, FLY_REGION, FLY_VM_SIZE   optional; as in deploy.sh
#   LOAD_TEMPORAL_PORT, LOAD_MYSQL_PORT, LOAD_DEBUG_PORT, PROXY_TRIES   local proxy ports, and how long to wait for them
set -euo pipefail
: "${FLY_API_TOKEN:?}"
prefix=${FLY_APP_PREFIX:-cas-jamesponwith}
cd "$(dirname "$0")/../.."
drill=$prefix-drill-load-$(date -u +%m%d%H%M)
tport=${LOAD_TEMPORAL_PORT:-17233} mport=${LOAD_MYSQL_PORT:-13306} dport=${LOAD_DEBUG_PORT:-19091}
# Its own password: production's never leaves production.
password=$(openssl rand -hex 16)

pids=()
# shellcheck disable=SC2329 # run by the EXIT trap below
cleanup() {
  for pid in ${pids[@]+"${pids[@]}"}; do kill "$pid" 2>/dev/null || true; done
  for app in "$drill-worker" "$drill-temporal" "$drill-mysql"; do
    case "$app" in
      *-drill-*) flyctl status -a "$app" >/dev/null 2>&1 || continue # never created
        flyctl apps destroy "$app" --yes >/dev/null 2>&1 || echo "WARN: could not destroy $app" >&2 ;;
      *) echo "refusing to destroy $app: not a drill app" >&2 ;;
    esac
  done
}
trap cleanup EXIT

FLY_APP_PREFIX=$drill CAS_MYSQL_PASSWORD=$password VERSION=${VERSION:-$(git describe --tags --always)} \
  deploy/fly/deploy.sh

flyctl proxy "$tport:7233" -a "$drill-temporal" >/dev/null 2>&1 & pids+=($!)
flyctl proxy "$mport:3306" -a "$drill-mysql" >/dev/null 2>&1 & pids+=($!)
flyctl proxy "$dport:9091" -a "$drill-worker" >/dev/null 2>&1 & pids+=($!)
for port in "$tport" "$mport"; do
  for try in $(seq 1 "${PROXY_TRIES:-30}"); do
    (: <"/dev/tcp/127.0.0.1/$port") 2>/dev/null && break
    [ "$try" = "${PROXY_TRIES:-30}" ] && { echo "proxy on port $port never came up" >&2; exit 1; }
    sleep 1
  done
done

for app in mysql temporal worker; do
  echo "$app machine: $(flyctl machine list -a "$drill-$app" --json | jq -c '.[0].config.guest')"
done

status=0
CAS_LOAD=1 CAS_LOAD_TEMPORAL=127.0.0.1:$tport CAS_MYSQL_DSN="root:$password@tcp(127.0.0.1:$mport)/cas" \
  go test -run '^TestLoad$' -v -count=1 -timeout 30m . || status=$?

# Where the time went: the worker's Temporal SDK timers (/debug/vars), slowest mean first.
echo "worker timers, mean over the run:"
curl -fsS --max-time 10 "http://127.0.0.1:$dport/debug/vars" | jq -r '.temporal as $t | $t | keys[] |
  select(endswith("_seconds") or contains("_seconds{")) as $k | ($k | sub("_seconds"; "_count")) as $c |
  select($t[$c] > 0) | "\($t[$k] / $t[$c] * 1000 | floor)ms\tx\($t[$c])\t\($k | sub("_seconds"; ""))"' |
  sort -rn | head -25 || echo "(no metrics)"
exit "$status"
