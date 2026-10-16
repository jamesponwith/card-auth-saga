#!/usr/bin/env bash
# Deploy the stack to Fly (ADR 0007): MySQL, then Temporal, then the worker at
# VERSION, then prove it with a /healthz smoke test. If the worker fails it,
# redeploy the image that was running and fail anyway. Idempotent: apps,
# volumes and secrets are created only if missing. Run by release.yml on a tag.
#
#   FLY_API_TOKEN, CAS_MYSQL_PASSWORD   from GitHub Actions secrets
#   VERSION                             the release tag (v0.2.0) or version (0.2.0)
#   FLY_APP_PREFIX, FLY_REGION          optional; see the defaults below
set -euo pipefail
: "${FLY_API_TOKEN:?}" "${CAS_MYSQL_PASSWORD:?}" "${VERSION:?}"
VERSION=${VERSION#v} # a tag (v0.2.0) or a version (0.2.0): goreleaser stamps without the v
prefix=${FLY_APP_PREFIX:-cas-jamesponwith}
region=${FLY_REGION:-iad}
cd "$(dirname "$0")/../.."
root=$PWD # every path below is absolute: build contexts, Dockerfiles, configs

ensure_app() {
  flyctl status -a "$1" >/dev/null 2>&1 || flyctl apps create "$1" --org personal
}
ensure_volume() { # app, volume, size in GB
  flyctl volumes list -a "$1" --json | grep -q "\"name\": *\"$2\"" ||
    flyctl volumes create "$2" -a "$1" --region "$region" --size "$3" --yes
  # Daily snapshots kept two weeks; restore-drill.sh proves they restore.
  local id
  id=$(flyctl volumes list -a "$1" --json | jq -r --arg n "$2" 'first(.[] | select(.name == $n)) | .id')
  flyctl volumes update "$id" -a "$1" --scheduled-snapshots=true --snapshot-retention 14 >/dev/null
}
deploy() { # app, config, extra flags...
  local app=$1 config=$2
  shift 2
  # One machine per app: MySQL and Temporal are bound to their volume, and two
  # workers would be fine but buy nothing here.
  flyctl deploy "$@" -a "$app" -c "$root/deploy/fly/$config" --primary-region "$region" --ha=false --yes
}

mysql=$prefix-mysql temporal=$prefix-temporal worker=$prefix-worker
ensure_app "$mysql"
ensure_volume "$mysql" mysql_data 3
flyctl secrets set -a "$mysql" --stage MYSQL_ROOT_PASSWORD="$CAS_MYSQL_PASSWORD" >/dev/null
deploy "$mysql" mysql.toml

ensure_app "$temporal"
ensure_volume "$temporal" temporal_data 1
deploy "$temporal" temporal.toml "$root/deploy/temporal" --dockerfile "$root/deploy/temporal/Dockerfile"

ensure_app "$worker"
flyctl secrets set -a "$worker" --stage >/dev/null \
  TEMPORAL_ADDRESS="$temporal.internal:7233" \
  CAS_MYSQL_DSN="root:$CAS_MYSQL_PASSWORD@tcp($mysql.internal:3306)/cas"

url="https://$worker.fly.dev/healthz"
# smoke: does the public /healthz report ok at version $1? It means the worker
# reached Temporal and MySQL, and the latest audit (if any) was clean.
smoke() {
  for _ in $(seq 1 "${SMOKE_TRIES:-30}"); do
    body=$(curl -fsS --max-time 10 "$url" || true)
    if echo "$body" | grep -q '"status":"ok"' && echo "$body" | grep -q "\"version\":\"$1\""; then
      return 0
    fi
    sleep "${SMOKE_WAIT:-10}"
  done
  return 1
}

# What the worker runs now, by digest (immutable), to roll back to.
prev_image=$(flyctl image show -a "$worker" --json 2>/dev/null |
  jq -r 'first(.[] | select(.Digest)) | "\(.Registry)/\(.Repository)@\(.Digest)"' 2>/dev/null || true)
prev_version=$(curl -fsS --max-time 10 "$url" 2>/dev/null | jq -r '.version // empty' 2>/dev/null || true)

deploy "$worker" worker.toml "$root" --dockerfile "$root/Dockerfile" --build-arg VERSION="$VERSION"
if smoke "$VERSION"; then
  echo "deployed $VERSION: $body"
  exit 0
fi
echo "FAIL: $url never reported ok at $VERSION; last: ${body:-no response}" >&2
if [ -z "$prev_image" ] || [ -z "$prev_version" ]; then
  echo "no earlier release to roll back to" >&2
  exit 1
fi
echo "rolling back to $prev_version ($prev_image)" >&2
deploy "$worker" worker.toml --image "$prev_image"
if smoke "$prev_version"; then
  echo "rolled back to $prev_version; $VERSION is not deployed" >&2
else
  echo "ROLLBACK FAILED: $prev_version isn't healthy either (last: ${body:-no response})." >&2
  echo "If $VERSION ran a migration, $prev_version refuses the newer schema: fix forward with a new release." >&2
fi
exit 1
