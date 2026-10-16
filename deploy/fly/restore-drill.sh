#!/usr/bin/env bash
# Restore drill (ADR 0007): prove the production backups restore. For MySQL and
# Temporal, restore the latest snapshot of the production volume into a
# throwaway app, boot it on the exact image production runs, and check the
# data; then destroy the throwaway apps. Run monthly by restore-drill.yml.
#
#   FLY_API_TOKEN, CAS_MYSQL_PASSWORD   from GitHub Actions secrets
#   FLY_APP_PREFIX, FLY_REGION          optional; as in deploy.sh
#   SNAPSHOT_WAIT, CHECK_TRIES          test knobs: snapshot polls and check retries
set -euo pipefail
: "${FLY_API_TOKEN:?}" "${CAS_MYSQL_PASSWORD:?}"
prefix=${FLY_APP_PREFIX:-cas-jamesponwith}
region=${FLY_REGION:-iad}
cd "$(dirname "$0")/../.."
root=$PWD
drill=$prefix-drill-$(date -u +%m%d%H%M)
mysql=$prefix-mysql temporal=$prefix-temporal

# Tear down whatever the drill made, and only that: never an app without
# "-drill-" in its name.
made=()
# shellcheck disable=SC2329 # run by the EXIT trap below
cleanup() {
  for app in ${made[@]+"${made[@]}"}; do
    case "$app" in
      *-drill-*) flyctl apps destroy "$app" --yes >/dev/null 2>&1 || echo "WARN: could not destroy $app" >&2 ;;
      *) echo "refusing to destroy $app: not a drill app" >&2 ;;
    esac
  done
}
trap cleanup EXIT

# latest_snapshot prints the newest snapshot of a production volume, taking one
# if the volume has none yet (a volume younger than a day).
latest_snapshot() { # app, volume name
  local vol snap
  vol=$(flyctl volumes list -a "$1" --json | jq -r --arg n "$2" 'first(.[] | select(.name == $n)) | .id')
  [ -n "$vol" ] || { echo "no volume $2 on $1" >&2; return 1; }
  snap=$(flyctl volumes snapshots list "$vol" --json | jq -r 'sort_by(.created_at) | last | .id // empty')
  if [ -z "$snap" ]; then
    echo "$2 has no snapshot yet: taking one" >&2
    flyctl volumes snapshots create "$vol" >/dev/null
    for _ in $(seq 1 "${SNAPSHOT_WAIT:-60}"); do
      snap=$(flyctl volumes snapshots list "$vol" --json | jq -r 'map(select(.status == "created")) | sort_by(.created_at) | last | .id // empty')
      [ -n "$snap" ] && break
      sleep 5
    done
  fi
  [ -n "$snap" ] || { echo "no usable snapshot of $2" >&2; return 1; }
  echo "$snap"
}

# restore boots a throwaway copy of a production app from its latest snapshot.
restore() { # production app, volume name, size in GB, config, drill app
  local snap image
  snap=$(latest_snapshot "$1" "$2")
  image=$(flyctl image show -a "$1" --json | jq -r 'first(.[] | select(.Digest)) | "\(.Registry)/\(.Repository)@\(.Digest)"')
  echo "restoring $1:$2 from $snap into $5 ($image)" >&2
  flyctl apps create "$5" --org personal >/dev/null
  made+=("$5")
  flyctl volumes create "$2" -a "$5" --snapshot-id "$snap" --region "$region" --size "$3" --yes >/dev/null
  if [ "$4" = mysql.toml ]; then # the restored data has its own root password; the image still asks for one
    flyctl secrets set -a "$5" --stage MYSQL_ROOT_PASSWORD="$CAS_MYSQL_PASSWORD" >/dev/null
  fi
  flyctl deploy -a "$5" -c "$root/deploy/fly/$4" --image "$image" --primary-region "$region" --ha=false --yes >/dev/null
}

# on runs a command on an app's machine, retrying while it boots.
on() { # app, command
  local out
  for _ in $(seq 1 "${CHECK_TRIES:-30}"); do
    if out=$(flyctl ssh console -a "$1" -C "$2" 2>/dev/null); then
      echo "$out"
      return 0
    fi
    sleep 10
  done
  echo "could not run on $1: $2" >&2
  return 1
}
sql() { # app, query: one value, no headers
  on "$1" "env MYSQL_PWD=$CAS_MYSQL_PASSWORD mysql -uroot -N -B cas -e \"$2\"" | tail -1 | tr -d '[:space:]'
}

restore "$mysql" mysql_data 3 mysql.toml "$drill-mysql"
restore "$temporal" temporal_data 1 temporal.toml "$drill-temporal"

failed=0
check() { # name, got, want
  if [ "$2" = "$3" ]; then echo "ok   $1: $2"; else echo "FAIL $1: got '$2', want '$3'" >&2; failed=1; fi
}
cards_off='SELECT COUNT(*) FROM (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(id, ':', 2), ':', -1) AS card, SUM(balance) AS s FROM accounts WHERE id LIKE 'card:%' AND (id LIKE '%:holds' OR id LIKE '%:posted' OR id LIKE '%:open-to-buy') GROUP BY card HAVING s <> 0) x'
check "ledger sums to zero" "$(sql "$drill-mysql" 'SELECT COALESCE(SUM(balance), 0) FROM accounts')" 0
check "every card's money nets to zero" "$(sql "$drill-mysql" "$cards_off")" 0
check "schema version matches production" "$(sql "$drill-mysql" 'SELECT MAX(version) FROM schema_migrations')" "$(sql "$mysql" 'SELECT MAX(version) FROM schema_migrations')"
accounts=$(sql "$drill-mysql" 'SELECT COUNT(*) FROM accounts')
check "ledger has data" "$([ "${accounts:-0}" -gt 0 ] && echo yes || echo "no ($accounts accounts)")" yes

workflows=$(on "$drill-temporal" "temporal workflow count --address localhost:7233" | grep -oE '[0-9]+' | tail -1)
check "Temporal has its workflows" "$([ "${workflows:-0}" -gt 0 ] && echo yes || echo "no ($workflows)")" yes
check "Temporal has the CardID attribute" "$(on "$drill-temporal" "temporal operator search-attribute list --address localhost:7233" | grep -c CardID)" 1

if [ "$failed" = 0 ]; then
  echo "restore drill passed: $accounts accounts, $workflows workflows restored"
else
  echo "restore drill FAILED" >&2
fi
exit "$failed"
