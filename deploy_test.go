package main

import (
	"os"
	osexec "os/exec" // "exec" is a MySQL test helper in this package
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// deploy/fly/deploy.sh against a stand-in flyctl and curl that model a worker
// serving a version: flyctl remembers apps and volumes, a deploy changes the
// running version (built at VERSION, or an earlier image by digest), and curl
// serves /healthz for whatever runs, degraded if that version is in $STUB/bad.
func TestDeployScript(t *testing.T) {
	for _, tool := range []string{"bash", "jq"} {
		if _, err := osexec.LookPath(tool); err != nil {
			t.Skip("needs " + tool)
		}
	}
	stub := t.TempDir()
	bin := filepath.Join(stub, "bin")
	for name, body := range map[string]string{
		"flyctl": `#!/usr/bin/env bash
echo "flyctl $*" >> "$STUB/calls"
case "$1 $2" in
  "status -a") [ -e "$STUB/app-$3" ] || exit 1 ;;
  "apps create") touch "$STUB/app-$3" ;;
  "volumes list") # a JSON array, as flyctl prints it
    echo "["; sep=""; for f in "$STUB"/vol-"$4"-*; do [ -e "$f" ] && { n=${f##*-$4-}; echo "$sep{\"name\": \"$n\", \"id\": \"vol_$n\"}"; sep=","; }; done; echo "]" ;;
  "volumes create") touch "$STUB/vol-$5-$3" ;;
  "image show")
    [ -s "$STUB/current" ] && echo "[{\"Registry\":\"registry.fly.io\",\"Repository\":\"$4\",\"Digest\":\"sha256:$(cat "$STUB/current")\"}]" || echo "[]" ;;
  "deploy "*)
    for a in "$@"; do
      case "$a" in
        VERSION=*) echo "${a#VERSION=}" > "$STUB/current" ;;
        *@sha256:*) echo "${a##*@sha256:}" > "$STUB/current" ;;
      esac
    done ;;
esac
exit 0
`,
		"curl": `#!/usr/bin/env bash
[ -s "$STUB/current" ] || exit 22 # nothing serving yet
v=$(cat "$STUB/current")
status=ok
grep -qx "$v" "$STUB/bad" 2>/dev/null && status="degraded: mysql: refused"
echo "{\"status\":\"$status\",\"uptime_s\":3,\"version\":\"$v\"}"
`,
	} {
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// deploy runs deploy.sh at version, with these versions unhealthy, and
	// returns the flyctl calls, the output, and the version left running.
	deploy := func(version string, bad []string, env ...string) (calls, out, running string, err error) {
		_ = os.Remove(filepath.Join(stub, "calls"))
		if err := os.WriteFile(filepath.Join(stub, "bad"), []byte(strings.Join(bad, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		cmd := osexec.Command("bash", "deploy/fly/deploy.sh")
		cmd.Env = append([]string{
			"PATH=" + bin + ":" + os.Getenv("PATH"), "STUB=" + stub,
			"SMOKE_TRIES=2", "SMOKE_WAIT=0", "VERSION=" + version,
		}, env...)
		o, err := cmd.CombinedOutput()
		c, _ := os.ReadFile(filepath.Join(stub, "calls"))
		r, _ := os.ReadFile(filepath.Join(stub, "current"))
		return string(c), string(o), strings.TrimSpace(string(r)), err
	}
	secrets := []string{"FLY_API_TOKEN=x", "CAS_MYSQL_PASSWORD=p@ss w0rd"}

	// A bad first deploy has nothing to roll back to.
	_, out, _, err := deploy("v0.1.0", []string{"0.1.0"}, secrets...)
	if err == nil || !strings.Contains(out, "no earlier release to roll back to") {
		t.Errorf("bad first deploy: err %v, want a failure with nothing to roll back to:\n%s", err, out)
	}
	_ = os.Remove(filepath.Join(stub, "current"))

	// First deploy: creates the stack, in dependency order, built at the tag.
	calls, out, running, err := deploy("v0.2.0", nil, secrets...)
	if err != nil || running != "0.2.0" {
		t.Fatalf("first deploy: err %v, running %q:\n%s", err, running, out)
	}
	if n := strings.Count(calls, "apps create") + strings.Count(calls, "volumes create"); n != 0 {
		t.Errorf("first deploy created %d more apps or volumes, want 0 (the bad first deploy made them):\n%s", n, calls)
	}
	last := -1
	for _, app := range []string{"-a cas-jamesponwith-mysql -c", "-a cas-jamesponwith-temporal -c", "-a cas-jamesponwith-worker -c"} {
		i := strings.Index(calls, app)
		if i < last {
			t.Errorf("deploy order wrong: want MySQL, then Temporal, then the worker:\n%s", calls)
		}
		last = i
	}
	if !strings.Contains(calls, "--build-arg VERSION=0.2.0") {
		t.Errorf("worker not built at version 0.2.0 (tag v0.2.0):\n%s", calls)
	}

	// A healthy upgrade: no rollback.
	calls, out, running, err = deploy("v0.3.0", nil, secrets...)
	if err != nil || running != "0.3.0" || strings.Contains(calls, "--image") {
		t.Errorf("good upgrade: err %v, running %q, rolled back %v:\n%s", err, running, strings.Contains(calls, "--image"), out)
	}

	// A broken release rolls back to the digest that was running, and still fails.
	calls, out, running, err = deploy("v0.4.0", []string{"0.4.0"}, secrets...)
	if err == nil || running != "0.3.0" || !strings.Contains(calls, "--image registry.fly.io/cas-jamesponwith-worker@sha256:0.3.0") ||
		!strings.Contains(out, "rolled back to 0.3.0") {
		t.Errorf("bad upgrade: err %v, running %q, want a failed job with 0.3.0 back by digest:\n%s\ncalls:\n%s", err, running, out, calls)
	}

	// If the old version can't come back either (a migration it refuses), say so.
	_, out, _, err = deploy("v0.5.0", []string{"0.5.0", "0.3.0"}, secrets...)
	if err == nil || !strings.Contains(out, "ROLLBACK FAILED") || !strings.Contains(out, "fix forward") {
		t.Errorf("failed rollback: err %v, want ROLLBACK FAILED and fix-forward advice:\n%s", err, out)
	}

	if _, _, _, err := deploy("v1", nil, "FLY_API_TOKEN=x"); err == nil {
		t.Error("deployed without CAS_MYSQL_PASSWORD")
	}
}

// Every flag the deploy scripts pass to flyctl exists in the flyctl that's
// installed: the stubs accept anything, and the first real deploy failed on a
// flag (deploy --region) that flyctl doesn't have. Skipped where flyctl isn't
// installed, such as CI; run it after bumping the pinned version.
func TestDeployScriptFlyctlFlags(t *testing.T) {
	if _, err := osexec.LookPath("flyctl"); err != nil {
		t.Skip("flyctl not installed")
	}
	cmd := regexp.MustCompile(`flyctl ((?:volumes snapshots|apps|volumes|secrets|image|ssh) \w+|deploy|status)\b([^|;&\n]*)`)
	flag := regexp.MustCompile(`--[a-z][a-z-]*`)
	// deploy.sh's own deploy helper passes its extra arguments to flyctl deploy.
	helper := regexp.MustCompile(`(?m)^deploy "\$\w+" \S+()(.*)$`)
	checked := 0
	for _, script := range []string{"deploy/fly/deploy.sh", "deploy/fly/restore-drill.sh"} {
		src, err := os.ReadFile(script)
		if err != nil {
			t.Fatal(err)
		}
		calls := cmd.FindAllStringSubmatch(string(src), -1)
		for _, m := range helper.FindAllStringSubmatch(string(src), -1) {
			calls = append(calls, []string{m[0], "deploy", m[2]})
		}
		for _, m := range calls {
			help, err := osexec.Command("flyctl", append(strings.Fields(m[1]), "--help")...).CombinedOutput()
			if err != nil {
				t.Fatalf("%s: flyctl %s --help: %v", script, m[1], err)
			}
			for _, f := range flag.FindAllString(m[2], -1) {
				checked++
				if !regexp.MustCompile(`\s` + regexp.QuoteMeta(f) + `\b`).Match(help) {
					t.Errorf("%s: flyctl %s has no %s flag", script, m[1], f)
				}
			}
		}
	}
	if checked < 25 {
		t.Errorf("checked only %d flags; the command pattern no longer matches the scripts", checked)
	}
}

// restore-drill.sh against a stand-in flyctl whose ssh answers the drill's
// checks: it passes on a good restore, fails on a broken one, takes a snapshot
// when a volume has none, and always destroys the drill apps and only those.
func TestRestoreDrillScript(t *testing.T) {
	for _, tool := range []string{"bash", "jq"} {
		if _, err := osexec.LookPath(tool); err != nil {
			t.Skip("needs " + tool)
		}
	}
	stub := t.TempDir()
	bin := filepath.Join(stub, "bin")
	flyctl := `#!/usr/bin/env bash
echo "flyctl $*" >> "$STUB/calls"
case "$1 $2" in
  "volumes list") echo "[{\"name\":\"mysql_data\",\"id\":\"vol_m\"},{\"name\":\"temporal_data\",\"id\":\"vol_t\"}]" ;;
  "volumes snapshots")
    case "$3" in
      list) [ -e "$STUB/snap-$4" ] && echo "[{\"id\":\"snap_$4\",\"status\":\"created\",\"created_at\":\"2026-10-16T00:00:00Z\"}]" || echo "[]" ;;
      create) touch "$STUB/snap-$4" ;;
    esac ;;
  "image show") echo "[{\"Registry\":\"registry.fly.io\",\"Repository\":\"$4\",\"Digest\":\"sha256:abc\"}]" ;;
  "ssh console")
    cmd=${*: -1}
    if [[ "$cmd" == *"base64 -d"* ]]; then # a query sent base64-encoded, as sql() does
      q=${cmd#*echo }; cmd=$(echo "${q%% *}" | base64 -d)
      [[ "$cmd" == *"'card:%'"* || "$cmd" != *card* ]] || { echo "quotes lost in: $cmd" >&2; exit 1; }
    fi
    case "$cmd" in
      *"COALESCE(SUM(balance)"*) echo "${LEDGER_SUM:-0}" ;;
      *"HAVING s <> 0"*) echo 0 ;;
      *"MAX(version)"*) echo 6 ;;
      *"COUNT(*) FROM accounts"*) echo 42 ;;
      *"workflow count"*) echo "Total: 7" ;;
      *"search-attribute list"*) echo "CardID  Keyword" ;;
    esac ;;
esac
exit 0
`
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "flyctl"), []byte(flyctl), 0o755); err != nil {
		t.Fatal(err)
	}
	drill := func(env ...string) (calls, out string, err error) {
		_ = os.Remove(filepath.Join(stub, "calls"))
		cmd := osexec.Command("bash", "deploy/fly/restore-drill.sh")
		cmd.Env = append([]string{"PATH=" + bin + ":" + os.Getenv("PATH"), "STUB=" + stub,
			"FLY_API_TOKEN=x", "CAS_MYSQL_PASSWORD=pw", "SNAPSHOT_WAIT=2", "CHECK_TRIES=1"}, env...)
		o, err := cmd.CombinedOutput()
		c, _ := os.ReadFile(filepath.Join(stub, "calls"))
		return string(c), string(o), err
	}
	destroyed := func(calls string) []string {
		var apps []string
		for _, line := range strings.Split(calls, "\n") {
			if rest, ok := strings.CutPrefix(line, "flyctl apps destroy "); ok {
				apps = append(apps, strings.Fields(rest)[0])
			}
		}
		return apps
	}

	calls, out, err := drill()
	if err != nil || !strings.Contains(out, "restore drill passed: 42 accounts, 7 workflows restored") {
		t.Errorf("good restore: err %v:\n%s", err, out)
	}
	if strings.Count(calls, "volumes snapshots create") != 2 {
		t.Errorf("volumes without snapshots: want one taken for each:\n%s", calls)
	}
	if !strings.Contains(calls, "--snapshot-id snap_vol_m") || !strings.Contains(calls, "--image registry.fly.io/cas-jamesponwith-mysql@sha256:abc") {
		t.Errorf("restore didn't use the snapshot and production's image:\n%s", calls)
	}
	for _, app := range destroyed(calls) {
		if !strings.Contains(app, "-drill-") {
			t.Fatalf("destroyed %s, which isn't a drill app", app)
		}
	}
	if n := len(destroyed(calls)); n != 2 {
		t.Errorf("destroyed %d apps, want the 2 drill apps", n)
	}

	calls, out, err = drill("LEDGER_SUM=5") // a restored ledger that doesn't balance
	if err == nil || !strings.Contains(out, "FAIL ledger sums to zero") {
		t.Errorf("broken restore: err %v, want the drill to fail:\n%s", err, out)
	}
	if strings.Contains(calls, "volumes snapshots create") {
		t.Errorf("took new snapshots although the volumes have some:\n%s", calls)
	}
	if n := len(destroyed(calls)); n != 2 {
		t.Errorf("after a failed drill, destroyed %d apps, want the 2 drill apps", n)
	}
}
