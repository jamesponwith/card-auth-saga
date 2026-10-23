package main

import (
	"net"
	"os"
	osexec "os/exec" // "exec" is a MySQL test helper in this package
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// deployStubs stand in for flyctl and curl, modelling a worker serving a
// version: flyctl remembers apps and volumes, a deploy changes the running
// version (built at VERSION, or an earlier image by digest), and curl serves
// /healthz for whatever runs, degraded if that version is in $STUB/bad.
var deployStubs = map[string]string{
	"flyctl": `#!/usr/bin/env bash
echo "flyctl $*" >> "$STUB/calls"
case "$1 $2" in
  "status -a") [ -e "$STUB/app-$3" ] || exit 1 ;;
  "apps destroy") rm -f "$STUB/app-$3" ;;
  "apps create") [ -n "$DENY_CREATE" ] && { echo "Error: unauthorized" >&2; exit 1; }; touch "$STUB/app-$3" ;;
  "volumes list") # a JSON array, as flyctl prints it
    echo "["; sep=""; for f in "$STUB"/vol-"$4"-*; do [ -e "$f" ] && { n=${f##*-$4-}; echo "$sep{\"name\": \"$n\", \"id\": \"vol_$n\"}"; sep=","; }; done; echo "]" ;;
  "volumes create") touch "$STUB/vol-$5-$3" ;;
  "ips list") echo "[{\"Type\":\"v6\",\"Address\":\"2a09::1\"},{\"Type\":\"shared_v4\",\"Address\":\"66.0.0.7\"}]" ;;
  "image show")
    [ -s "$STUB/current" ] && echo "[{\"Registry\":\"registry.fly.io\",\"Repository\":\"$4\",\"Digest\":\"sha256:$(cat "$STUB/current")\"}]" || echo "[]" ;;
  "deploy "*)
    for a in "$@"; do
      case "$a" in
        VERSION=*) echo "${a#VERSION=}" > "$STUB/current" ;;
        *@sha256:*) echo "${a##*@sha256:}" > "$STUB/current" ;;
        *.toml*) grep -h "size = " "$a" >> "$STUB/calls" ;; # a drill's machine size
      esac
    done ;;
esac
exit 0
`,
	"curl": `#!/usr/bin/env bash
echo "curl $*" >> "$STUB/calls"
[ -s "$STUB/current" ] || exit 22 # nothing serving yet
v=$(cat "$STUB/current")
status=ok
grep -qx "$v" "$STUB/bad" 2>/dev/null && status="degraded: mysql: refused"
echo "{\"status\":\"$status\",\"uptime_s\":3,\"version\":\"$v\"}"
`,
}

// writeStubs writes stand-in commands into a temporary bin directory, returning
// the directory their state lives in ($STUB) and the bin directory to put first on PATH.
func writeStubs(t *testing.T, stubs ...map[string]string) (stub, bin string) {
	stub = t.TempDir()
	bin = filepath.Join(stub, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, m := range stubs {
		for name, body := range m {
			if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	return stub, bin
}

// deploy/fly/deploy.sh against deployStubs.
func TestDeployScript(t *testing.T) {
	t.Parallel() // each runs its script against stubs of its own
	for _, tool := range []string{"bash", "jq"} {
		if _, err := osexec.LookPath(tool); err != nil {
			t.Skip("needs " + tool)
		}
	}
	stub, bin := writeStubs(t, deployStubs)
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
	if !strings.Contains(calls, "--resolve cas-jamesponwith-worker.fly.dev:443:66.0.0.7 https://cas-jamesponwith-worker.fly.dev/healthz") {
		t.Errorf("smoke test didn't reach the worker's shared IPv4 by name, past DNS:\n%s", calls)
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

	// An app-scoped deploy token can't create a missing app: say how to fix it.
	_ = os.Remove(filepath.Join(stub, "app-cas-jamesponwith-temporal"))
	_, out, _, err = deploy("v0.6.0", nil, append(secrets, "DENY_CREATE=1")...)
	if err == nil || !strings.Contains(out, "fly apps create cas-jamesponwith-temporal --org personal") {
		t.Errorf("missing app with a deploy token: err %v, want instructions to create it:\n%s", err, out)
	}
}

// Every flag the deploy scripts pass to flyctl exists in the flyctl that's
// installed: the stubs accept anything, and the first real deploy failed on a
// flag (deploy --region) that flyctl doesn't have. Skipped where flyctl isn't
// installed, such as CI; run it after bumping the pinned version.
func TestDeployScriptFlyctlFlags(t *testing.T) {
	t.Parallel() // each runs its script against stubs of its own
	if _, err := osexec.LookPath("flyctl"); err != nil {
		t.Skip("flyctl not installed")
	}
	cmd := regexp.MustCompile(`flyctl ((?:volumes snapshots|apps|volumes|secrets|image|ssh) \w+|deploy|status|proxy)\b([^|;&\n]*)`)
	flag := regexp.MustCompile(`--[a-z][a-z-]*`)
	// deploy.sh's own deploy helper passes its extra arguments to flyctl deploy.
	helper := regexp.MustCompile(`(?m)^deploy "\$\w+" \S+()(.*)$`)
	checked := 0
	for _, script := range []string{"deploy/fly/deploy.sh", "deploy/fly/restore-drill.sh", "deploy/fly/load-drill.sh"} {
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
	t.Parallel() // each runs its script against stubs of its own
	for _, tool := range []string{"bash", "jq"} {
		if _, err := osexec.LookPath(tool); err != nil {
			t.Skip("needs " + tool)
		}
	}
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
  "ips list") echo '[{"Type":"shared_v4","Address":"66.0.0.9"}]' ;;
  "deploy -a") [[ "$*" == *worker.toml* ]] && touch "$STUB/upgraded" ;;
  "ssh console")
    cmd=${*: -1}
    if [[ "$cmd" == *"base64 -d"* ]]; then # a query sent base64-encoded, as sql() does
      q=${cmd#*echo }; cmd=$(echo "${q%% *}" | base64 -d)
      [[ "$cmd" == *"'card:%'"* || "$cmd" != *LIKE* ]] || { echo "quotes lost in: $cmd" >&2; exit 1; }
    fi
    case "$cmd" in
      *"COALESCE(SUM(balance)"*) echo "${LEDGER_SUM:-0}" ;;
      *"HAVING s <> 0"*) echo 0 ;;
      *"MAX(version)"*) [[ -e "$STUB/upgraded" && "$*" == *-drill-* ]] && echo 8 || echo 6 ;; # migrated by the rehearsal
      *"NOT IN (SELECT number"*) echo 0 ;;
      *"workflow list"*"ExecutionStatus='Running'"*) echo '[{"execution":{"workflowId":"statement:smoke-1"}},{"execution":{"workflowId":"auth-9"}}]' ;;
      *"workflow stack"*) [[ "$cmd" == *"'${BAD_REPLAY:-none}'"* ]] && exit 1; echo "coroutine root" ;;
      *"/card-auth-saga audit"*) echo "audit: clean" ;;
      *"/card-auth-saga authorize"*) set -- $cmd; [[ "$4" == *-b ]] && echo "approved: $3" || echo "declined: $3 (Replaced)" ;;
      *"/card-auth-saga "*) echo "ok: ${cmd#/card-auth-saga }" ;;
      *"COUNT(*) FROM accounts"*) echo 42 ;;
      *"workflow count"*) echo "Total: 7" ;;
      *"search-attribute list"*) echo "CardID  Keyword" ;;
    esac ;;
esac
exit 0
`
	curl := `#!/usr/bin/env bash
echo "curl $*" >> "$STUB/calls"
echo "{\"status\":\"ok\",\"version\":\"$UPGRADE\"}"
`
	stub, bin := writeStubs(t, map[string]string{"flyctl": flyctl, "curl": curl})
	drill := func(env ...string) (calls, out string, err error) {
		_ = os.Remove(filepath.Join(stub, "calls"))
		_ = os.Remove(filepath.Join(stub, "upgraded"))
		cmd := osexec.Command("bash", "deploy/fly/restore-drill.sh")
		cmd.Env = append([]string{"PATH=" + bin + ":" + os.Getenv("PATH"), "STUB=" + stub,
			"FLY_API_TOKEN=x", "CAS_MYSQL_PASSWORD=pw", "SNAPSHOT_WAIT=2", "CHECK_TRIES=1", "CHECK_WAIT=0"}, env...)
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
	if strings.Contains(calls, "worker.toml") {
		t.Errorf("a plain restore drill deployed a worker:\n%s", calls)
	}

	// The release rehearsal (ADR 0011): this checkout, as a worker on the copy.
	calls, out, err = drill("UPGRADE=abc123")
	if err != nil || !strings.Contains(out, "upgrade drill passed: abc123 on production's data, 2 running workflows replayed") {
		t.Errorf("rehearsal: err %v:\n%s", err, out)
	}
	if !strings.Contains(calls, "--build-arg VERSION=abc123") || !strings.Contains(calls, "--resolve") {
		t.Errorf("worker not built at the rehearsed version, or health not checked by IP:\n%s", calls)
	}
	if strings.Contains(calls, "cas-jamesponwith-temporal.internal") || strings.Contains(calls, "cas-jamesponwith-mysql.internal") {
		t.Errorf("the rehearsal worker points at production's Temporal or MySQL:\n%s", calls)
	}
	if !regexp.MustCompile(`TEMPORAL_ADDRESS=cas-jamesponwith-drill-\d+-temporal\.internal:7233`).MatchString(calls) {
		t.Errorf("the rehearsal worker doesn't point at the drill's Temporal:\n%s", calls)
	}
	if apps := destroyed(calls); len(apps) != 3 {
		t.Errorf("rehearsal destroyed %v, want the 3 drill apps, worker included", apps)
	}
	for _, app := range destroyed(calls) {
		if !strings.Contains(app, "-drill-") {
			t.Fatalf("destroyed %s, which isn't a drill app", app)
		}
	}

	_, out, err = drill("UPGRADE=abc123", "BAD_REPLAY=auth-9") // the new code can't replay a production run
	if err == nil || !strings.Contains(out, "FAIL production's running workflows replay on abc123: got ' auth-9 (1 replayed)'") {
		t.Errorf("a run that won't replay: err %v, want the rehearsal to fail naming it:\n%s", err, out)
	}
}

// load-drill.sh against deployStubs, a stand-in go that records what TestLoad
// would get, and real listeners for flyctl proxy's ports: it loads a fresh
// drill stack with a password of its own, and destroys that stack and only
// that, whether the load passes, fails, or never starts.
func TestLoadDrillScript(t *testing.T) {
	t.Parallel() // each runs its script against stubs of its own
	for _, tool := range []string{"bash", "jq", "openssl"} {
		if _, err := osexec.LookPath(tool); err != nil {
			t.Skip("needs " + tool)
		}
	}
	stub, bin := writeStubs(t, deployStubs, map[string]string{"go": `#!/usr/bin/env bash
echo "go $* TEMPORAL=$CAS_LOAD_TEMPORAL DSN=$CAS_MYSQL_DSN" >> "$STUB/go"
exit "${GO_EXIT:-0}"
`})
	port := func() string { // a listening port, standing in for a flyctl proxy that came up
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	}
	tport, mport := port(), port()
	drill := func(env ...string) (calls, gocall, out string, err error) {
		for _, f := range []string{"calls", "go", "current"} {
			_ = os.Remove(filepath.Join(stub, f))
		}
		cmd := osexec.Command("bash", "deploy/fly/load-drill.sh")
		cmd.Env = append([]string{"PATH=" + bin + ":" + os.Getenv("PATH"), "STUB=" + stub,
			"FLY_API_TOKEN=x", "CAS_MYSQL_PASSWORD=production-pw", "VERSION=v0.9.0", "SMOKE_TRIES=1", "SMOKE_WAIT=0",
			"LOAD_TEMPORAL_PORT=" + tport, "LOAD_MYSQL_PORT=" + mport, "PROXY_TRIES=2"}, env...)
		o, err := cmd.CombinedOutput()
		c, _ := os.ReadFile(filepath.Join(stub, "calls"))
		g, _ := os.ReadFile(filepath.Join(stub, "go"))
		return string(c), string(g), string(o), err
	}
	// left lists the apps still standing, and fails on any outside the drill.
	left := func(calls string) []string {
		for _, line := range strings.Split(calls, "\n") {
			if strings.Contains(line, " -a ") && !strings.Contains(line, "-drill-load-") {
				t.Errorf("touched an app outside the drill: %s", line)
			}
		}
		apps, _ := filepath.Glob(filepath.Join(stub, "app-*"))
		return apps
	}

	calls, gocall, out, err := drill()
	if err != nil {
		t.Fatalf("good drill: %v:\n%s", err, out)
	}
	if !strings.Contains(gocall, "go test -run ^TestLoad$ ") || !strings.Contains(gocall, "TEMPORAL=127.0.0.1:"+tport) ||
		!strings.Contains(gocall, "@tcp(127.0.0.1:"+mport+")/cas") {
		t.Errorf("TestLoad not pointed at the proxied drill stack: %s", gocall)
	}
	if strings.Contains(gocall+calls, "production-pw") {
		t.Errorf("the drill used production's MySQL password")
	}
	if n := strings.Count(calls, "apps destroy"); n != 3 {
		t.Errorf("destroyed %d apps, want the 3 drill apps:\n%s", n, calls)
	}
	if apps := left(calls); len(apps) != 0 {
		t.Errorf("drill apps left standing: %v", apps)
	}

	if calls, _, out, err := drill("FLY_VM_SIZE=performance-1x"); err != nil || strings.Count(calls, `size = "performance-1x"`) != 3 {
		t.Errorf("FLY_VM_SIZE: err %v, want all 3 machines resized:\n%s\n%s", err, out, calls)
	}

	for _, tt := range []struct {
		name, env, want string
		loaded          bool
	}{
		{"load fails", "GO_EXIT=1", "", true},
		{"deploy fails", "", "never reported ok", false},
		{"proxy never comes up", "LOAD_MYSQL_PORT=1", "proxy on port 1 never came up", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "deploy fails" { // the worker never reports ok
				if err := os.WriteFile(filepath.Join(stub, "bad"), []byte("0.9.0\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				defer func() { _ = os.Remove(filepath.Join(stub, "bad")) }()
			}
			calls, gocall, out, err := drill(tt.env)
			if err == nil || !strings.Contains(out, tt.want) || (gocall != "") != tt.loaded {
				t.Errorf("err %v, loaded %v; want a failure saying %q, loaded %v:\n%s", err, gocall != "", tt.want, tt.loaded, out)
			}
			if apps := left(calls); len(apps) != 0 {
				t.Errorf("drill apps left standing after a failure: %v", apps)
			}
		})
	}
}
