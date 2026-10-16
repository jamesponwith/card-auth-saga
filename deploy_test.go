package main

import (
	"os"
	osexec "os/exec" // "exec" is a MySQL test helper in this package
	"path/filepath"
	"strings"
	"testing"
)

// deploy/fly/deploy.sh against a stand-in flyctl (which remembers apps and
// volumes) and curl (which returns a canned /healthz): it creates what's
// missing and nothing else, deploys in dependency order, and succeeds only
// once the public /healthz reports ok at this version.
func TestDeployScript(t *testing.T) {
	if _, err := osexec.LookPath("bash"); err != nil {
		t.Skip("needs bash")
	}
	stub := t.TempDir()
	bin := filepath.Join(stub, "bin")
	for name, body := range map[string]string{
		"flyctl": `#!/usr/bin/env bash
echo "flyctl $*" >> "$STUB/calls"
case "$1 $2" in
  "status -a") [ -e "$STUB/app-$3" ] || exit 1 ;;
  "apps create") touch "$STUB/app-$3" ;;
  "volumes list") for f in "$STUB"/vol-"$4"-*; do [ -e "$f" ] && echo "{\"name\": \"${f##*-$4-}\"}"; done ;;
  "volumes create") touch "$STUB/vol-$5-$3" ;;
esac
exit 0
`,
		"curl": "#!/usr/bin/env bash\necho \"$HEALTH\"\n",
	} {
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	run := func(health string, env ...string) (calls string, err error) {
		_ = os.Remove(filepath.Join(stub, "calls"))
		cmd := osexec.Command("bash", "deploy/fly/deploy.sh")
		cmd.Env = append([]string{
			"PATH=" + bin + ":" + os.Getenv("PATH"), "STUB=" + stub, "HEALTH=" + health,
			"SMOKE_TRIES=2", "SMOKE_WAIT=0",
		}, env...)
		out, err := cmd.CombinedOutput()
		c, _ := os.ReadFile(filepath.Join(stub, "calls"))
		if err != nil {
			t.Logf("deploy.sh: %s", out)
		}
		return string(c), err
	}
	secrets := []string{"FLY_API_TOKEN=x", "CAS_MYSQL_PASSWORD=p@ss w0rd", "VERSION=v0.2.0"}
	healthy := `{"status":"ok","uptime_s":3,"version":"0.2.0"}`

	calls, err := run(healthy, secrets...)
	if err != nil {
		t.Fatalf("first deploy: %v", err)
	}
	if n := strings.Count(calls, "apps create") + strings.Count(calls, "volumes create"); n != 5 {
		t.Errorf("first deploy created %d apps and volumes, want 5 (3 apps, 2 volumes):\n%s", n, calls)
	}
	order := []string{"-a cas-jamesponwith-mysql -c", "-a cas-jamesponwith-temporal -c", "-a cas-jamesponwith-worker -c"}
	last := -1
	for _, app := range order {
		i := strings.Index(calls, app)
		if i < last {
			t.Errorf("deploy order wrong: want MySQL, then Temporal, then the worker:\n%s", calls)
		}
		last = i
	}
	if !strings.Contains(calls, "--build-arg VERSION=0.2.0") {
		t.Errorf("worker not built at version 0.2.0 (tag v0.2.0):\n%s", calls)
	}

	calls, err = run(healthy, secrets...)
	if err != nil || strings.Count(calls, "create") != 0 || strings.Count(calls, " deploy ") != 3 {
		t.Errorf("second deploy: err %v, %d creates, %d deploys; want none created, 3 deployed", err, strings.Count(calls, "create"), strings.Count(calls, " deploy "))
	}

	for name, health := range map[string]string{
		"degraded":    `{"status":"degraded: mysql: refused","uptime_s":3,"version":"0.2.0"}`,
		"old version": `{"status":"ok","uptime_s":3,"version":"0.1.0"}`,
		"no response": "",
	} {
		if _, err := run(health, secrets...); err == nil {
			t.Errorf("%s /healthz: deploy succeeded, want the smoke test to fail it", name)
		}
	}
	if _, err := run(healthy, "FLY_API_TOKEN=x", "VERSION=v1"); err == nil {
		t.Error("deployed without CAS_MYSQL_PASSWORD")
	}
}
