package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/carbynestack/castor-go/internal/testenv"
)

func TestMain(m *testing.M) {
	if os.Getenv("CASTOR_TEST_AS_MAIN") == "1" { // the test binary doubles as the castor binary
		os.Args = append([]string{"castor"}, strings.Fields(os.Getenv("CASTOR_TEST_ARGS"))...)
		main()
		return
	}
	code := m.Run()
	testenv.Shutdown()
	os.Exit(code)
}

func portOf(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	_, p, _ := net.SplitHostPort(ts.Listener.Addr().String())
	return p
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, p, _ := net.SplitHostPort(l.Addr().String())
	return p
}

func TestHealthcheckExitCodes(t *testing.T) {
	status := 200
	hang := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/actuator/health" {
			w.WriteHeader(404)
			return
		}
		if status == -1 {
			<-hang
		}
		w.WriteHeader(status)
	}))
	defer ts.Close()
	defer close(hang)
	t.Setenv("CASTOR_SERVICE_PORT", portOf(t, ts))
	for _, c := range []struct {
		status, want int
	}{{200, 0}, {204, 1}, {301, 1}, {404, 1}, {500, 1}, {503, 1}} {
		status = c.status
		if got := healthcheck(); got != c.want {
			t.Errorf("status %d: exit code %d, want %d", c.status, got, c.want)
		}
	}
	// nothing listening
	ts.Close()
	if got := healthcheck(); got != 1 {
		t.Fatalf("service down: %d", got)
	}
	t.Setenv("CASTOR_SERVICE_PORT", "notaport")
	if got := healthcheck(); got != 1 {
		t.Fatalf("garbage port: %d", got)
	}
	// default port 10100 when unset: nothing there either
	t.Setenv("CASTOR_SERVICE_PORT", "")
	if l, err := net.Listen("tcp", "127.0.0.1:10100"); err == nil {
		l.Close()
		if got := healthcheck(); got != 1 {
			t.Fatalf("default port: %d", got)
		}
	}
}

// The binary's `healthcheck` subcommand, run for real: its process exit code is what Docker evaluates.
func TestHealthcheckSubcommandProcessExit(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer bad.Close()
	for _, c := range []struct {
		port string
		want int
	}{{portOf(t, ok), 0}, {portOf(t, bad), 1}, {freePort(t), 1}} {
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(), "CASTOR_TEST_AS_MAIN=1", "CASTOR_TEST_ARGS=healthcheck", "CASTOR_SERVICE_PORT="+c.port)
		err := cmd.Run()
		code := 0
		if ee, isExit := err.(*exec.ExitError); isExit {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		if code != c.want {
			t.Errorf("port %s: exit code %d, want %d", c.port, code, c.want)
		}
	}
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"CASTOR_SERVICE_PORT", "IS_MASTER", "SLAVE_URIS", "NO_SSL_VALIDATION", "TRUSTED_CERTIFICATES", "INITIAL_FRAGMENT_SIZE",
		"RESERVATION_TIMEOUT", "RESERVATION_RETRY_DELAY", "REDIS_HOST", "REDIS_PORT", "MINIO_ENDPOINT", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "LOG_LEVEL"} {
		t.Setenv(k, "")
	}
}

// Startup failures are reported by run() before anything is served.
func TestRunStartupFailures(t *testing.T) {
	for _, c := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"invalid number", map[string]string{"CASTOR_SERVICE_PORT": "x"}, `invalid CASTOR_SERVICE_PORT="x"`},
		{"invalid boolean", map[string]string{"IS_MASTER": "maybe"}, `invalid IS_MASTER="maybe"`},
		{"fragment size", map[string]string{"INITIAL_FRAGMENT_SIZE": "0"}, "INITIAL_FRAGMENT_SIZE must be >= 1"},
		{"no redis", map[string]string{"MINIO_ENDPOINT": "http://m"}, "REDIS_HOST is required"},
		{"no minio", map[string]string{"REDIS_HOST": "r"}, "MINIO_ENDPOINT is required"},
		{"database unreachable", map[string]string{"REDIS_HOST": "r", "MINIO_ENDPOINT": "http://m", "DB_HOST": "127.0.0.1", "DB_PORT": "1", "DB_USER": "u"}, "postgres: "},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			done := make(chan error, 1)
			go func() { done <- run() }()
			select {
			case err := <-done:
				if err == nil || !strings.HasPrefix(err.Error(), c.want) {
					t.Fatalf("got %v, want prefix %q", err, c.want)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("run did not return")
			}
		})
	}
}

func pgEnv(t *testing.T) map[string]string {
	t.Helper()
	u, err := url.Parse(testenv.PostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	pw, _ := u.User.Password()
	return map[string]string{"DB_HOST": u.Hostname(), "DB_PORT": u.Port(), "DB_USER": u.User.Username(), "DB_PASSWORD": pw, "DB_NAME": strings.TrimPrefix(u.Path, "/")}
}

func TestRunMinioUnreachable(t *testing.T) {
	clearEnv(t)
	env := pgEnv(t)
	env["REDIS_HOST"], env["MINIO_ENDPOINT"], env["CASTOR_SERVICE_PORT"] = "127.0.0.1", "http://127.0.0.1:1", freePort(t)
	for k, v := range env {
		t.Setenv(k, v)
	}
	err := run()
	if err == nil || !strings.HasPrefix(err.Error(), "minio: ") {
		t.Fatalf("got %v", err)
	}
}

// Full start-up against the containers: health goes UP, the healthcheck command succeeds, SIGTERM
// shuts the service down cleanly, afterwards the healthcheck fails.
func TestRunServesAndShutsDownOnSIGTERM(t *testing.T) {
	clearEnv(t)
	env := pgEnv(t)
	redisHost, redisPort, _ := net.SplitHostPort(testenv.RedisAddr(t))
	port := freePort(t)
	env["REDIS_HOST"], env["REDIS_PORT"], env["MINIO_ENDPOINT"] = redisHost, redisPort, testenv.MinioEndpoint(t)
	env["CASTOR_SERVICE_PORT"], env["MINIO_BUCKET"], env["IS_MASTER"] = port, "x", "true"
	for k, v := range env {
		t.Setenv(k, v)
	}
	done := make(chan error, 1)
	go func() { done <- run() }()
	base := "http://127.0.0.1:" + port
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get(base + "/actuator/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		select {
		case err := <-done:
			t.Fatalf("run ended early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("service did not become healthy")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := healthcheck(); got != 0 {
		t.Fatalf("healthcheck of the running service: %d", got)
	}
	resp, err := http.Get(base + "/actuator/info")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no shutdown after SIGTERM")
	}
	if got := healthcheck(); got != 1 {
		t.Fatalf("healthcheck after shutdown: %d", got)
	}
}

func TestLoggerLevels(t *testing.T) {
	for _, lv := range []string{"DEBUG", "TRACE", "INFO", "WARN", "ERROR", "", "nonsense"} {
		if logger(lv) == nil {
			t.Fatal(lv)
		}
	}
}
