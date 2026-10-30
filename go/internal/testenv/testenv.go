// Package testenv provides throw-away Postgres, Redis and Minio instances for the integration
// style tests (the Java *IT classes used Testcontainers, which does not work with every Docker
// version, so plain `docker run` is used instead).
//
// Each service can also be supplied from outside through CASTOR_TEST_PG (DSN), CASTOR_TEST_REDIS
// (host:port) and CASTOR_TEST_MINIO (endpoint URL). If neither the variable nor a usable Docker
// daemon is available the calling test is skipped. Containers are named castortest-*, started
// lazily once per test process and removed by Shutdown (call it from TestMain).
package testenv

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Images and credentials match the Java testconfig / chart versions.
const (
	pgImage    = "postgres:11.1"
	redisImage = "redis:7.0"
	minioImage = "quay.io/minio/minio:RELEASE.2022-04-16T04-26-02Z"

	MinioAccessKey = "dummy-key"
	MinioSecretKey = "dummy-secret"
)

type instance struct {
	once sync.Once
	addr string
	err  error
}

var (
	pg, redis, minio instance
	mu               sync.Mutex
	started          []string
)

func dockerAvailable() bool {
	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}
	return exec.Command("docker", "info").Run() == nil
}

// run starts a detached container publishing containerPort on a random loopback port and returns
// the host:port it is reachable on.
func run(name, containerPort string, args ...string) (string, error) {
	full := fmt.Sprintf("castortest-%s-%d", name, os.Getpid())
	a := append([]string{"run", "-d", "--rm", "--name", full, "-p", "127.0.0.1::" + containerPort}, args...)
	if out, err := exec.Command("docker", a...).CombinedOutput(); err != nil {
		return "", fmt.Errorf("docker run %s: %v: %s", name, err, out)
	}
	mu.Lock()
	started = append(started, full)
	mu.Unlock()
	out, err := exec.Command("docker", "port", full, containerPort).Output()
	if err != nil {
		return "", err
	}
	line := strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0]
	_, port, err := net.SplitHostPort(line)
	if err != nil {
		return "", err
	}
	return "127.0.0.1:" + port, nil
}

func waitFor(d time.Duration, what string, ready func() error) error {
	deadline := time.Now().Add(d)
	var err error
	for time.Now().Before(deadline) {
		if err = ready(); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("%s not ready after %s: %v", what, d, err)
}

func get(t testing.TB, env string, in *instance, start func() (string, error)) string {
	t.Helper()
	if v := os.Getenv(env); v != "" {
		return v
	}
	if !dockerAvailable() {
		t.Skipf("%s not set and docker is unavailable", env)
	}
	in.once.Do(func() { in.addr, in.err = start() })
	if in.err != nil {
		t.Fatalf("starting container: %v", in.err)
	}
	return in.addr
}

// PostgresDSN returns a DSN for an empty, ready Postgres 11.1 database "castor".
func PostgresDSN(t testing.TB) string {
	return get(t, "CASTOR_TEST_PG", &pg, func() (string, error) {
		addr, err := run("pg", "5432", "-e", "POSTGRES_PASSWORD=pw", "-e", "POSTGRES_DB=castor", pgImage)
		if err != nil {
			return "", err
		}
		dsn := "postgres://postgres:pw@" + addr + "/castor?sslmode=disable"
		return dsn, waitFor(60*time.Second, "postgres", func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			c, err := pgx.Connect(ctx, dsn)
			if err != nil {
				return err
			}
			return c.Close(ctx)
		})
	})
}

// RedisAddr returns host:port of a ready Redis 7.0.
func RedisAddr(t testing.TB) string {
	return get(t, "CASTOR_TEST_REDIS", &redis, func() (string, error) {
		addr, err := run("redis", "6379", redisImage)
		if err != nil {
			return "", err
		}
		return addr, waitFor(30*time.Second, "redis", func() error {
			c, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				return err
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(time.Second))
			if _, err := c.Write([]byte("PING\r\n")); err != nil {
				return err
			}
			b := make([]byte, 16)
			n, err := c.Read(b)
			if err != nil || !strings.HasPrefix(string(b[:n]), "+PONG") {
				return fmt.Errorf("unexpected reply %q: %v", b[:n], err)
			}
			return nil
		})
	})
}

// MinioEndpoint returns the http:// endpoint of a ready Minio (credentials MinioAccessKey/MinioSecretKey).
func MinioEndpoint(t testing.TB) string {
	return get(t, "CASTOR_TEST_MINIO", &minio, func() (string, error) {
		addr, err := run("minio", "9000", "-e", "MINIO_ROOT_USER="+MinioAccessKey, "-e", "MINIO_ROOT_PASSWORD="+MinioSecretKey, minioImage, "server", "/data")
		if err != nil {
			return "", err
		}
		// the health endpoint answers before the object layer is initialised: ask for the bucket list
		return "http://" + addr, waitFor(60*time.Second, "minio", func() error {
			c, err := miniogo.New(addr, &miniogo.Options{Creds: credentials.NewStaticV4(MinioAccessKey, MinioSecretKey, "")})
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err = c.ListBuckets(ctx)
			return err
		})
	})
}

// Shutdown removes every container started by this process. Call it from TestMain.
func Shutdown() {
	mu.Lock()
	defer mu.Unlock()
	for _, n := range started {
		_ = exec.Command("docker", "rm", "-f", n).Run()
	}
	started = nil
}
