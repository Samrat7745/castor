// Package testenv provides throw-away Postgres, Redis and Minio instances for the integration
// style tests (the Java *IT classes used Testcontainers; this is the Go port of it,
// testcontainers-go, which negotiates the Docker API version and therefore works with current
// Docker engines).
//
// Each service can also be supplied from outside through CASTOR_TEST_PG (DSN), CASTOR_TEST_REDIS
// (host:port) and CASTOR_TEST_MINIO (endpoint URL). If neither the variable nor a usable Docker
// daemon is available the calling test is skipped. Containers are started lazily once per test
// process and terminated by Shutdown (call it from TestMain); testcontainers' reaper removes them
// should the process die before that.
package testenv

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
	started          []testcontainers.Container
)

func dockerAvailable() bool {
	p, err := testcontainers.NewDockerProvider()
	if err != nil {
		return false
	}
	defer p.Close()
	return p.Health(context.Background()) == nil
}

// run starts a container exposing port on a random host port and returns host:port once req's wait
// strategy has succeeded.
func run(req testcontainers.ContainerRequest, port string) (string, error) {
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if c != nil {
		mu.Lock()
		started = append(started, c)
		mu.Unlock()
	}
	if err != nil {
		return "", err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}
	mapped, err := c.MappedPort(ctx, port)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, mapped.Port()), nil
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
		var dsn string
		addr, err := run(testcontainers.ContainerRequest{
			Image:        pgImage,
			ExposedPorts: []string{"5432/tcp"},
			Env:          map[string]string{"POSTGRES_PASSWORD": "pw", "POSTGRES_DB": "castor"},
			// the image restarts the server once after initialisation, so wait for the second start-up message
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90 * time.Second),
		}, "5432/tcp")
		if err != nil {
			return "", err
		}
		dsn = "postgres://postgres:pw@" + addr + "/castor?sslmode=disable"
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
		return run(testcontainers.ContainerRequest{
			Image:        redisImage,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
		}, "6379/tcp")
	})
}

// MinioEndpoint returns the http:// endpoint of a ready Minio (credentials MinioAccessKey/MinioSecretKey).
func MinioEndpoint(t testing.TB) string {
	return get(t, "CASTOR_TEST_MINIO", &minio, func() (string, error) {
		addr, err := run(testcontainers.ContainerRequest{
			Image:        minioImage,
			ExposedPorts: []string{"9000/tcp"},
			Env:          map[string]string{"MINIO_ROOT_USER": MinioAccessKey, "MINIO_ROOT_PASSWORD": MinioSecretKey},
			Cmd:          []string{"server", "/data"},
			WaitingFor:   wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
		}, "9000/tcp")
		if err != nil {
			return "", err
		}
		// the listening port answers before the object layer is initialised: ask for the bucket list
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

// Shutdown terminates every container started by this process. Call it from TestMain.
func Shutdown() {
	mu.Lock()
	defer mu.Unlock()
	for _, c := range started {
		_ = c.Terminate(context.Background())
	}
	started = nil
}
