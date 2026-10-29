// Package config parses the environment (same variable names as the Java service).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all runtime settings.
type Config struct {
	Port                int
	IsMaster            bool
	SlaveURIs           []string
	NoSSLValidation     bool
	TrustedCertificates []string
	InitialFragmentSize int64
	ReservationTimeout  int64 // ms
	RetryDelay          int64 // ms
	MessageBuffer       int64
	ServerHeartbeat     int64
	ClientHeartbeat     int64

	RedisHost string
	RedisPort int
	// Literal quotes are part of the Java property values (see SPEC H3).
	ReservationStore string
	ConsumptionStore string
	TelemetryTTLSec  int64

	MinioEndpoint  string
	MinioBucket    string
	MinioAccessKey string
	MinioSecretKey string

	DBHost, DBUser, DBPassword string
	DBPort                     int
	DBName                     string

	LogLevel string
}

// Getenv abstracts os.Getenv for tests.
type Getenv func(string) string

// Load reads the configuration from the process environment.
func Load() (*Config, error) { return LoadFrom(os.Getenv) }

// LoadFrom reads the configuration via get.
func LoadFrom(get Getenv) (*Config, error) {
	c := &Config{
		MessageBuffer: 10485760, ServerHeartbeat: 0, ClientHeartbeat: 10000,
		ReservationStore: `"castor-reservation-store"`,
		ConsumptionStore: `"castor-consumption-"`,
		TelemetryTTLSec:  15552000,
		MinioBucket:      "minio-castor-bucket", MinioAccessKey: "dummy-key", MinioSecretKey: "dummy-secret",
		DBName: "castor",
	}
	var err error
	def := func(k, d string) string {
		if v := get(k); v != "" {
			return v
		}
		return d
	}
	num := func(k string, d int64) int64 {
		s := def(k, strconv.FormatInt(d, 10))
		v, e := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if e != nil && err == nil {
			err = fmt.Errorf("invalid %s=%q", k, s)
		}
		return v
	}
	boolean := func(k string, d bool) bool {
		s := strings.TrimSpace(def(k, strconv.FormatBool(d)))
		v, e := strconv.ParseBool(s)
		if e != nil && err == nil {
			err = fmt.Errorf("invalid %s=%q", k, s)
		}
		return v
	}
	c.Port = int(num("CASTOR_SERVICE_PORT", 10100))
	c.IsMaster = boolean("IS_MASTER", true)
	c.SlaveURIs = splitList(get("SLAVE_URIS"))
	c.NoSSLValidation = boolean("NO_SSL_VALIDATION", false)
	c.TrustedCertificates = splitList(get("TRUSTED_CERTIFICATES"))
	c.InitialFragmentSize = num("INITIAL_FRAGMENT_SIZE", 1000)
	c.ReservationTimeout = num("RESERVATION_TIMEOUT", 5000)
	c.RetryDelay = num("RESERVATION_RETRY_DELAY", 50)
	c.RedisHost = get("REDIS_HOST")
	c.RedisPort = int(num("REDIS_PORT", 6379))
	c.MinioEndpoint = get("MINIO_ENDPOINT")
	c.DBHost = def("DB_HOST", "localhost")
	c.DBPort = int(num("DB_PORT", 5432))
	c.DBName = def("DB_NAME", "castor") // extension: Java fixes the name to castor
	c.DBUser = get("DB_USER")
	c.DBPassword = get("DB_PASSWORD")
	c.LogLevel = strings.ToUpper(def("LOG_LEVEL", "INFO"))
	if err != nil {
		return nil, err
	}
	if c.InitialFragmentSize < 1 {
		return nil, fmt.Errorf("INITIAL_FRAGMENT_SIZE must be >= 1")
	}
	return c, nil
}

// Validate checks the settings that the Java profile `k8s` requires.
func (c *Config) Validate() error {
	if c.RedisHost == "" {
		return fmt.Errorf("REDIS_HOST is required")
	}
	if c.MinioEndpoint == "" {
		return fmt.Errorf("MINIO_ENDPOINT is required")
	}
	return nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ReservationKeyPrefix is Spring CacheKeyPrefix.simple() applied to the reservation store name.
func (c *Config) ReservationKeyPrefix() string { return c.ReservationStore + "::" }

// ConsumptionKeyPrefix returns the consumption key prefix for a tuple type's toString().
func (c *Config) ConsumptionKeyPrefix(tupleTypeString string) string {
	return c.ConsumptionStore + tupleTypeString + "::"
}
