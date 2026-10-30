package config

import (
	"reflect"
	"testing"
)

// Matrix of every environment variable: missing, empty, valid, edge and invalid values with the
// exact expected result or error text.

func TestNumericVariablesMatrix(t *testing.T) {
	type get func(*Config) int64
	vars := []struct {
		name string
		def  int64
		get  get
	}{
		{"CASTOR_SERVICE_PORT", 10100, func(c *Config) int64 { return int64(c.Port) }},
		{"INITIAL_FRAGMENT_SIZE", 1000, func(c *Config) int64 { return c.InitialFragmentSize }},
		{"RESERVATION_TIMEOUT", 5000, func(c *Config) int64 { return c.ReservationTimeout }},
		{"RESERVATION_RETRY_DELAY", 50, func(c *Config) int64 { return c.RetryDelay }},
		{"REDIS_PORT", 6379, func(c *Config) int64 { return int64(c.RedisPort) }},
		{"DB_PORT", 5432, func(c *Config) int64 { return int64(c.DBPort) }},
	}
	for _, v := range vars {
		for _, tc := range []struct {
			in      string
			want    int64
			wantErr string
		}{
			{"", v.def, ""}, // empty counts as unset
			{"1", 1, ""},
			{"  42  ", 42, ""},
			{"+7", 7, ""},
			{"007", 7, ""},
			{"abc", 0, `invalid ` + v.name + `="abc"`},
			{"1.5", 0, `invalid ` + v.name + `="1.5"`},
			{"1e3", 0, `invalid ` + v.name + `="1e3"`},
			{"0x10", 0, `invalid ` + v.name + `="0x10"`},
			{"1 000", 0, `invalid ` + v.name + `="1 000"`},
			{"99999999999999999999", 0, `invalid ` + v.name + `="99999999999999999999"`},
			{" ", 0, `invalid ` + v.name + `=" "`},
		} {
			if v.name == "INITIAL_FRAGMENT_SIZE" && tc.in == "1e3" {
				continue
			}
			c, err := LoadFrom(env(map[string]string{v.name: tc.in}))
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr || c != nil {
					t.Errorf("%s=%q: got (%v, %v), want error %q", v.name, tc.in, c, err, tc.wantErr)
				}
				continue
			}
			if err != nil || v.get(c) != tc.want {
				t.Errorf("%s=%q: got %v (%v), want %d", v.name, tc.in, c, err, tc.want)
			}
		}
	}
}

// Values outside the sensible range that are accepted as they are (documented behaviour: the
// listener / timeouts fail later); the ones that are validated are covered below.
func TestNumericEdgeValuesAreTakenVerbatim(t *testing.T) {
	c, err := LoadFrom(env(map[string]string{"CASTOR_SERVICE_PORT": "0", "RESERVATION_TIMEOUT": "-1", "RESERVATION_RETRY_DELAY": "0", "REDIS_PORT": "70000", "DB_PORT": "-5"}))
	if err != nil || c.Port != 0 || c.ReservationTimeout != -1 || c.RetryDelay != 0 || c.RedisPort != 70000 || c.DBPort != -5 {
		t.Fatalf("%+v %v", c, err)
	}
	c, err = LoadFrom(env(map[string]string{"INITIAL_FRAGMENT_SIZE": "1"}))
	if err != nil || c.InitialFragmentSize != 1 {
		t.Fatal(c, err)
	}
	c, err = LoadFrom(env(map[string]string{"INITIAL_FRAGMENT_SIZE": "9223372036854775807"}))
	if err != nil || c.InitialFragmentSize != 9223372036854775807 {
		t.Fatal(c, err)
	}
}

func TestInitialFragmentSizeMustBePositive(t *testing.T) {
	for _, in := range []string{"0", "-1", "-9223372036854775808"} {
		c, err := LoadFrom(env(map[string]string{"INITIAL_FRAGMENT_SIZE": in}))
		if c != nil || err == nil || err.Error() != "INITIAL_FRAGMENT_SIZE must be >= 1" {
			t.Errorf("%s: %v %v", in, c, err)
		}
	}
}

func TestBooleanVariablesMatrix(t *testing.T) {
	for _, name := range []string{"IS_MASTER", "NO_SSL_VALIDATION"} {
		def := name == "IS_MASTER"
		for _, tc := range []struct {
			in      string
			want    bool
			wantErr bool
		}{
			{"", def, false},
			{"true", true, false}, {"TRUE", true, false}, {"True", true, false}, {"t", true, false}, {"T", true, false}, {"1", true, false},
			{"false", false, false}, {"FALSE", false, false}, {"f", false, false}, {"0", false, false}, {" true ", true, false},
			{"yes", false, true}, {"no", false, true}, {"on", false, true}, {"tRuE", false, true}, {"2", false, true}, {" ", false, true},
		} {
			c, err := LoadFrom(env(map[string]string{name: tc.in}))
			if tc.wantErr {
				want := `invalid ` + name + `="` + trimSpaces(tc.in) + `"`
				if err == nil || err.Error() != want {
					t.Errorf("%s=%q: got %v, want %q", name, tc.in, err, want)
				}
				continue
			}
			got := c.IsMaster
			if name == "NO_SSL_VALIDATION" {
				got = c.NoSSLValidation
			}
			if err != nil || got != tc.want {
				t.Errorf("%s=%q: got %v (%v), want %v", name, tc.in, got, err, tc.want)
			}
		}
	}
}

func trimSpaces(s string) string {
	for len(s) > 0 && s[0] == ' ' {
		s = s[1:]
	}
	for len(s) > 0 && s[len(s)-1] == ' ' {
		s = s[:len(s)-1]
	}
	return s
}

func TestListVariablesMatrix(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"", nil},
		{" ", nil},
		{",", nil},
		{",,,", nil},
		{"http://a", []string{"http://a"}},
		{"http://a,http://b", []string{"http://a", "http://b"}},
		{" http://a ,\thttp://b\n,", []string{"http://a", "http://b"}},
		{"http://a,,http://b", []string{"http://a", "http://b"}},
		{"http://a,http://a", []string{"http://a", "http://a"}}, // duplicates are kept
		{"a b", []string{"a b"}},
	} {
		c, err := LoadFrom(env(map[string]string{"SLAVE_URIS": tc.in, "TRUSTED_CERTIFICATES": tc.in}))
		if err != nil || !reflect.DeepEqual(c.SlaveURIs, tc.want) || !reflect.DeepEqual(c.TrustedCertificates, tc.want) {
			t.Errorf("%q: %#v %#v (%v), want %#v", tc.in, c.SlaveURIs, c.TrustedCertificates, err, tc.want)
		}
	}
}

func TestStringVariablesMatrix(t *testing.T) {
	c, err := LoadFrom(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{Port: 10100, IsMaster: true, InitialFragmentSize: 1000, ReservationTimeout: 5000, RetryDelay: 50,
		MessageBuffer: 10485760, ServerHeartbeat: 0, ClientHeartbeat: 10000,
		ReservationStore: `"castor-reservation-store"`, ConsumptionStore: `"castor-consumption-"`, TelemetryTTLSec: 15552000,
		MinioBucket: "minio-castor-bucket", MinioAccessKey: "dummy-key", MinioSecretKey: "dummy-secret",
		RedisPort: 6379, DBHost: "localhost", DBPort: 5432, DBName: "castor", LogLevel: "INFO"}
	if !reflect.DeepEqual(*c, want) {
		t.Fatalf("defaults:\n got %+v\nwant %+v", *c, want)
	}
	c, err = LoadFrom(env(map[string]string{"REDIS_HOST": "r", "MINIO_ENDPOINT": "http://m", "DB_HOST": "db", "DB_USER": "u", "DB_PASSWORD": " p w ", "DB_NAME": "other", "LOG_LEVEL": "debug"}))
	if err != nil || c.RedisHost != "r" || c.MinioEndpoint != "http://m" || c.DBHost != "db" || c.DBUser != "u" || c.DBPassword != " p w " || c.DBName != "other" || c.LogLevel != "DEBUG" {
		t.Fatalf("%+v %v", c, err)
	}
	// empty strings fall back to the defaults where there is one
	c, _ = LoadFrom(env(map[string]string{"DB_HOST": "", "DB_NAME": "", "LOG_LEVEL": ""}))
	if c.DBHost != "localhost" || c.DBName != "castor" || c.LogLevel != "INFO" {
		t.Fatalf("%+v", c)
	}
	// the log level is only upper-cased, not validated
	c, _ = LoadFrom(env(map[string]string{"LOG_LEVEL": "loud"}))
	if c.LogLevel != "LOUD" {
		t.Fatal(c.LogLevel)
	}
}

// The first invalid variable (in the order the settings are read) is reported.
func TestFirstErrorWins(t *testing.T) {
	_, err := LoadFrom(env(map[string]string{"DB_PORT": "x", "IS_MASTER": "maybe", "CASTOR_SERVICE_PORT": "y"}))
	if err == nil || err.Error() != `invalid CASTOR_SERVICE_PORT="y"` {
		t.Fatal(err)
	}
	_, err = LoadFrom(env(map[string]string{"DB_PORT": "x", "NO_SSL_VALIDATION": "maybe"}))
	if err == nil || err.Error() != `invalid NO_SSL_VALIDATION="maybe"` {
		t.Fatal(err)
	}
	// a parse error hides the fragment size rule
	_, err = LoadFrom(env(map[string]string{"INITIAL_FRAGMENT_SIZE": "0", "REDIS_PORT": "z"}))
	if err == nil || err.Error() != `invalid REDIS_PORT="z"` {
		t.Fatal(err)
	}
}

func TestValidateMatrix(t *testing.T) {
	for _, tc := range []struct {
		redis, minio, want string
	}{
		{"", "", "REDIS_HOST is required"},
		{"", "http://m", "REDIS_HOST is required"},
		{"r", "", "MINIO_ENDPOINT is required"},
		{"r", "http://m", ""},
		{" ", "http://m", ""}, // not trimmed: only emptiness is checked
	} {
		c, err := LoadFrom(env(map[string]string{"REDIS_HOST": tc.redis, "MINIO_ENDPOINT": tc.minio}))
		if err != nil {
			t.Fatal(err)
		}
		err = c.Validate()
		if (tc.want == "") != (err == nil) || (err != nil && err.Error() != tc.want) {
			t.Errorf("redis=%q minio=%q: %v, want %q", tc.redis, tc.minio, err, tc.want)
		}
	}
}

func TestLoadReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv("CASTOR_SERVICE_PORT", "4711")
	t.Setenv("REDIS_HOST", "rh")
	c, err := Load()
	if err != nil || c.Port != 4711 || c.RedisHost != "rh" {
		t.Fatal(c, err)
	}
	t.Setenv("CASTOR_SERVICE_PORT", "nope")
	if _, err := Load(); err == nil || err.Error() != `invalid CASTOR_SERVICE_PORT="nope"` {
		t.Fatal(err)
	}
}
