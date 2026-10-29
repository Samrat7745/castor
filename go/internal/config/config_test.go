package config

import "testing"

func env(m map[string]string) Getenv { return func(k string) string { return m[k] } }

func TestDefaults(t *testing.T) {
	c, err := LoadFrom(env(map[string]string{"REDIS_HOST": "r", "MINIO_ENDPOINT": "http://m:9000"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != 10100 || !c.IsMaster || c.InitialFragmentSize != 1000 || c.ReservationTimeout != 5000 || c.RetryDelay != 50 || c.DBHost != "localhost" || c.DBPort != 5432 {
		t.Fatalf("%+v", c)
	}
	if c.ReservationKeyPrefix() != `"castor-reservation-store"::` || c.ConsumptionKeyPrefix("bit_gfp") != `"castor-consumption-"bit_gfp::` {
		t.Fatal("prefixes")
	}
	if c.Validate() != nil {
		t.Fatal("validate")
	}
}

func TestOverrides(t *testing.T) {
	c, err := LoadFrom(env(map[string]string{"IS_MASTER": "false", "SLAVE_URIS": " http://a , http://b,", "CASTOR_SERVICE_PORT": "8080", "NO_SSL_VALIDATION": "true", "TRUSTED_CERTIFICATES": "/x.pem,/y.pem"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.IsMaster || len(c.SlaveURIs) != 2 || c.SlaveURIs[1] != "http://b" || c.Port != 8080 || !c.NoSSLValidation || len(c.TrustedCertificates) != 2 {
		t.Fatalf("%+v", c)
	}
	if c.Validate() == nil {
		t.Fatal("expected validation failure")
	}
	if _, err := LoadFrom(env(map[string]string{"IS_MASTER": "maybe"})); err == nil {
		t.Fatal("bad bool")
	}
}
