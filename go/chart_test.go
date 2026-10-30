package castorgo_test

import (
	"bytes"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/carbynestack/castor-go/internal/config"
	"sigs.k8s.io/yaml"
)

// Renders the Helm chart (castor-service/charts/castor) with `helm template` and checks what the
// Go service depends on: environment variables, ports, probes, security context, kubeVersion.
// Skipped when helm is not on PATH.

const chartDir = "../castor-service/charts/castor"

type obj = map[string]any

func helmTemplate(t *testing.T, args ...string) (docs []obj, err error) {
	t.Helper()
	helm, lerr := exec.LookPath("helm")
	if lerr != nil {
		t.Skip("helm is not on PATH")
	}
	cmd := exec.Command(helm, append([]string{"template", "rel", chartDir, "--namespace", "ns"}, args...)...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if rerr := cmd.Run(); rerr != nil {
		return nil, &helmError{stderr.String()}
	}
	for _, part := range regexp.MustCompile(`(?m)^---\s*$`).Split(out.String(), -1) {
		if strings.TrimSpace(part) == "" {
			continue
		}
		var d obj
		if err := yaml.Unmarshal([]byte(part), &d); err != nil {
			t.Fatalf("rendered YAML does not parse: %v\n%s", err, part)
		}
		if len(d) > 0 {
			docs = append(docs, d)
		}
	}
	return docs, nil
}

type helmError struct{ msg string }

func (e *helmError) Error() string { return e.msg }

func render(t *testing.T, args ...string) (deploy, svc obj) {
	t.Helper()
	docs, err := helmTemplate(t, args...)
	if err != nil {
		t.Fatalf("helm template %v: %v", args, err)
	}
	for _, d := range docs {
		switch d["kind"] {
		case "Deployment":
			deploy = d
		case "Service":
			svc = d
		default:
			t.Fatalf("unexpected kind %v", d["kind"])
		}
	}
	if deploy == nil || svc == nil {
		t.Fatalf("want a Deployment and a Service, got %d documents", len(docs))
	}
	return
}

// path walks maps by key and lists by index ("containers", 0, "env").
func path(t *testing.T, v any, keys ...any) any {
	t.Helper()
	for _, k := range keys {
		switch kk := k.(type) {
		case string:
			m, ok := v.(obj)
			if !ok {
				t.Fatalf("%v is not an object at %q", v, kk)
			}
			v = m[kk]
		case int:
			l, ok := v.([]any)
			if !ok || kk >= len(l) {
				t.Fatalf("%v is not a list with index %d", v, kk)
			}
			v = l[kk]
		}
	}
	return v
}

func container(t *testing.T, deploy obj) obj {
	t.Helper()
	return path(t, deploy, "spec", "template", "spec", "containers", 0).(obj)
}

func envOf(t *testing.T, c obj) map[string]obj {
	t.Helper()
	out := map[string]obj{}
	for _, e := range c["env"].([]any) {
		m := e.(obj)
		out[m["name"].(string)] = m
	}
	return out
}

func envValues(t *testing.T, c obj) map[string]string {
	t.Helper()
	out := map[string]string{}
	for k, e := range envOf(t, c) {
		if v, ok := e["value"]; ok {
			out[k], _ = v.(string)
		} else if v == nil && e["valueFrom"] == nil {
			out[k] = ""
		}
	}
	return out
}

func TestChartDefaults(t *testing.T) {
	deploy, svc := render(t)
	c := container(t, deploy)
	if c["name"] != "castor" || c["image"] != "ghcr.io/carbynestack/castor-service:latest" || c["imagePullPolicy"] != "IfNotPresent" {
		t.Fatalf("container %v %v %v", c["name"], c["image"], c["imagePullPolicy"])
	}
	if path(t, deploy, "spec", "replicas") != float64(1) {
		t.Fatal("replicas")
	}
	if path(t, deploy, "spec", "template", "spec", "imagePullSecrets") != nil {
		t.Fatal("imagePullSecrets rendered without secrets")
	}
	want := map[string]string{
		"SPRING_PROFILES_ACTIVE": "k8s", "IS_MASTER": "true", "SLAVE_URIS": "", "NO_SSL_VALIDATION": "false", "INITIAL_FRAGMENT_SIZE": "1000",
		"REDIS_HOST": "redis.default.svc.cluster.local", "REDIS_PORT": "6379", "MINIO_ENDPOINT": "http://minio.default.svc.cluster.local:9000",
		"DB_HOST": "dbms-repl.default.svc.cluster.local", "DB_NAME": "castor", "DB_PORT": "5432", "LOG_LEVEL": "INFO", "RESERVATION_TIMEOUT": "5000",
	}
	if got := envValues(t, c); !reflect.DeepEqual(got, want) {
		t.Fatalf("env\n got %v\nwant %v", got, want)
	}
	env := envOf(t, c)
	for name, key := range map[string]string{"DB_USER": "username", "DB_PASSWORD": "password"} {
		ref := path(t, env[name], "valueFrom", "secretKeyRef").(obj)
		if ref["key"] != key {
			t.Errorf("%s: secret key %v", name, ref["key"])
		}
	}
	if len(env) != len(want)+2 {
		t.Fatalf("%d environment variables", len(env))
	}

	ports := c["ports"].([]any)
	if len(ports) != 1 || !reflect.DeepEqual(ports[0], obj{"name": "mgmt", "containerPort": float64(10100), "protocol": "TCP"}) {
		t.Fatalf("ports %v", ports)
	}
	wantSC := obj{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "runAsNonRoot": true, "runAsUser": float64(1000),
		"capabilities": obj{"drop": []any{"ALL"}}, "seccompProfile": obj{"type": "RuntimeDefault"}}
	if !reflect.DeepEqual(c["securityContext"], wantSC) {
		t.Fatalf("securityContext %v", c["securityContext"])
	}
	for name, p := range map[string]obj{"livenessProbe": {"period": 10, "delay": 120, "fail": 3}, "readinessProbe": {"period": 5, "delay": 0, "fail": 3}} {
		pr := c[name].(obj)
		if path(t, pr, "httpGet", "path") != "/actuator/health" || path(t, pr, "httpGet", "port") != float64(10100) ||
			pr["periodSeconds"] != float64(p["period"].(int)) || pr["initialDelaySeconds"] != float64(p["delay"].(int)) || pr["failureThreshold"] != float64(p["fail"].(int)) {
			t.Errorf("%s: %v", name, pr)
		}
	}

	// service
	if svc["metadata"].(obj)["namespace"] != "ns" || path(t, svc, "spec", "type") != "ClusterIP" || path(t, svc, "metadata", "annotations") != nil {
		t.Fatalf("service %v", svc)
	}
	sp := path(t, svc, "spec", "ports", 0).(obj)
	if sp["port"] != float64(10100) || sp["targetPort"] != float64(10100) || sp["name"] != "http" || sp["protocol"] != "TCP" {
		t.Fatalf("service port %v", sp)
	}
	// the service selects the pods of the deployment
	if !reflect.DeepEqual(path(t, svc, "spec", "selector"), path(t, deploy, "spec", "selector", "matchLabels")) ||
		!reflect.DeepEqual(path(t, svc, "spec", "selector"), path(t, deploy, "spec", "template", "metadata", "labels")) {
		t.Fatal("service selector does not match the pod labels")
	}
}

func TestChartOverrides(t *testing.T) {
	deploy, svc := render(t,
		"--set", "castor.isMaster=false",
		"--set", "castor.slaveUris={http://a:10100,https://b.example/castor}",
		"--set", "castor.noSslValidation=true",
		"--set", "castor.db.name=castor_db2",
		"--set", "castor.db.port=15432",
		"--set", "castor.db.userSecretName=pg-user",
		"--set", "castor.db.passwordSecretName=pg-pass",
		"--set", "castor.logLevel=DEBUG",
		"--set", "castor.reservationTimeout=250",
		"--set", "castor.initialFragmentSize=77",
		"--set", "castor.image.registry=reg.example",
		"--set", "castor.image.repository=team/castor",
		"--set", "castor.image.tag=1.2.3",
		"--set", "castor.image.pullPolicy=Always",
		"--set", "castor.image.pullSecrets={s1,s2}",
		"--set", "castor.probes.liveness.initialDelay=5",
		"--set", "castor.probes.liveness.period=7",
		"--set", "castor.probes.liveness.failureThreshold=9",
		"--set", "castor.probes.readiness.initialDelay=2",
		"--set", "castor.probes.readiness.period=3",
		"--set", "castor.probes.readiness.failureThreshold=4",
		"--set", "service.type=NodePort",
		"--set", "service.port=30100",
		"--set-string", "service.annotations.example\\.com/a=b",
		"--set", "replicaCount=3",
	)
	c := container(t, deploy)
	if c["image"] != "reg.example/team/castor:1.2.3" || c["imagePullPolicy"] != "Always" {
		t.Fatalf("image %v %v", c["image"], c["imagePullPolicy"])
	}
	if !reflect.DeepEqual(path(t, deploy, "spec", "template", "spec", "imagePullSecrets"), []any{obj{"name": "s1"}, obj{"name": "s2"}}) {
		t.Fatal(path(t, deploy, "spec", "template", "spec", "imagePullSecrets"))
	}
	if path(t, deploy, "spec", "replicas") != float64(3) {
		t.Fatal("replicas")
	}
	env := envValues(t, c)
	for k, v := range map[string]string{"IS_MASTER": "false", "SLAVE_URIS": "http://a:10100,https://b.example/castor", "NO_SSL_VALIDATION": "true",
		"DB_NAME": "castor_db2", "DB_PORT": "15432", "LOG_LEVEL": "DEBUG", "RESERVATION_TIMEOUT": "250", "INITIAL_FRAGMENT_SIZE": "77"} {
		if env[k] != v {
			t.Errorf("%s = %q, want %q", k, env[k], v)
		}
	}
	e := envOf(t, c)
	if path(t, e["DB_USER"], "valueFrom", "secretKeyRef", "name") != "pg-user" || path(t, e["DB_PASSWORD"], "valueFrom", "secretKeyRef", "name") != "pg-pass" {
		t.Fatal("secret names")
	}
	live, ready := c["livenessProbe"].(obj), c["readinessProbe"].(obj)
	if live["initialDelaySeconds"] != float64(5) || live["periodSeconds"] != float64(7) || live["failureThreshold"] != float64(9) ||
		ready["initialDelaySeconds"] != float64(2) || ready["periodSeconds"] != float64(3) || ready["failureThreshold"] != float64(4) {
		t.Fatalf("probes %v %v", live, ready)
	}
	if path(t, svc, "spec", "type") != "NodePort" || path(t, svc, "spec", "ports", 0, "port") != float64(30100) || path(t, svc, "spec", "ports", 0, "targetPort") != float64(10100) {
		t.Fatalf("service %v", svc["spec"])
	}
	if !reflect.DeepEqual(path(t, svc, "metadata", "annotations"), obj{"example.com/a": "b"}) {
		t.Fatalf("annotations %v", path(t, svc, "metadata", "annotations"))
	}
	// whatever the values, the container port and probes keep pointing at the port the service listens on
	if c["ports"].([]any)[0].(obj)["containerPort"] != float64(10100) {
		t.Fatal("container port")
	}
}

func TestChartNameOverrides(t *testing.T) {
	deploy, svc := render(t, "--set", "fullnameOverride=my-castor")
	if deploy["metadata"].(obj)["name"] != "my-castor" || svc["metadata"].(obj)["name"] != "my-castor" {
		t.Fatal("fullnameOverride")
	}
	deploy, _ = render(t, "--set", "nameOverride=other")
	if path(t, deploy, "metadata", "labels", "app.kubernetes.io/name") != "other" {
		t.Fatal(path(t, deploy, "metadata", "labels"))
	}
}

func TestChartKubeVersion(t *testing.T) {
	if _, err := helmTemplate(t, "--kube-version", "1.24.9"); err == nil || !strings.Contains(err.Error(), "chart requires kubeVersion: >=1.25.0-0") {
		t.Fatalf("kubernetes 1.24 accepted: %v", err)
	}
	for _, v := range []string{"1.25.0", "1.30.2", "1.28.3-gke.100"} {
		if _, err := helmTemplate(t, "--kube-version", v); err != nil {
			t.Errorf("kubernetes %s rejected: %v", v, err)
		}
	}
}

// The chart and the Go service agree on the environment: every variable the chart sets (but the
// Spring profile) is read by internal/config, and what the chart renders loads and validates.
func TestChartEnvironmentIsUnderstoodByTheService(t *testing.T) {
	src, err := os.ReadFile("internal/config/config.go")
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?:num|boolean|def|get)\("([A-Z_]+)"`).FindAllStringSubmatch(string(src), -1) {
		known[m[1]] = true
	}
	deploy, _ := render(t, "--set", "castor.slaveUris={http://s1:10100,http://s2:10100}", "--set", "castor.db.name=other")
	c := container(t, deploy)
	for name := range envOf(t, c) {
		if name != "SPRING_PROFILES_ACTIVE" && !known[name] {
			t.Errorf("the chart sets %s, the service does not read it", name)
		}
	}
	vals := envValues(t, c)
	cfg, err := config.LoadFrom(func(k string) string { return vals[k] })
	if err != nil {
		t.Fatalf("service rejects the chart's environment: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if !cfg.IsMaster || cfg.DBName != "other" || cfg.RedisPort != 6379 || cfg.DBPort != 5432 || cfg.InitialFragmentSize != 1000 || cfg.ReservationTimeout != 5000 ||
		!reflect.DeepEqual(cfg.SlaveURIs, []string{"http://s1:10100", "http://s2:10100"}) {
		t.Fatalf("%+v", cfg)
	}
	// the probe and the service port are the service's default listen port
	if cfg.Port != 10100 || path(t, c, "livenessProbe", "httpGet", "port") != float64(cfg.Port) {
		t.Fatal("port mismatch")
	}
}

func TestChartRejectsBrokenValues(t *testing.T) {
	// annotations must be a map
	if _, err := helmTemplate(t, "--set", "service.annotations=notamap"); err == nil {
		t.Fatal("scalar annotations accepted")
	}
}
