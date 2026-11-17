# Castor (Go)

Go implementation of the Carbyne Stack Castor tuple service. It is a drop-in
replacement for the Java service in this repository: same REST and
WebSocket/STOMP API, same Postgres schema, MinIO object layout, Redis keys and
tuple formats. The Java sources stay in the repository as the reference
implementation; behaviour is verified against them by a recorded-fixture parity
suite (see *Testing*).

## Build and run

```bash
cd go
go build -o castor ./cmd/castor
docker build -t castor-service:dev .      # distroless image, runs as uid 1000
```

## Configuration (environment variables)

| Variable                  | Default     | Meaning                                                                    |
|---------------------------|-------------|----------------------------------------------------------------------------|
| `CASTOR_SERVICE_PORT`     | `10100`     | HTTP port                                                                  |
| `IS_MASTER`               | `true`      | Master (coordinates reservations) or slave                                 |
| `SLAVE_URIS`              |             | Comma separated slave base URIs (master only)                              |
| `DB_HOST` / `DB_PORT`     | `localhost` / `5432` | Postgres                                                          |
| `DB_NAME`                 | `castor`    | Database name (extension: the Java service always uses `castor`)           |
| `DB_USER` / `DB_PASSWORD` |             | Postgres credentials                                                       |
| `REDIS_HOST` / `REDIS_PORT` |           | Redis                                                                      |
| `MINIO_ENDPOINT`          |             | MinIO endpoint (credentials as in the Java service)                        |
| `INITIAL_FRAGMENT_SIZE`   | `1000`      | Fragment size a chunk is split into                                        |
| `RESERVATION_TIMEOUT`     | `5000`      | Reservation wait timeout in ms                                             |
| `RESERVATION_RETRY_DELAY` | `50`        | Retry delay in ms                                                          |
| `NO_SSL_VALIDATION`       | `false`     | Skip TLS validation towards slaves                                         |
| `TRUSTED_CERTIFICATES`    |             | PEM files trusted for TLS towards slaves                                   |
| `LOG_LEVEL`               | `INFO`      |                                                                            |

`SPRING_PROFILES_ACTIVE` and other Spring settings from the Java chart are
accepted and ignored.

## Go client

`go/client` replaces `castor-java-client` (see `client/doc.go` for the
Java-to-Go method table): `IntraVcpClient` (download tuples, telemetry) and
`InterVcpClient` (share reservations with slaves).

```go
c, _ := client.NewIntraVcpClient(client.Options{ServiceAddresses: []string{"http://castor:10100"}})
tt, _ := client.ParseTupleType("MULTIPLICATION_TRIPLE_GFP")
list, err := c.DownloadTupleShares(ctx, uuid.New(), tt, 10)
```

## Testing

```bash
go vet ./... && go test ./...                         # unit tests (also covers the Java serialization codec fixtures)
CASTOR_URL=http://host:10100 go test -tags integration ./client   # client against a live Castor holding tuples
```

Parity against the Java service (recorded fixtures, two Castors, Postgres,
Redis, MinIO) lives in the `parity/castor` harness of the migration workspace;
the existing Java client is checked against the Go service with
`castor-java-client/src/test/.../LiveGoServiceIT.java`.

## Differences from the Java service

None intended on the wire. Spring-specific error pages (404/405/415) and the
Prometheus metric name and content type are reproduced. Log format differs
(structured `slog`).

Verified against the ORIGINAL published image (`ghcr.io/carbynestack/castor-service:0.3.0`, OpenJDK 1.8.0_292):
all 144 parity fixtures recorded from that image pass (`parity/castor`, `scripts/up_img.sh`), and UUID path,
query and JSON values are accepted/rejected/canonicalised exactly like Spring + Java 8 `UUID.fromString` and
Jackson (`internal/model/javauuid.go`, cases measured on the image).

Remaining known differences:

* Telemetry consumption counters are updated like Java (plain GET then SET, no Redis transaction), so
  concurrent updates of the same millisecond key can be lost, as in Java.
* Under heavy concurrent reservations Postgres can abort a reservation with a deadlock; the request fails
  (no retry), as Java's design does (the master shares the reservation inside the same transaction, so a
  failure after sharing can leave the slave ahead). Not changed on purpose.
