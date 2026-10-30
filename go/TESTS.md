# Test coverage of the original Java test suite

Every test class of the Java modules `castor-common`, `castor-service` and `castor-java-client` is
mapped to the Go test that covers the same behaviour. 168 Java `@Test` methods are in scope
(`LiveGoServiceIT` is excluded; it is the Java-side check of this Go service): 159 are ported or
re-expressed, 1 was already covered by an older Go test, 8 are not applicable (reason given).
The `castor-upload-java-client` module is covered by the Go `client.UploadClient` (client/upload.go); see the last section.

The Java `*IT` classes used Testcontainers; the Go tests use testcontainers-go (the Go port of it) through
`internal/testenv` (postgres:11.1, redis:7.0, quay.io/minio/minio:RELEASE.2022-04-16T04-26-02Z, terminated in
`TestMain`). Existing instances can be supplied instead with
`CASTOR_TEST_PG` (DSN), `CASTOR_TEST_REDIS` (host:port) and `CASTOR_TEST_MINIO` (URL); tests skip when
neither variable nor Docker is available.

```
go test -p 2 -count=1 ./...        # add -race for the race detector
```

Mockito based Java unit tests are expressed against the real backing service (database, Redis,
Minio) or miniredis wherever the mocked collaborator has a Go counterpart; spies (`spyStore`,
`fakePeers`) stand in for the object store and the slave clients.

## castor-common

| Java test (method) | Go test |
|---|---|
| **CastorServiceUriTest** | `client/clients_test.go` |
| null address, no scheme, invalid URI (3) | `TestServiceAddressValidation` |
| https / ws URI with domain (2) | `TestRestBase` (REST URI; the WebSocket URI is built in `NewUploadClient` and exercised by `TestUploadRoundTripAndBearerToken`) |
| trailing slash, telemetry URI | `TestRestBase`, `TestEndpointURIs` |
| request tuples URI, telemetry URI with interval, path segments (3) | `TestEndpointURIs` (parameter names, base path, reservation id segment) |
| `givenCastorServiceUri_whenGetActivateTupleChunkUri` | n/a: the Go client has no chunk activation; the service route is covered by `httpapi.TestActivateTelemetry` |
| **ArrayBackedTupleTest** | |
| valid stream, stream of invalid length | `client.TestTupleListFromTupleData`, `model.TestTupleDataOfInvalidLengthIsRejected` |
| valid / invalid number of shares | `client.TestTupleListRoundTripAllTypes`, `client.TestTupleWithWrongNumberOfSharesIsRejected` |
| share index out of bounds | n/a: Go tuples are plain slices, no accessor that throws |
| **FieldTest** (4: Gfp, Gf2n, missing name, missing elementSize) | `client.TestFieldJSON` |
| **ReservationElementTest** (7: null id, zero tuples, negative start, 3 missing properties, valid) | `model.TestReservationElementJSON` |
| **ReservationTest** (8: null id, empty id, no elements, valid, 4 missing properties) | `model.TestReservationJSONRules` |
| **ShareTest** (3: invalid mac, invalid value, valid) | `client.TestShareJSON` |
| **TupleChunkTest** | |
| valid attributes | already covered: `javaser.TestTupleChunk`, `javaser.TestBitDataPreserved` |
| data of invalid length | n/a: `TupleChunk.of` has no Go counterpart; like Java's deserialization path the service does not validate the length (`service.TestUploadCountsOnlyWholeTuples` pins that) |
| **TupleListTest** | `client/tuples_test.go` |
| from stream, missing mac, JSON round trip (all types), empty round trip (4) | `TestTupleListFromTupleData`, `TestTupleListMissingMacIsRejected`, `TestTupleListRoundTripAllTypes` (covers both round-trip tests) |
| export as chunk, IOException on export (2) | n/a: no `TupleList.asChunk` in Go (the client only downloads) |
| **UploadTupleChunkResponseTest** (3: success, failure, truncated payload) | `javaser.TestUploadResponseRoundTrip`, `javaser.TestUploadResponseTruncatedPayloadIsRejected` |

## castor-java-client

| Java test (method) | Go test |
|---|---|
| **DefaultCastorIntraVcpClientTest** | `client/clients_test.go` |
| null service address | `TestServiceAddressValidation` |
| SSL configuration (validation off, trusted certificate, bearer token) | `TestSSLConfigurationAndBearerToken` (plus `TestClientTimeout`) |
| download shares OK / error | `TestIntraClientDownloadAndErrors` (also `TestDownloadTupleSharesAgainstServiceJSON`) |
| telemetry OK / error, with and without interval (4) | `TestIntraClientTelemetryContent`, `TestIntraClientDownloadAndErrors`, `TestEndpointURIs` |
| **DefaultCastorInterVcpClientTest** | |
| `serviceAddresses` is null | n/a: Lombok `@NonNull`; an empty list is rejected instead (`TestServiceAddressValidation`) |
| single address null / empty | `TestServiceAddressValidation` |
| SSL configuration | `TestSSLConfigurationAndBearerToken` |
| share: one endpoint fails, communication fails, success (3) | `TestInterShareReservation` (and `peer.TestShareReservationAsksEverySlave`, `peer.TestTransportErrorsCarryTheirCause`) |
| update: communication fails, success (2) | `TestInterUpdateReservationStatus` (and `peer.TestEveryDeclaredSlaveReceivesTheStatusUpdate`) |
| **LiveGoServiceIT** | skipped (Java-side live check of the Go service; Go counterpart `client/integration_test.go`) |

## castor-service

| Java test (method) | Go test |
|---|---|
| **DefaultTuplesDownloadServiceTest** (2) | `service.TestDownloadAsSlaveFailsWhenTuplesCannotBeRetrieved`, `TestDownloadAsMasterReturnsTuplesOfExistingReservationAndCleansUp` (+ `TestDownloadSucceedsEvenIfDeletingTheChunkFails`) |
| **DedicatedTransactionServiceTest** (1) | `pgstore.TestInTx` (commit, rollback, own connection: the observable `REQUIRES_NEW` behaviour) |
| **TelemetryServiceTest** (1) | `service.TestTelemetryDataForInterval` (+ `TestTelemetryConsumptionRateRounding`) |
| **DefaultTuplesDownloadServiceAsMasterIT** (3: sharing fails, retrieval fails, success and cleanup) | `service.TestITDownloadAsMasterRollsBackWhenSharingFails`, `TestITDownloadAsMasterKeepsReservationWhenRetrievalFails`, `TestITDownloadAsMasterReturnsTuplesAndCleansUp` |
| **DefaultCastorWebSocketServiceTest** | `service/ports_service_test.go` |
| invalid payload, no tuples, write fails, success (4) | `TestUploadInvalidPayload`, `TestUploadChunkWithoutTuples`, `TestUploadFailsWhenWritingToTheTupleStoreFails`, `TestUploadSuccess` |
| generate fragments: zero tuples, multiple tuples (2) | `TestGenerateFragmentsForChunk` |
| **ConcurrencyIT** (1) | `service.TestITSubsequentReservationProcessedFirstSucceeds` (sequential as in Java, plus parallel) |
| **ConsumptionCachingServiceTest** (3: no value, existing value, filter and aggregate) | `rediscache.TestKeepConsumptionKeyValueAndTTL`, `TestConsumptionSinceFilters` |
| **ConsumptionCachingServiceIT** (4: empty, time frames, TTL, sum) | `rediscache.TestITGivenCacheIsEmptyReturnZero`, `TestITMultipleConsumptionValuesForTimeFrame`, `TestITTimeToLiveRemovesValueAutomatically`, `TestITMultipleValuesForSameTimestampAreSummed` |
| **ReservationCachingServiceTest** | `service/ports_service_test.go` |
| keep reservation, keep conflict (2) | `TestKeepReservationStoresAndEmitsConsumption`, `TestKeepReservationConflict` |
| update missing / success (2) | `TestUpdateReservation` |
| get unlocked: expected, matching config, mismatch type, mismatch count (4) | `TestGetUnlockedReservation` |
| forget reservation | `TestForgetReservation` |
| keep-and-apply: conflict, no fragment, split (3) | `TestApplyReservationConflictLeavesEverythingUntouched`, `TestApplyReservationWithoutMatchingFragment`, `TestApplyReservationSplitsFragmentAroundTheReservedRange` |
| get with retry: timeout, success (2) | `TestSlaveTimesOutWithoutReleasedReservationAndStopsPolling`, `TestSlaveWaitsUntilReservationIsUnlocked` (+ `TestSlaveReportsGenericMessageWhenPolledReservationDoesNotMatch`) |
| create: dedicated tx service missing, client missing (2) | `TestCreateReservationRequiresMaster` (one condition in Go: `Peers == nil`) |
| create: creation throws, activation throws, success (3) | `TestCreateReservationPeerCommunicationFails`, `TestCreateReservationSuccess` |
| **ReservationCachingServiceIT** (5: empty, untouched, rollback, forget, apply across fragments) | `service.TestITGetUnlockedReservationFromEmptyCacheReturnsNil`, `TestITReservationInCacheStaysUntouchedByGet`, `TestITSharingFailureRollsBackFragmentation`, `TestITForgetReservationRemovesItFromCache`, `TestITApplyReservationAcrossSeveralFragments` |
| **CreateReservationSupplierTest** (4: insufficient, no fragment, sharing fails, success) | `service.TestCreateReservationInsufficientTuples`, `TestComposeElementsWithoutLocatableFragment`, `TestCreateReservationSharingFails`, `TestCreateReservationSuccess` |
| **UnlockReservationSupplierTest** (1) | `service.TestCreateReservationSuccess` (unlock is part of `createReservation`: peers are told, cache updated, returned status UNLOCKED) |
| **WaitForReservationCallableTest** (2: cancelled returns null, locked then unlocked) | `service.TestSlaveTimesOutWithoutReleasedReservationAndStopsPolling`, `TestSlaveWaitsUntilReservationIsUnlocked` |
| **TupleChunkFragmentEntityTest** | `pgstore/fragments_test.go` |
| negative start index, invalid length, valid configuration, invalid `setEndIndex` (4) | `TestNewFragmentValidation`, `TestSplitAtRejectsIndexBeforeStart` |
| tuple chunk id null, tuple type null (2) | n/a: cannot be null in Go (validated string / enum name) |
| **TupleChunkFragmentStorageServiceTest** (19) | `pgstore/fragments_test.go`: `TestKeepPersists`, `TestKeepConflictingListPersistsNothing`, `TestFindAvailableFragmentForChunkContainingIndex`, `TestFindAvailableFragmentByType`, `TestSplitAtAndBefore`, `TestSplitBefore`, `TestHasConflict`, `TestAvailableTuples`, `TestUnlockChunk` (+ service `TestActivateMissing`), `TestDeleteByReservationAndChunkReferenced`, `TestUpdatePersists`; run against the real database instead of a mocked repository |
| **TupleChunkFragmentStorageServiceIT** (18: find for chunk ×6, find for type ×5, conflict ×2, count, activate, delete, referenced ×2, ordering) | same tests as above (`TestFindAvailable*`, `TestHasConflict`, `TestAvailableTuples`, `TestUnlockChunk`, `TestDeleteByReservationAndChunkReferenced`) |
| **MinioTupleStoreTest** (8: bucket, save, save fails, negative index, zero length, read fails, read ok, delete missing) | `objstore/minio_test.go`: `TestNewMakesBucketOnlyIfMissing`, `TestSavePutsChunkDataUnderChunkId`, `TestSaveFailureIsReportedWithJavaMessageAndCause`, `TestReadRejectsInvalidArguments`, `TestReadOfMissingObjectFailsWithJavaMessageAndCause`, `TestReadReturnsRequestedRange`, `TestDeleteOfMissingChunkIsHarmless` (+ `service.TestDownloadSucceedsEvenIfDeletingTheChunkFails`) |
| testconfig (`Reusable*Container`, `PersistenceTestEnvironment`), `TupleChunkFragmentEntityListMatcher`, test resources | n/a: test infrastructure, replaced by `internal/testenv` and the helpers in `service/fixture_test.go` |

## Production changes found by the port

Behaviour that differed from Java (all fixed, minimal):

* `model.Reservation` JSON: a missing property was reported as "missing required property"; now
  `Missing required creator property '<name>'` like Jackson (ReservationTest / ReservationElementTest).
  Chunk ids are lower-cased like `UUID.toString()`.
* `client`: the tuple list decoder accepted incomplete JSON silently (missing `mac`/`value`/`name`,
  unequal share lengths, wrong number of shares, unknown `@type`). New `client/tuples.go` enforces the
  Jackson creator rules of `Field`, `Share`, `Tuple` and `TupleList` (FieldTest, ShareTest, TupleListTest,
  ArrayBackedTupleTest).
* `httpapi`: the `reservationId` query parameter is bound to `java.util.UUID` in Java, so the
  reservation id (`<uuid>_<type>`) is always lower case; Go used the raw string, so an upper-case id
  on master and lower-case id on a slave would not meet.
* `objstore`: errors now carry the Java messages `Persisting TupleChunk failed.` and `Error while
  reading tuples from database.` (with the cause) like `MinioTupleStore`.
* `service`: errors that Java wraps in a `CastorServiceException` with a cause
  (`Failed retrieving tuples from database.`, `Failed to reserve the tuples.`, `Creating the
  reservation failed.`, `Failed writing tuple chunk to database.`) now expose the cause through
  `Unwrap`.
* `rediscache`: `KeepConsumption` could give up with `TxFailedErr` after 20 optimistic retries under
  in-process contention on one millisecond key (found by `TestITMultipleValuesForSameTimestampAreSummed`);
  the read-modify-write is now serialized per process (WATCH still protects against other replicas).

## Known residual differences (not covered by a Java test, not changed)

* Spring binds `UUID` parameters with the lenient `UUID.fromString`, which also accepts short
  components such as `1-1-1-1-1`; the Go service only accepts canonical 36 character UUIDs
  (Jackson, used for JSON bodies, is strict like Go).
* `objstore.Read` fails if the object is shorter than the requested range; Java's
  `TupleList.fromStream` silently returns fewer tuples when whole tuples are missing. Fragments never
  reference bytes outside their chunk, so this only matters for a corrupted object.
* Concurrent `KeepConsumption` calls from different replicas on the same millisecond key still rely on
  the 20 bounded WATCH retries.

## castor-java-client upload module (castor-upload-java-client) -> client/upload.go, client/upload_test.go

The tests run the Go client against the real Go STOMP server (`internal/stomp`) over a real WebSocket, and against a live
Java and Go Castor (`TestLiveUpload`, integration tag).

| Java test class / method group | Go test |
|---|---|
| **DefaultCastorUploadClientTest** (13) | |
| builder null address, configuration | `TestUploadClientRejectsBadAddress` |
| too many tuples -> exception | `TestUploadRequiresConnectionAndLimitsTuples` |
| upload times out -> false | `TestUploadTimeoutIsFalse` |
| successful upload -> true | `TestUploadRoundTripAndBearerToken`, `TestConcurrentUploadsGetTheirOwnResponses` |
| activation failure -> CastorClientException | `TestActivateTupleChunk` |
| disconnect, connect timeout / failure, already connected | `TestDisconnectEndsTheSession`, `TestConnectFailureMessage`, `TestConnectTwiceKeepsTheSession` |
| http/websocket client construction fails, thread interrupted | n/a: Java construction plumbing / `Thread.interrupt`; the Go client takes a `context.Context` (`UploadTupleChunk` returns `ctx.Err()`) |
| **WebSocketClientTest** (8) | |
| bearer token on connect | `TestUploadRoundTripAndBearerToken` |
| connection already established | `TestConnectTwiceKeepsTheSession` |
| disconnect | `TestDisconnectEndsTheSession` |
| send while not connected -> ConnectionFailedException | `TestUploadRequiresConnectionAndLimitsTuples` |
| send expected message, valid response | `TestUploadRoundTripAndBearerToken` (the server decodes the Java-serialized chunk) |
| response without chunk id discarded | `TestResponseWithoutChunkIDIsDiscarded` |
| unsupported payload -> UnsupportedPayloadException | n/a: undecodable messages are ignored by the Go read loop (no exception type) |
| **ResponseCollectorTest** (7) | `TestUploadTimeoutIsFalse`, `TestUploadFailureResponseIsFalse`, `TestUploadRoundTripAndBearerToken`, `TestConcurrentUploadsGetTheirOwnResponses` (registration/lookup is internal to the client) |
| **WebSocketSessionHandlerTest** (2), **ReconnectSchedulerTest** (3) | n/a: Spring session handler / reconnect timer; the Go client reconnects explicitly (`TestDisconnectEndsTheSession` covers reconnect) |

## Additional unit, property, fuzz and contract tests

Beyond the Java ports above. All container based tests use `internal/testenv` and skip without Docker; run
everything with `go test -race -p 2 ./...`. Coverage (statements, `go test -coverprofile`, before -> after): pgstore
76 -> 98, peer 80 -> 96, stomp 80 -> 96, httpapi 92 -> 94, config 93 -> 100, model 94 -> 96, javaser 85 -> 87,
client 86 -> 88, cmd/castor 0 -> 82, total 87.2 % (CI floor 85 %, see CI below).

| File | Covers |
|---|---|
| `internal/service/stress_test.go` | Reservation invariants under load, real Postgres and Redis: a master and a slave service (own database and Redis DB, linked in process) serve ~75 concurrent random-size requests over 5 chunks with more demand than supply. Asserted while running (snapshot every 15 ms) and at the end: no tuple handed out twice, nothing beyond the stock granted, fragments never overlap / are never empty / stay inside the chunk, master and slave hand out identical bytes and end with identical free ranges, free + granted = total (failed requests release exactly what they marked), consumption counters equal the granted tuples on both sides, no reservation left in Redis, fully consumed chunks removed from the object store. A second run makes every 4th sharing fail (rollback). `TestStressTimeoutAndRollback...`: slave timeout leaves the stock untouched, failed sharing rolls back fragments, Redis and consumption. |
| `internal/service/telemetry_clock_test.go` | Telemetry with the injectable `Service.Now`: lower window edge inclusive / one millisecond earlier excluded for the 30 s and 8 s windows, sliding with the clock, empty windows, per-type isolation, rounding per window, counter expiry (miniredis `FastForward`, renewed TTL), consumption stamped by the service clock. |
| `internal/stomp/robust_test.go` | Oversized frame / message / header, frame split at every byte position and coalesced frames, missed and kept client heart-beat, disconnect mid-upload (20 variants), malformed frame mid-session (bystander unaffected), SUBSCRIBE/SEND before CONNECT, 60 concurrent sessions, slow consumer (write timeout, others keep receiving), duplicate SUBSCRIBE / UNSUBSCRIBE, SEND to unknown destinations, bearer token not enforced, non-WebSocket requests, failing and panicking upload handlers. Every test ends with a full protocol round trip on a fresh connection. |
| `internal/stomp/fuzz_test.go` | `FuzzParseFrames` (no panic, split invariance). |
| `internal/javaser/fuzz_test.go` | `FuzzDecode` (all `Decode*` readers), `FuzzRoundTrip` (encoder -> decoder). |
| `internal/model/fuzz_test.go` | `FuzzReservationJSON`, `FuzzParseTupleType`, `FuzzIsUUID`. |
| `client/fuzz_test.go` | `FuzzTupleListJSON`, `FuzzParseStomp` (found a panic on `content-length:-1`, fixed). |
| `internal/httpapi/fuzz_test.go` | `FuzzRequestParameters`: query / path parameters and bodies on every route answer < 500. |
| `*/testdata/fuzz/Fuzz*` | Seed corpora, including the minimised crasher `client/testdata/fuzz/FuzzParseStomp/negative-content-length`. Run one with `go test -run='^$' -fuzz='^FuzzParseFrames$' -fuzztime=20s ./internal/stomp`. |
| `internal/config/matrix_test.go` | Every environment variable: unset, empty, valid, edge, invalid with the exact error text; first error wins; `Validate` matrix. |
| `cmd/castor/main_test.go` | `healthcheck` exit codes (200 / other status / down / bad port, also as a real process), `run()` start-up failures (invalid env, missing REDIS_HOST / MINIO_ENDPOINT, unreachable Postgres / Minio), full start-up against the containers and clean SIGTERM shutdown. |
| `internal/pgstore/extra_test.go` | Closed pool, cancelled context, unreachable server, commit failure, 3 s lock timeout, DSN escaping (and a login with an awkward password), random split sequences keep fragments contiguous, random chunk / fragment sizes, activation of active chunks, deletion of consumed chunks, unicode / odd reservation ids, boundary and scan errors. |
| `internal/peer/extra_test.go` | Slave 2xx/4xx/5xx, huge / broken / silent / slow slaves, partial failure with several slaves (order, who is asked), exact requests on the wire, redirects (307/308 keep the method, 301-303 become GET, loops), TLS (untrusted, `NoSSLValidation`, custom CA, wrong CA, wss), `New` validation, concurrent use. |
| `internal/httpapi/extra_test.go` | 405 on every route and method, 404 bodies, 415 media types, ignored Accept, malformed / huge bodies and URLs, no Authorization needed, panicking backend answers 500, health with each dependency down or hanging, Prometheus output, info, error mapping, query / path parameter variants. |
| `internal/httpapi/replay_test.go` | Replay of the 33 stateless recorded Java fixtures (validation, routing and actuator responses, `testdata/parity`). The stateful fixtures (downloads, telemetry, reservations, STOMP) are not replayed in process: they depend on a two-VCP rig sequence with state dumps via `docker exec` and time windows, which the parity suite (`parity/castor`) already replays against the Go rig. |
| `client/contract_test.go` | The public clients (intra, inter, upload) against the real in-process `httpapi` + `stomp` + `service` (Postgres container, miniredis): upload + activate over STOMP, parallel download on master and slave, telemetry, share / update reservation, bearer token on every request, error cases. |
| `chart_test.go` | `helm template` of `castor-service/charts/castor` (skipped without helm): defaults and overrides (isMaster / slaveUris, db.name, service type / annotations, probes, image, pull secrets), ports, securityContext, kubeVersion, and that the rendered environment is understood by `internal/config`. |

CI (`.github/workflows/go.build-and-test.yaml`): `go test -race -p 2 -coverprofile` over all packages but `internal/e2e`;
`scripts/check-coverage.sh coverage.out` fails below the floor (default 85 %, `COVERAGE_FLOOR` overrides); each fuzz
target runs 15 s on pull requests. Run the script locally: `bash scripts/check-coverage.sh coverage.out 85`.
