# Test coverage of the original Java test suite

Every test class of the Java modules `castor-common`, `castor-service` and `castor-java-client` is
mapped to the Go test that covers the same behaviour. 168 Java `@Test` methods are in scope
(`LiveGoServiceIT` is excluded; it is the Java-side check of this Go service): 159 are ported or
re-expressed, 1 was already covered by an older Go test, 8 are not applicable (reason given).
The `castor-upload-java-client` module is covered by the Go `client.UploadClient` (client/upload.go); see the last section.

The Java `*IT` classes used Testcontainers; the Go tests use plain `docker run` containers through
`internal/testenv` (postgres:11.1, redis:7.0, quay.io/minio/minio:RELEASE.2022-04-16T04-26-02Z, names
`castortest-*`, removed in `TestMain`). Existing instances can be supplied instead with
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
