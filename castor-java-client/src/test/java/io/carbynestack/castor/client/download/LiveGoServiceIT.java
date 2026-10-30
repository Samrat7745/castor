/*
 * Copyright (c) 2026 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.download;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.carbynestack.castor.common.entities.TelemetryData;
import io.carbynestack.castor.common.entities.TupleList;
import io.carbynestack.castor.common.entities.TupleMetric;
import io.carbynestack.castor.common.entities.TupleType;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Runs the existing Java client against a live Castor holding tuples (used to verify the Go port is a drop-in). Not part
 * of the default surefire run: {@code CASTOR_URL=http://h:p ./mvnw -o -pl castor-java-client -am test
 * -Dtest=LiveGoServiceIT -Dsurefire.failIfNoSpecifiedTests=false}.
 */
class LiveGoServiceIT {
  @Test
  void downloadsTuplesAndTelemetryFromLiveService() throws Exception {
    String url = System.getenv("CASTOR_URL");
    assumeTrue(url != null && !url.isEmpty(), "CASTOR_URL not set");
    CastorIntraVcpClient client = DefaultCastorIntraVcpClient.builder(url).build();
    TelemetryData td = client.getTelemetryData();
    assertFalse(td.getMetrics().isEmpty());
    TupleMetric available =
        td.getMetrics().stream().filter(m -> m.getAvailable() > 0).findFirst().orElse(null);
    assumeTrue(available != null, "no tuples available on the service");
    TupleType type = available.getType();
    TupleList list = client.downloadTupleShares(UUID.randomUUID(), type, 1);
    assertEquals(1, list.size());
    assertNotNull(client.getTelemetryData(30));
  }
}
