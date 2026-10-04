/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import org.junit.Test;
import org.slf4j.Logger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AeronClusterErrorPolicyTest {

    private final AeronClusterErrorPolicy policy = new AeronClusterErrorPolicy();

    @Test
    public void classifySuppressesDnsPendingErrors() {
        Throwable throwable = new IllegalStateException("UnknownHostException: peer unresolved");

        assertEquals(AeronClusterErrorPolicy.Decision.SUPPRESS_DNS_PENDING, policy.classify(throwable));
    }

    @Test
    public void handleLogsClusterWarningsAtInfo() {
        Logger log = mock(Logger.class);
        Throwable throwable = new IllegalStateException("io.aeron.cluster.client.ClusterEvent: WARN - leader heartbeat timeout");

        policy.handle("Consensus Module", throwable, log);

        verify(log).info("✈️  Aeron Cluster warning (informational): {}", throwable.getMessage());
    }

    @Test
    public void handleLogsHeartbeatTimeoutsAtInfo() {
        Logger log = mock(Logger.class);
        Throwable throwable = new IllegalStateException("leader heartbeat timeout");

        policy.handle("Consensus Module", throwable, log);

        verify(log).info("✈️  Leader heartbeat timeout (normal during election): {}", throwable.getMessage());
    }

    @Test
    public void handleLogsUnexpectedErrors() {
        Logger log = mock(Logger.class);
        Throwable throwable = new IllegalStateException("boom");

        policy.handle("Consensus Module", throwable, log);

        verify(log).error("{} error", "Consensus Module", throwable);
    }
}
