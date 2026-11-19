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
package org.apache.jackrabbit.oak.spi.audit;

import java.util.Collections;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AuditEventListenerTest {

    @Test
    public void defaultRankIsZero() {
        AuditEventListener listener = new AuditEventListener() {
            @Override
            public @NotNull AuditDomain getDomain() {
                return AuditDomain.of("test");
            }

            @Override
            public void onEvents(@NotNull List<AuditEvent> events) {
                // no-op for this test
            }
        };
        assertEquals(0, listener.getRank());
    }

    @Test
    public void listenerReceivesCallToOnEvents() {
        // Per AuditEventListener.onEvents Javadoc: "the non-empty list of
        // events for this listener's domain." Test exercises the contract
        // with a non-empty list — passing emptyList would contradict the
        // documented contract.
        AuditEvent event = new AuditEvent() {
            @Override public @NotNull AuditDomain getDomain() { return AuditDomain.of("test"); }
            @Override public @NotNull AuditType getType() { return AuditType.of("t"); }
            @Override public long getTimestamp() { return 0L; }
        };
        List<AuditEvent> input = Collections.singletonList(event);

        final List<AuditEvent>[] received = new List[]{null};
        AuditEventListener listener = new AuditEventListener() {
            @Override
            public @NotNull AuditDomain getDomain() {
                return AuditDomain.of("test");
            }

            @Override
            public void onEvents(@NotNull List<AuditEvent> events) {
                received[0] = events;
            }
        };
        listener.onEvents(input);
        assertEquals(1, received[0].size());
        assertEquals(event, received[0].get(0));
    }
}
