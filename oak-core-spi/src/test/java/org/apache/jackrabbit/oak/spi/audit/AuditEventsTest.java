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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.Root;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AuditEventsTest {

    @After
    public void tearDown() {
        AuditEvents.install(null);
    }

    private static AuditEvent fixedEvent(@NotNull String domain) {
        return new AuditEvent() {
            @Override public @NotNull String getDomain() { return domain; }
            @Override public @NotNull String getType() { return "t"; }
            @Override public long getTimestamp() { return 0L; }
            @Override public @NotNull Map<String, Object> getPayload() { return Collections.emptyMap(); }
        };
    }

    @Test
    public void facadeNoOpWhenNoSinkInstalled() {
        assertFalse(AuditEvents.isEnabled());
        assertFalse(AuditEvents.isEnabledFor("test.domain"));
        AuditEvents.record(mock(Root.class), fixedEvent("test.domain"));
        AuditEvents.dispatch(fixedEvent("test.domain"));
        // no exception, no observable effect — verified by no sink installed
    }

    @Test
    public void recordRoutesThroughInstalledSink() {
        AtomicReference<AuditEvent> received = new AtomicReference<>();
        AuditEvents.install(new AuditEvents.Sink() {
            @Override public boolean isEnabled() { return true; }
            @Override public boolean isEnabledFor(@NotNull String domain) { return true; }
            @Override public void record(@NotNull Root root, @NotNull AuditEvent event) { received.set(event); }
            @Override public void dispatch(@NotNull AuditEvent event) { /* not used */ }
        });
        AuditEvent e = fixedEvent("test.domain");
        AuditEvents.record(mock(Root.class), e);
        assertSame(e, received.get());
    }

    @Test
    public void dispatchRoutesThroughInstalledSink() {
        AtomicReference<AuditEvent> received = new AtomicReference<>();
        AuditEvents.install(new AuditEvents.Sink() {
            @Override public boolean isEnabled() { return true; }
            @Override public boolean isEnabledFor(@NotNull String domain) { return true; }
            @Override public void record(@NotNull Root root, @NotNull AuditEvent event) { /* not used */ }
            @Override public void dispatch(@NotNull AuditEvent event) { received.set(event); }
        });
        AuditEvent e = fixedEvent("aem.content");
        AuditEvents.dispatch(e);
        assertSame(e, received.get());
    }

    @Test
    public void installNullResetsToNoOp() {
        AuditEvents.install(new AuditEvents.Sink() {
            @Override public boolean isEnabled() { return true; }
            @Override public boolean isEnabledFor(@NotNull String domain) { return true; }
            @Override public void record(@NotNull Root root, @NotNull AuditEvent event) { }
            @Override public void dispatch(@NotNull AuditEvent event) { }
        });
        assertTrue(AuditEvents.isEnabled());
        AuditEvents.install(null);
        assertFalse(AuditEvents.isEnabled());
    }
}
