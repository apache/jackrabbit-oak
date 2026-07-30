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

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AuditEventTest {

    @Test
    public void defaultGetPayloadReturnsEmptyMap() {
        // Impl that does NOT override getPayload() — exercises the default method body.
        AuditEvent e = new AuditEvent() {
            @Override public @NotNull String getDomain() { return "test"; }
            @Override public @NotNull String getType() { return "t"; }
            @Override public long getTimestamp() { return 0L; }
        };
        assertEquals(Collections.emptyMap(), e.getPayload());
    }

    //---------------------------< static factory of(domain, type, payload) >---

    @Test
    public void factoryWithPayloadReturnsEventWithSuppliedFields() {
        long before = System.currentTimeMillis();
        AuditEvent e = AuditEvent.of("test.domain", "membership.added",
                Map.of("groupPath", "/g", "memberPath", "/u"));
        long after = System.currentTimeMillis();

        assertNotNull(e);
        assertEquals("test.domain", e.getDomain());
        assertEquals("membership.added", e.getType());
        assertEquals(Map.of("groupPath", "/g", "memberPath", "/u"), e.getPayload());
        // Capture timestamp is taken inside of(...) — must fall within the
        // call window observed by the test thread.
        assertTrue("captured timestamp must be in [before, after]",
                before <= e.getTimestamp() && e.getTimestamp() <= after);
    }

    @Test
    public void factoryWithoutPayloadReturnsEventWithEmptyPayload() {
        AuditEvent e = AuditEvent.of("test.domain", "membership.removed");
        assertEquals("test.domain", e.getDomain());
        assertEquals("membership.removed", e.getType());
        assertEquals(Collections.emptyMap(), e.getPayload());
    }

    @Test
    public void factoryRejectsBlankDomain() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AuditEvent.of("", "type", Map.of()));
        assertTrue(ex.getMessage().contains("domain"));
    }

    @Test
    public void factoryRejectsWhitespaceDomain() {
        // String.isBlank() — exercise the non-empty-but-blank path.
        assertThrows(IllegalArgumentException.class,
                () -> AuditEvent.of("   ", "type", Map.of()));
    }

    @Test
    public void factoryRejectsBlankType() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AuditEvent.of("test.domain", "", Map.of()));
        assertTrue(ex.getMessage().contains("type"));
    }

    @Test
    public void factoryRejectsWhitespaceType() {
        assertThrows(IllegalArgumentException.class,
                () -> AuditEvent.of("test.domain", " \t ", Map.of()));
    }

    @Test
    public void noPayloadOverloadAlsoValidatesDomain() {
        // The no-payload overload delegates to the 3-arg form; verify both
        // validation branches still fire via this route.
        assertThrows(IllegalArgumentException.class, () -> AuditEvent.of("", "type"));
        assertThrows(IllegalArgumentException.class, () -> AuditEvent.of("test.domain", ""));
    }

    @Test
    public void factoryPayloadIsImmutable() {
        AuditEvent e = AuditEvent.of("test.domain", "t", Map.of("k", "v"));
        Map<String, Object> p = e.getPayload();
        assertThrows(UnsupportedOperationException.class, () -> p.put("k2", "v2"));
    }

    @Test
    public void factoryDecouplesCallerMap() {
        // Map.copyOf returns the same instance for an already-immutable Map.of result,
        // so we use HashMap to verify the defensive-copy semantics.
        java.util.Map<String, Object> mutable = new java.util.HashMap<>();
        mutable.put("k", "v");
        AuditEvent e = AuditEvent.of("test.domain", "t", mutable);

        mutable.put("k2", "v2"); // mutate the source AFTER construction
        assertEquals("event payload must not reflect post-construction source mutation",
                Map.of("k", "v"), e.getPayload());
    }
}
