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

import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/**
 * Unit tests for the package-private {@link AuditEventImpl} value holder.
 * <p>
 * Co-located in the same package as the impl so we can construct it
 * directly (it is not exposed in the SPI). The factory paths are covered
 * by {@link AuditEventTest}; this class focuses on the ctor + getters.
 */
public class AuditEventImplTest {

    @Test
    public void ctorStoresFields() {
        Map<String, Object> payload = Map.of("k", "v");
        AuditEventImpl e = new AuditEventImpl("test.domain", "t", 42L, payload);

        assertEquals("test.domain", e.getDomain());
        assertEquals("t", e.getType());
        assertEquals(42L, e.getTimestamp());
        // Factory invariant: payload is stored by reference (already immutable).
        assertSame(payload, e.getPayload());
    }

    @Test
    public void ctorAcceptsEmptyPayload() {
        AuditEventImpl e = new AuditEventImpl("test.domain", "t", 0L, Map.of());
        assertEquals(Map.of(), e.getPayload());
    }

    @Test
    public void payloadInstanceIsImmutable() {
        // AuditEventImpl trusts the caller to pass an immutable Map.
        // We exercise that the contract holds end-to-end by constructing
        // with Map.of() (immutable) and verifying mutation throws.
        AuditEventImpl e = new AuditEventImpl("test.domain", "t", 0L, Map.of("k", "v"));
        assertThrows(UnsupportedOperationException.class,
                () -> e.getPayload().put("k2", "v2"));
    }
}
