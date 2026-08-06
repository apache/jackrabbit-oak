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
package org.apache.jackrabbit.oak.security.audit;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditDispatch;
import org.apache.jackrabbit.oak.spi.audit.AuditType;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AuditEventEmitterImplTest {

    private AtomicReference<AuditEvent> dispatched;

    @Before
    public void installSink() {
        dispatched = new AtomicReference<>();
        AuditDispatch.install(new AuditDispatch.Sink() {
            @Override public boolean isEnabled() { return true; }
            @Override public boolean isEnabledFor(@NotNull AuditDomain domain) { return "yes".equals(domain.name()); }
            @Override public void record(@NotNull Root root, @NotNull AuditEvent event) { /* unused */ }
            @Override public void dispatch(@NotNull AuditEvent event) { dispatched.set(event); }
        });
    }

    @After
    public void tearDown() {
        AuditDispatch.install(null);
    }

    private static AuditEvent fixedEvent(@NotNull AuditDomain domain) {
        return new AuditEvent() {
            @Override public @NotNull AuditDomain getDomain() { return domain; }
            @Override public @NotNull AuditType getType() { return AuditType.of("t"); }
            @Override public long getTimestamp() { return 0L; }
            @Override public @NotNull Map<String, Object> getPayload() { return Collections.emptyMap(); }
        };
    }

    @Test
    public void emitRoutesToFacadeDispatch() {
        AuditEventEmitterImpl impl = new AuditEventEmitterImpl();
        AuditEvent e = fixedEvent(AuditDomain.of("yes"));
        impl.emit(e);
        assertSame(e, dispatched.get());
    }

    @Test
    public void isEnabledForRoutesToFacade() {
        AuditEventEmitterImpl impl = new AuditEventEmitterImpl();
        assertTrue(impl.isEnabledFor(AuditDomain.of("yes")));
        assertFalse(impl.isEnabledFor(AuditDomain.of("no")));
    }
}
