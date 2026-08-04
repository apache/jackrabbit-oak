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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Direct unit tests for {@link AuditBuffer}, the per-thread, per-session
 * staging area for commit-attached audit events. Exercises the public
 * package-private surface ({@code record} / {@code peek} / {@code drain} /
 * {@code clearAll}) plus the {@code AuditBufferLifecycle.Listener} callbacks,
 * the defensive-copy contract of {@code peek}, the soft per-session cap, and
 * the {@link ThreadLocal} thread-confinement (including the drain-from-wrong-
 * thread guard).
 */
public class AuditBufferTest {

    private static final String SESSION = "session-1";
    private static final String OTHER_SESSION = "session-2";
    private static final AuditDomain DOMAIN = AuditDomain.of("test.domain");

    private AuditBuffer buffer;

    @Before
    public void setUp() {
        buffer = new AuditBuffer();
    }

    @After
    public void tearDown() {
        // Drop any ThreadLocal residue on the test thread.
        buffer.clearAll();
    }

    private static AuditEvent event(AuditType type) {
        return AuditEvent.of(DOMAIN, type);
    }

    //--------------------------------------------------< record / peek / drain >---

    @Test
    public void recordThenPeekReturnsStagedEventsInOrder() {
        buffer.record(SESSION, event(AuditType.of("a")));
        buffer.record(SESSION, event(AuditType.of("b")));

        List<AuditEvent> staged = buffer.peek(SESSION);
        assertEquals(2, staged.size());
        assertEquals("a", staged.get(0).getType().name());
        assertEquals("b", staged.get(1).getType().name());
    }

    @Test
    public void peekReturnsNullWhenNothingStaged() {
        assertNull(buffer.peek(SESSION));
    }

    @Test
    public void recordIsPerSession() {
        buffer.record(SESSION, event(AuditType.of("a")));
        buffer.record(OTHER_SESSION, event(AuditType.of("x")));
        buffer.record(OTHER_SESSION, event(AuditType.of("y")));

        assertEquals(1, buffer.peek(SESSION).size());
        assertEquals(2, buffer.peek(OTHER_SESSION).size());
    }

    /**
     * {@code peek} returns a defensive copy: mutating the returned list must
     * not affect the buffer, and a fresh {@code peek} still observes the
     * original staged events.
     */
    @Test
    public void peekReturnsDefensiveCopy() {
        buffer.record(SESSION, event(AuditType.of("a")));
        List<AuditEvent> first = buffer.peek(SESSION);

        // The returned list is immutable (List.copyOf) — structural mutation throws.
        assertThrows(UnsupportedOperationException.class, () -> first.add(event(AuditType.of("injected"))));

        // And it is decoupled from the backing list: recording more does not
        // grow the previously-returned snapshot.
        buffer.record(SESSION, event(AuditType.of("b")));
        assertEquals("earlier peek snapshot must be decoupled", 1, first.size());
        assertEquals("buffer itself reflects the new event", 2, buffer.peek(SESSION).size());
    }

    @Test
    public void drainReturnsStagedEventsAndEmptiesSession() {
        AuditEvent a = event(AuditType.of("a"));
        AuditEvent b = event(AuditType.of("b"));
        buffer.record(SESSION, a);
        buffer.record(SESSION, b);

        List<AuditEvent> drained = buffer.drain(SESSION);
        assertEquals(2, drained.size());
        assertSame(a, drained.get(0));
        assertSame(b, drained.get(1));
        assertNull("session must be empty after drain", buffer.peek(SESSION));
    }

    @Test
    public void drainReturnsNullWhenNothingStaged() {
        assertNull(buffer.drain(SESSION));
    }

    @Test
    public void drainIsScopedToTheRequestedSession() {
        buffer.record(SESSION, event(AuditType.of("a")));
        buffer.record(OTHER_SESSION, event(AuditType.of("x")));

        buffer.drain(SESSION);
        assertNull(buffer.peek(SESSION));
        assertEquals("other session must be untouched", 1, buffer.peek(OTHER_SESSION).size());
    }

    //--------------------------------------------------< lifecycle callbacks >---

    @Test
    public void onRefreshDrainsSession() {
        buffer.record(SESSION, event(AuditType.of("a")));
        buffer.onRefresh(SESSION);
        assertNull(buffer.peek(SESSION));
    }

    @Test
    public void onCommitFailedDrainsSession() {
        buffer.record(SESSION, event(AuditType.of("a")));
        buffer.onCommitFailed(SESSION);
        assertNull(buffer.peek(SESSION));
    }

    @Test
    public void clearAllRemovesCurrentThreadEvents() {
        buffer.record(SESSION, event(AuditType.of("a")));
        buffer.record(OTHER_SESSION, event(AuditType.of("x")));
        buffer.clearAll();
        assertNull(buffer.peek(SESSION));
        assertNull(buffer.peek(OTHER_SESSION));
    }

    //--------------------------------------------------< soft per-session cap >---

    /**
     * Once a session reaches {@link AuditBuffer#MAX_EVENTS_PER_SESSION}
     * staged events, further events are dropped and a single WARN is logged
     * for the session — not one WARN per dropped event.
     */
    @Test
    public void overflowDropsBeyondCapAndWarnsOncePerSession() {
        LogCustomizer log = LogCustomizer.forLogger(AuditBuffer.class)
                .enable(Level.WARN).create();
        log.starting();
        try {
            for (int i = 0; i < AuditBuffer.MAX_EVENTS_PER_SESSION + 5; i++) {
                buffer.record(SESSION, event(AuditType.of("e" + i)));
            }
            assertEquals("buffer must be capped at the maximum",
                    AuditBuffer.MAX_EVENTS_PER_SESSION, buffer.peek(SESSION).size());
            assertEquals("exactly one WARN must be logged for the overflowing session",
                    1, log.getLogs().size());
            assertTrue("WARN must name the session; was: " + log.getLogs().get(0),
                    log.getLogs().get(0).contains(SESSION));
        } finally {
            log.finished();
        }
    }

    /**
     * The overflow warning re-arms once the session slot is cleared: a second
     * overflow episode (after a drain) logs its own WARN. Pins that the
     * once-per-session flag lives on the per-session slot, which is recreated
     * on the next {@code record} after a drain.
     */
    @Test
    public void overflowWarningReArmsAfterDrain() {
        LogCustomizer log = LogCustomizer.forLogger(AuditBuffer.class)
                .enable(Level.WARN).create();
        log.starting();
        try {
            for (int i = 0; i <= AuditBuffer.MAX_EVENTS_PER_SESSION; i++) {
                buffer.record(SESSION, event(AuditType.of("first" + i)));
            }
            buffer.drain(SESSION);
            for (int i = 0; i <= AuditBuffer.MAX_EVENTS_PER_SESSION; i++) {
                buffer.record(SESSION, event(AuditType.of("second" + i)));
            }
            assertEquals("a fresh overflow episode after drain must WARN again",
                    2, log.getLogs().size());
        } finally {
            log.finished();
        }
    }

    //--------------------------------------------------< thread confinement >---

    /**
     * The staging area is a {@link ThreadLocal}: another thread neither
     * observes ({@code peek}) nor drains this thread's staged events, and a
     * drain issued from the wrong thread leaves this thread's events intact
     * (the drain-from-wrong-thread guard).
     */
    @Test
    public void stagedEventsAreThreadConfined() throws InterruptedException {
        buffer.record(SESSION, event(AuditType.of("a")));

        AtomicReference<List<AuditEvent>> otherPeek = new AtomicReference<>();
        AtomicReference<List<AuditEvent>> otherDrain = new AtomicReference<>();
        Thread other = new Thread(() -> {
            otherPeek.set(buffer.peek(SESSION));
            otherDrain.set(buffer.drain(SESSION));
        });
        other.start();
        other.join();

        assertNull("another thread must not observe this thread's events", otherPeek.get());
        assertNull("drain from another thread must return null", otherDrain.get());
        // The wrong-thread drain must not have touched this thread's slot.
        assertEquals("this thread's events must survive a wrong-thread drain",
                1, buffer.peek(SESSION).size());
    }
}
