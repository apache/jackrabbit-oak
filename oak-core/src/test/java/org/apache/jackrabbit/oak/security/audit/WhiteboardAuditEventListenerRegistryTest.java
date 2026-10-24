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
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditEventListener;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.slf4j.event.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WhiteboardAuditEventListenerRegistryTest {

    private static final class StubListener implements AuditEventListener {
        private final AuditDomain domain;
        private final int rank;
        StubListener(AuditDomain domain, int rank) {
            this.domain = domain;
            this.rank = rank;
        }
        @Override public @NotNull AuditDomain getDomain() { return domain; }
        @Override public int getRank() { return rank; }
        @Override public void onEvents(@NotNull List<AuditEvent> events) { /* not exercised here */ }
    }

    @Test
    public void emptyByDefault() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            assertEquals(0, reg.getListeners().size());
            assertFalse(reg.hasAnyListener());
            assertFalse(reg.hasListenerFor(AuditDomain.of("oak.security")));
        } finally {
            reg.stop();
        }
    }

    @Test
    public void registeredListenerIsListed() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("oak.security"), 0), Map.of());
            assertEquals(1, reg.getListeners().size());
            assertTrue(reg.hasAnyListener());
            assertTrue(reg.hasListenerFor(AuditDomain.of("oak.security")));
        } finally {
            reg.stop();
        }
    }

    @Test
    public void listenersSortedByRankDescending() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("d"), 1), Map.of());
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("d"), 10), Map.of());
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("d"), 5), Map.of());
            List<AuditEventListener> sorted = reg.getListeners();
            assertEquals(3, sorted.size());
            assertEquals(10, sorted.get(0).getRank());
            assertEquals(5, sorted.get(1).getRank());
            assertEquals(1, sorted.get(2).getRank());
        } finally {
            reg.stop();
        }
    }

    /**
     * Pins the live-lookup contract — {@code hasListenerFor} must reflect
     * the current Whiteboard state, not a snapshot taken at start time.
     * Regression guard for the stale-cache bug fixed in commit
     * {@code 3a60d60309}.
     */
    @Test
    public void hasListenerForReflectsLiveRegistrations() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            // First call when no listener for "oak.security" exists.
            assertFalse(reg.hasListenerFor(AuditDomain.of("oak.security")));
            // Register and re-check — must observe the new registration.
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("oak.security"), 0), Map.of());
            assertTrue(reg.hasListenerFor(AuditDomain.of("oak.security")));
            // Different domain must still return false.
            assertFalse(reg.hasListenerFor(AuditDomain.of("example.content")));
        } finally {
            reg.stop();
        }
    }

    /**
     * Listener ordering with a mix of distinct and equal ranks:
     * <ul>
     *   <li>Strict descending order where ranks differ — the highest-rank
     *       listener comes first, the lowest-rank last.</li>
     *   <li>Equal-rank entries appear as a contiguous block; their internal
     *       order is determined by the underlying {@link Whiteboard} (and
     *       must be stable across repeat {@code getListeners()} calls per
     *       the registry Javadoc).</li>
     * </ul>
     * Note: {@link DefaultWhiteboard} stores services in an identity-hash
     * set, so it does NOT preserve registration order for equal-rank
     * entries — only OSGi's {@code OsgiWhiteboard} honors registration
     * order via {@code service.ranking}. This test therefore asserts set
     * equality (not list equality) on the equal-rank block, plus
     * determinism on repeat calls.
     */
    @Test
    public void stableOrderAmongEqualRanks() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            StubListener high = new StubListener(AuditDomain.of("d"), 10);
            StubListener midA = new StubListener(AuditDomain.of("d"), 5);
            StubListener midB = new StubListener(AuditDomain.of("d"), 5);
            StubListener midC = new StubListener(AuditDomain.of("d"), 5);
            StubListener low = new StubListener(AuditDomain.of("d"), 1);
            wb.register(AuditEventListener.class, high, Map.of());
            wb.register(AuditEventListener.class, midA, Map.of());
            wb.register(AuditEventListener.class, midB, Map.of());
            wb.register(AuditEventListener.class, midC, Map.of());
            wb.register(AuditEventListener.class, low, Map.of());

            List<AuditEventListener> sorted = reg.getListeners();
            assertEquals(5, sorted.size());

            // Strict ordering where ranks differ.
            assertSame("highest rank must be first", high, sorted.get(0));
            assertSame("lowest rank must be last", low, sorted.get(4));

            // Equal-rank entries (rank 5) form a contiguous block in
            // positions 1..3 — set equality, not list equality, because
            // DefaultWhiteboard does not preserve registration order.
            Set<AuditEventListener> middle = Set.copyOf(sorted.subList(1, 4));
            assertEquals("middle three positions must hold all rank-5 entries",
                    Set.of(midA, midB, midC), middle);

            // Stable sort: repeat call must return identical order. An
            // unstable sort would reorder the equal-rank entries on the
            // second call even with the same input.
            assertEquals("stable sort — repeat call returns identical order",
                    sorted, reg.getListeners());
        } finally {
            reg.stop();
        }
    }

    //------------------------< broken-listener accessor isolation >-------

    /**
     * Listener whose {@code getDomain()} throws — models a consumer bundle
     * with a broken classpath ({@link LinkageError} is exactly what a
     * missing transitive dependency produces at first call).
     */
    private static final class ThrowingDomainListener implements AuditEventListener {
        @Override public @NotNull AuditDomain getDomain() {
            throw new LinkageError("synthetic-getDomain");
        }
        @Override public int getRank() { return 100; }
        @Override public void onEvents(@NotNull List<AuditEvent> events) {
            fail("a listener with a broken getDomain() must never receive events");
        }
    }

    /**
     * Listener whose {@code getRank()} throws while {@code getDomain()}
     * works — exercises the rank-snapshot guard in {@code getListeners()}
     * independently of the domain guard.
     */
    private static final class ThrowingRankListener implements AuditEventListener {
        private final AuditDomain domain;
        ThrowingRankListener(AuditDomain domain) {
            this.domain = domain;
        }
        @Override public @NotNull AuditDomain getDomain() { return domain; }
        @Override public int getRank() {
            throw new RuntimeException("synthetic-getRank");
        }
        @Override public void onEvents(@NotNull List<AuditEvent> events) {
            fail("a listener with a broken getRank() must never receive events");
        }
    }

    /**
     * {@code getDomain()}/{@code getRank()} are listener code just like
     * {@code onEvents()} — the per-listener isolation barrier documented on
     * {@link AuditEventListener} must cover them too. A lone broken
     * listener must make {@code hasListenerFor} return {@code false}, not
     * propagate the {@link LinkageError} into the capture gate.
     */
    @Test
    public void hasListenerForToleratesThrowingGetDomain() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            wb.register(AuditEventListener.class, new ThrowingDomainListener(), Map.of());
            assertFalse("broken listener must be skipped, not propagated",
                    reg.hasListenerFor(AuditDomain.of("oak.security")));
        } finally {
            reg.stop();
        }
    }

    /**
     * A broken peer must not mask a healthy listener: the registry skips
     * the throwing listener and keeps scanning.
     */
    @Test
    public void hasListenerForFindsHealthyListenerDespiteBrokenPeer() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            wb.register(AuditEventListener.class, new ThrowingDomainListener(), Map.of());
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("oak.security"), 0), Map.of());
            assertTrue("healthy listener must be found despite broken peer",
                    reg.hasListenerFor(AuditDomain.of("oak.security")));
        } finally {
            reg.stop();
        }
    }

    /**
     * A throwing {@code getRank()} must exclude that listener from the
     * sorted view instead of blowing up the sort — the rank comparator
     * would otherwise rethrow from {@code List.sort} and abort dispatch
     * for every listener.
     */
    @Test
    public void getListenersSkipsListenerWhoseGetRankThrows() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            wb.register(AuditEventListener.class, new ThrowingRankListener(AuditDomain.of("d")), Map.of());
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("d"), 10), Map.of());
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("d"), 1), Map.of());
            List<AuditEventListener> out = reg.getListeners();
            assertEquals("broken-rank listener must be skipped", 2, out.size());
            assertEquals(10, out.get(0).getRank());
            assertEquals(1, out.get(1).getRank());
        } finally {
            reg.stop();
        }
    }

    /**
     * The skip semantics must not depend on listener count: a broken
     * listener that happens to be the only registration must be skipped
     * too, not returned unvetted through a single-element fast path.
     */
    @Test
    public void getListenersSkipsThrowingGetRankEvenWhenAlone() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            wb.register(AuditEventListener.class, new ThrowingRankListener(AuditDomain.of("d")), Map.of());
            assertTrue("a lone broken listener must be skipped, not returned",
                    reg.getListeners().isEmpty());
        } finally {
            reg.stop();
        }
    }

    /**
     * Skipping a broken listener is logged at WARN exactly once per
     * listener identity across all registry methods — repeated capture-gate
     * polling must not flood the log on behalf of a broken bundle.
     */
    @Test
    public void brokenListenerIsLoggedAtWarnOncePerListener() {
        LogCustomizer logs = LogCustomizer
                .forLogger(WhiteboardAuditEventListenerRegistry.class.getName())
                .enable(Level.WARN)
                .create();
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        logs.starting();
        try {
            wb.register(AuditEventListener.class, new ThrowingDomainListener(), Map.of());
            reg.hasListenerFor(AuditDomain.of("oak.security"));
            reg.hasListenerFor(AuditDomain.of("oak.security"));
            reg.getListeners();
            assertEquals("exactly one WARN per broken listener identity",
                    1, logs.getLogs().size());
        } finally {
            logs.finished();
            reg.stop();
        }
    }

    /**
     * {@code getListeners()} promises an immutable snapshot in its Javadoc;
     * the multi-listener (sorted) path must honor it like the other paths.
     */
    @Test
    public void getListenersReturnsImmutableList() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("d"), 10), Map.of());
            wb.register(AuditEventListener.class, new StubListener(AuditDomain.of("d"), 1), Map.of());
            List<AuditEventListener> out = reg.getListeners();
            try {
                out.add(new StubListener(AuditDomain.of("d"), 0));
                fail("getListeners() must return an immutable list");
            } catch (UnsupportedOperationException expected) {
                // contract honored
            }
        } finally {
            reg.stop();
        }
    }

    /**
     * Unregistering a listener via the {@link Registration#unregister()}
     * handle must remove it from {@link WhiteboardAuditEventListenerRegistry#getListeners()}
     * and from {@link WhiteboardAuditEventListenerRegistry#hasListenerFor(String)}.
     */
    @Test
    public void unregisterRemovesListener() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardAuditEventListenerRegistry reg = new WhiteboardAuditEventListenerRegistry();
        reg.start(wb);
        try {
            Registration r = wb.register(AuditEventListener.class,
                    new StubListener(AuditDomain.of("oak.security"), 0), Map.of());
            assertEquals(1, reg.getListeners().size());
            assertTrue(reg.hasListenerFor(AuditDomain.of("oak.security")));

            r.unregister();

            assertEquals(0, reg.getListeners().size());
            assertFalse(reg.hasListenerFor(AuditDomain.of("oak.security")));
            assertFalse(reg.hasAnyListener());
        } finally {
            reg.stop();
        }
    }
}
