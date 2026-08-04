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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditEventEmitter;
import org.apache.jackrabbit.oak.spi.audit.AuditEventListener;
import org.apache.jackrabbit.oak.spi.audit.AuditEvents;
import org.apache.jackrabbit.oak.spi.audit.AuditType;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end integration test exercising both audit pipelines through a
 * direct {@link AuditPipeline} install. Uses {@link MemoryNodeStore}
 * for a real but in-process Oak instance.
 * <p>
 * Wiring path:
 * <ol>
 *   <li>{@link AuditPipeline#initialize(Whiteboard)} installs the
 *       audit feature toggle, listener registry, buffer and capture-time
 *       sink onto the whiteboard.</li>
 *   <li>{@code store.addObserver(audit.getDrainObserver())} attaches the
 *       {@link AuditDrainObserver} directly to the {@code MemoryNodeStore}.
 *       We don't use {@code Oak.with(Observer)} because we pass a custom
 *       whiteboard via {@code Oak.with(Whiteboard)}, which replaces Oak's
 *       default anonymous-override whiteboard and bypasses the auto-attach
 *       at {@code Oak.java:300-302}.</li>
 *   <li>Teardown closes the {@code Observable.addObserver} {@code Closeable}
 *       and calls {@link AuditPipeline#dispose()} — the same code
 *       path that OSGi {@code @Deactivate} uses.</li>
 * </ol>
 * No test mirror of {@code BufferSink} or observer wiring exists in this
 * class; a bug in either pipeline will surface here.
 */
public class AuditPipelineTest {

    private static final AuditDomain DOMAIN = AuditDomain.of("test.domain");
    private static final AuditDomain OTHER_DOMAIN = AuditDomain.of("other.domain");
    private static final String FEATURE_TOGGLE_NAME = AuditPipeline.FEATURE_TOGGLE_NAME;

    private Whiteboard whiteboard;
    private AuditPipeline auditConfig;
    private Closeable drainObserverSubscription;
    private Registration listenerRegistration;
    private List<AuditEvent> received;
    private ContentRepository repository;
    private AuditEventEmitter emitter;
    private SecurityProvider securityProvider;

    @Before
    public void setUp() {
        whiteboard = new DefaultWhiteboard();
        received = new CopyOnWriteArrayList<>();

        auditConfig = new AuditPipeline();
        // initialize() installs sinks/registry/buffer/toggle; the drain Observer
        // is attached per-store below via Observable.addObserver(...).
        auditConfig.initialize(whiteboard);
        securityProvider = SecurityProviderBuilder.newBuilder()
                .withWhiteboard(whiteboard)
                .build();

        // JAAS — wire the default authentication configuration from the
        // SecurityProvider's params so repository.login(adminCreds) succeeds.
        Configuration.setConfiguration(
                ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY));

        // Flip the feature toggle ON via the FeatureToggle service the
        // AuditPipeline.initialize() call registered on the
        // whiteboard.
        setToggle(true);

        // Register a domain-scoped listener that captures events for
        // verification. Single listener tests use DOMAIN; multi-listener
        // tests register additional listeners inline.
        AuditEventListener listener = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                received.addAll(events);
            }
        };
        listenerRegistration = whiteboard.register(AuditEventListener.class, listener, Map.of());

        MemoryNodeStore store = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        // Bare-metal embedded wiring: attach the drain observer directly to the
        // Observable. We can't use Oak.with(Observer) here because the test
        // replaces Oak's default whiteboard via .with(whiteboard), which bypasses
        // the auto-attach at Oak.java:300-302. See AuditPipelineTest class Javadoc.
        drainObserverSubscription = store.addObserver(auditConfig.getDrainObserver());

        repository = new Oak(store)
                .with(securityProvider)
                .with(whiteboard)
                .createContentRepository();

        emitter = new AuditEventEmitterImpl();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (drainObserverSubscription != null) {
                drainObserverSubscription.close();
            }
            if (listenerRegistration != null) {
                listenerRegistration.unregister();
            }
            if (auditConfig != null) {
                auditConfig.dispose();
            }
            if (repository instanceof Closeable) {
                ((Closeable) repository).close();
            }
        } finally {
            Configuration.setConfiguration(null);
        }
    }

    private static Credentials adminCredentials() {
        return new SimpleCredentials("admin", "admin".toCharArray());
    }

    private void setToggle(boolean enabled) {
        Tracker<FeatureToggle> toggleTracker = whiteboard.track(FeatureToggle.class);
        try {
            for (FeatureToggle ft : toggleTracker.getServices()) {
                if (FEATURE_TOGGLE_NAME.equals(ft.getName())) {
                    ft.setEnabled(enabled);
                }
            }
        } finally {
            toggleTracker.stop();
        }
    }

    private static AuditEvent eventFor(@NotNull AuditDomain domain,
                                       @NotNull AuditType type,
                                       @NotNull Map<String, Object> payload) {
        return new AuditEvent() {
            @Override public @NotNull AuditDomain getDomain() { return domain; }
            @Override public @NotNull AuditType getType() { return type; }
            @Override public long getTimestamp() { return System.currentTimeMillis(); }
            @Override public @NotNull Map<String, Object> getPayload() { return payload; }
        };
    }

    private ContentSession login() throws Exception {
        return repository.login(adminCredentials(), null);
    }

    //--------------------------------------------------------< original 3 >---

    @Test
    public void fireAndForgetEventCarriesNoCommitMetadata() {
        emitter.emit(eventFor(DOMAIN, AuditType.of("forget"), Map.of("key", "v")));
        assertEquals(1, received.size());
        AuditEvent e = received.get(0);
        assertEquals("forget", e.getType().name());
        assertFalse("fire-and-forget event must not carry commit.sessionId",
                e.getPayload().containsKey("oak.commit.sessionId"));
        assertFalse(e.getPayload().containsKey("oak.commit.userId"));
        assertEquals("v", e.getPayload().get("key"));
    }

    @Test
    public void emitNoListenerForDomainIsNoOp() {
        emitter.emit(eventFor(OTHER_DOMAIN, AuditType.of("x"), Map.of()));
        assertTrue(received.isEmpty());
    }

    @Test
    public void commitAttachedEventCarriesCommitMetadata() throws Exception {
        try (ContentSession session = login()) {
            Root root = session.getLatestRoot();
            AuditEvents.record(
                    root, eventFor(DOMAIN, AuditType.of("commit.type"), Map.of("note", "v")));
            root.getTree("/").setProperty("scratch", "value");
            root.commit();

            assertEquals(1, received.size());
            AuditEvent e = received.get(0);
            assertEquals("commit.type", e.getType().name());
            Map<String, Object> p = e.getPayload();
            assertTrue("commit-attached event must carry commit.sessionId",
                    p.containsKey("oak.commit.sessionId"));
            assertTrue(p.containsKey("oak.commit.userId"));
            assertTrue(p.containsKey("oak.commit.timestamp"));
            assertEquals("v", p.get("note"));
        }
    }

    //--------------------------< fire-and-forget attestation-key strip >---

    /**
     * Trust-contract regression (fire-and-forget half): an emitter that
     * pre-populates the three Oak-attested keys ({@code commit.sessionId},
     * {@code commit.userId}, {@code commit.timestamp}) must NOT get them
     * delivered to listeners — {@code BufferSink.dispatch} strips exactly
     * those three so their presence in a dispatched payload is a reliable
     * "Oak-attested commit-attached event" signal (see
     * {@link AuditEvent#getPayload()}). Without the strip, any bundle could
     * forge commit identity in audit logs (CWE-345).
     * <p>
     * Only the three reserved keys are stripped: an arbitrary
     * {@code commit.*}-prefixed passenger key and ordinary payload entries
     * are forwarded verbatim — mirrors
     * {@code CommitMetadataDecoratorTest#decoratorDoesNotProtectOtherCommitPrefixedKeys}
     * on the commit-attached half.
     */
    @Test
    public void fireAndForgetStripsForgedCommitAttestationKeys() {
        emitter.emit(eventFor(DOMAIN, AuditType.of("forged"), Map.of(
                "oak.commit.sessionId", "forged-session",
                "oak.commit.userId", "forged-admin",
                "oak.commit.timestamp", 99999999L,
                "commit.custom", "passenger",
                "key", "v")));

        assertEquals(1, received.size());
        Map<String, Object> p = received.get(0).getPayload();
        assertFalse("forged commit.sessionId must be stripped on fire-and-forget dispatch",
                p.containsKey("oak.commit.sessionId"));
        assertFalse("forged commit.userId must be stripped on fire-and-forget dispatch",
                p.containsKey("oak.commit.userId"));
        assertFalse("forged commit.timestamp must be stripped on fire-and-forget dispatch",
                p.containsKey("oak.commit.timestamp"));
        assertEquals("non-reserved commit.* keys are forwarded verbatim (untrusted)",
                "passenger", p.get("commit.custom"));
        assertEquals("ordinary payload entries are forwarded verbatim", "v", p.get("key"));
        assertEquals("domain must survive the strip", DOMAIN, received.get(0).getDomain());
        assertEquals("type must survive the strip", "forged", received.get(0).getType().name());
    }

    /**
     * Conditional-wrap pin (passes before and after the strip fix; guards
     * the GREEN implementation shape): an event WITHOUT any reserved
     * {@code commit.*} key is dispatched as the SAME instance — no
     * defensive wrapping, no payload copy. Emitters relying on concrete
     * event subtypes (typed accessors) keep working on the
     * fire-and-forget path as long as their payloads are clean.
     */
    @Test
    public void fireAndForgetCleanPayloadDispatchesSameEventInstance() {
        AuditEvent clean = eventFor(DOMAIN, AuditType.of("clean"), Map.of("key", "v"));
        emitter.emit(clean);

        assertEquals(1, received.size());
        assertSame("clean payloads must not be re-wrapped — concrete event type preserved",
                clean, received.get(0));
    }

    //------------------------------------------------< new — discard tests >---

    /**
     * After a failed commit, the events staged in the per-session buffer
     * must be discarded. The strongest assertion is end-to-end: do a
     * SUBSEQUENT successful commit on the same session and verify only the
     * fresh event arrives. A naive "buffer empty after failure" assertion
     * would pass under a regression that drained the wrong session's slot.
     */
    @Test
    public void commitFailureDiscardsStagedEvents() throws Exception {
        // Build a separate Oak instance with an injected throwing validator
        // — the main fixture's repository can't carry the validator without
        // breaking the success-path tests. The audit pipeline state on the
        // whiteboard is shared, which is what we want to exercise.
        MemoryNodeStore store2 = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        Closeable observer2 = store2.addObserver(auditConfig.getDrainObserver());
        ContentRepository repo2 = new Oak(store2)
                .with(securityProvider)
                .with(whiteboard)
                .with(new ThrowingValidatorProvider("trigger-failure"))
                .createContentRepository();
        try (ContentSession session = repo2.login(adminCredentials(), null)) {
            // Stage E1, then force commit failure via the trigger property.
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("discarded"),
                            Map.of("trace.id", "E1-from-failed-commit")));
            r1.getTree("/").setProperty("trigger-failure", "boom");
            try {
                r1.commit();
                fail("Expected CommitFailedException from injected validator");
            } catch (CommitFailedException expected) {
                // expected
            }

            // Subsequent successful commit on the SAME session.
            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("delivered"),
                            Map.of("trace.id", "E2-from-successful-commit")));
            r2.getTree("/").setProperty("scratch", "value");
            r2.commit();

            assertEquals("only E2 must be delivered", 1, received.size());
            AuditEvent d = received.get(0);
            assertEquals("event type is E2's", "delivered", d.getType().name());
            assertEquals("payload is E2's, not E1's or merged",
                    "E2-from-successful-commit", d.getPayload().get("trace.id"));
            assertEquals("commit.sessionId decorates with current session",
                    session.toString(), d.getPayload().get("oak.commit.sessionId"));
        } finally {
            observer2.close();
            if (repo2 instanceof Closeable) {
                ((Closeable) repo2).close();
            }
        }
    }

    /**
     * After {@code root.refresh()} the staged events for the session must
     * be discarded — mirrors {@link #commitFailureDiscardsStagedEvents()}.
     */
    @Test
    public void refreshDiscardsStagedEvents() throws Exception {
        try (ContentSession session = login()) {
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("discarded"),
                            Map.of("trace.id", "E1-discarded-by-refresh")));
            r1.refresh();

            // After refresh, dispatch a fresh event and commit.
            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("delivered"),
                            Map.of("trace.id", "E2-after-refresh")));
            r2.getTree("/").setProperty("scratch", "v");
            r2.commit();

            assertEquals("only E2 must be delivered", 1, received.size());
            AuditEvent d = received.get(0);
            assertEquals("delivered", d.getType().name());
            assertEquals("E2-after-refresh", d.getPayload().get("trace.id"));
        }
    }

    /**
     * After {@code root.rebase()} the staged events for the session must be
     * PRESERVED — rebase keeps the session's transient changes (they are
     * replayed on the new base), so the audit events captured alongside them
     * must survive too and be dispatched on the eventual commit.
     * <p>
     * <strong>Intentional behavior change (review CONCERN):</strong> a prior
     * iteration drained the buffer on rebase via {@code onRefresh}; that
     * dropped audit events for changes that survived the rebase. The drain
     * was removed from {@code MutableRoot.rebase()}. Contrast with
     * {@link #refreshDiscardsStagedEvents()} — refresh discards transient
     * changes and so still drains.
     */
    @Test
    public void rebasePreservesStagedEvents() throws Exception {
        try (ContentSession session = login()) {
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("preserved"),
                            Map.of("trace.id", "E1-survives-rebase")));
            r1.rebase();

            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("delivered"),
                            Map.of("trace.id", "E2-after-rebase")));
            r2.getTree("/").setProperty("scratch", "v");
            r2.commit();

            // Both events delivered, in capture order — E1 survived the rebase.
            assertEquals("rebase must PRESERVE staged events; E1 and E2 both delivered",
                    2, received.size());
            assertEquals("E1-survives-rebase",
                    received.get(0).getPayload().get("trace.id"));
            assertEquals("E2-after-rebase",
                    received.get(1).getPayload().get("trace.id"));
        }
    }

    //------------------------------< gate-transition tests >---
    // These tests pin how the {@code MutableRoot} lifecycle callouts behave
    // across a gate transition: the audit gate flips OFF between capture and
    // the lifecycle event, then ON again before the next commit. The gate
    // factors as {@code featureToggle.isEnabled() && registry.hasAnyListener()}
    // (see {@code AuditPipeline.BufferSink.isEnabled}) so it can flip
    // OFF via two functionally identical sources:
    //   1. Toggle flicker — {@code FT_OAK-12331} flipped off at runtime.
    //   2. Listener churn — the only registered listener deregisters.
    //
    // refresh() and commit-failure MUST drain even when the gate is OFF at
    // callout time — otherwise a stale event survives the gate-OFF window and
    // is later dispatched against a LATER commit's metadata (misattribution).
    // Those callouts are therefore UNCONDITIONAL in MutableRoot. → 4 tests.
    //
    // rebase() is different: it PRESERVES the session's transient changes, so
    // it intentionally does NOT drain (the audit events captured alongside
    // those surviving changes must survive too). The 2 rebase variants below
    // therefore assert PRESERVATION across the same gate transitions. → 2 tests.
    // (Six gate-transition regression tests total.)

    /**
     * Toggle flips OFF between capture and refresh: the lifecycle
     * callout MUST still fire and drain the buffer. Pins
     * {@code MutableRoot.refresh()} ALWAYS calling
     * {@code AuditBufferLifecycle.onRefresh(sessionId)} — without the
     * gate that an earlier iteration added on
     * {@code AuditEvents.isEnabled()}.
     */
    @Test
    public void refreshDiscardsStagedEventsAcrossToggleFlicker() throws Exception {
        try (ContentSession session = login()) {
            // Capture E1 with gate=ON.
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("staged-by-r1"),
                            Map.of("trace.id", "E1-must-not-leak-via-toggle")));

            // Flip gate OFF via toggle.
            setToggle(false);

            // Refresh: the callout must drain the buffer despite the
            // gate being off. Without this guarantee, E1 survives.
            r1.refresh();

            // Flip gate ON for the subsequent commit + dispatch.
            setToggle(true);

            // Capture E2 and commit on the SAME session.
            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("delivered-by-r2"),
                            Map.of("trace.id", "E2-current")));
            r2.getTree("/").setProperty("scratch", "v");
            r2.commit();

            // Strict: exactly one event, and it must be E2. If the
            // lifecycle drain was skipped, received would carry both
            // E1 (decorated with r2's commit metadata — the integrity
            // violation) and E2.
            assertEquals("only E2 must be delivered; E1 must NOT survive the toggle-flicker",
                    1, received.size());
            AuditEvent d = received.get(0);
            assertEquals("delivered-by-r2", d.getType().name());
            assertEquals("E2-current", d.getPayload().get("trace.id"));
        }
    }

    /**
     * Rebase variant of {@link #refreshDiscardsStagedEventsAcrossToggleFlicker}
     * — but INVERTED: rebase PRESERVES staged events (it does not drain). The
     * event captured before the rebase survives the toggle flicker and the
     * rebase, and is delivered (alongside the post-rebase event) on the
     * eventual commit, each keeping its own payload.
     */
    @Test
    public void rebasePreservesStagedEventsAcrossToggleFlicker() throws Exception {
        try (ContentSession session = login()) {
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("staged-by-r1"),
                            Map.of("trace.id", "E1-survives-rebase-toggle")));

            setToggle(false);
            r1.rebase();
            setToggle(true);

            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("delivered-by-r2"),
                            Map.of("trace.id", "E2-current")));
            r2.getTree("/").setProperty("scratch", "v");
            r2.commit();

            assertEquals("rebase preserves E1 across the toggle flicker; E1 and E2 both delivered",
                    2, received.size());
            assertEquals("E1-survives-rebase-toggle",
                    received.get(0).getPayload().get("trace.id"));
            assertEquals("E2-current",
                    received.get(1).getPayload().get("trace.id"));
        }
    }

    /**
     * Commit-failure variant of {@link #refreshDiscardsStagedEventsAcrossToggleFlicker}.
     * Pins the {@code finally if (!merged) onCommitFailed(...)} branch
     * in {@code MutableRoot.commit()} firing even when the gate is off.
     * Uses a separate Oak instance with the same throwing validator as
     * {@link #commitFailureDiscardsStagedEvents()} for a deterministic
     * commit failure.
     */
    @Test
    public void commitFailureDiscardsStagedEventsAcrossToggleFlicker() throws Exception {
        MemoryNodeStore store2 = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        Closeable observer2 = store2.addObserver(auditConfig.getDrainObserver());
        ContentRepository repo2 = new Oak(store2)
                .with(securityProvider)
                .with(whiteboard)
                .with(new ThrowingValidatorProvider("trigger-failure"))
                .createContentRepository();
        try (ContentSession session = repo2.login(adminCredentials(), null)) {
            // Capture E1 with gate=ON.
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("staged-by-failed-r1"),
                            Map.of("trace.id", "E1-must-not-leak-via-toggle-failure")));

            // Flip gate OFF, then trigger a deterministic commit failure.
            // The failing commit must still drain the buffer via the
            // finally-block callout — that's the invariant under test.
            setToggle(false);
            r1.getTree("/").setProperty("trigger-failure", "boom");
            try {
                r1.commit();
                fail("Expected CommitFailedException from injected validator");
            } catch (CommitFailedException expected) {
                // expected
            }

            // Flip gate ON for the subsequent successful commit + dispatch.
            setToggle(true);

            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("delivered-by-r2"),
                            Map.of("trace.id", "E2-current")));
            r2.getTree("/").setProperty("scratch", "v");
            r2.commit();

            assertEquals("only E2 must be delivered; E1 must NOT survive the toggle-flicker around commit-failure",
                    1, received.size());
            assertEquals("E2-current",
                    received.get(0).getPayload().get("trace.id"));
        } finally {
            observer2.close();
            if (repo2 instanceof Closeable) {
                ((Closeable) repo2).close();
            }
        }
    }

    /**
     * Listener-churn variant of {@link #refreshDiscardsStagedEventsAcrossToggleFlicker}.
     * The sole registered listener deregisters between capture and
     * refresh, flipping {@code AuditEvents.isEnabled()} via the
     * {@code registry.hasAnyListener()} factor. A fresh listener
     * re-registers (writing to the same {@code received} collection)
     * before the next commit. Verifies the gate-OFF source doesn't
     * matter — only that the callout always fires.
     */
    @Test
    public void refreshDiscardsStagedEventsAcrossListenerChurn() throws Exception {
        try (ContentSession session = login()) {
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("staged-by-r1"),
                            Map.of("trace.id", "E1-must-not-leak-via-listener-churn")));

            // Flip gate OFF via listener deregistration.
            listenerRegistration.unregister();

            r1.refresh();

            // Re-register a fresh listener writing to the same `received`
            // collection — the test only cares whether the leaked event
            // is observable downstream, not which listener instance sees it.
            listenerRegistration = whiteboard.register(AuditEventListener.class,
                    new AuditEventListener() {
                        @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
                        @Override public void onEvents(@NotNull List<AuditEvent> events) {
                            received.addAll(events);
                        }
                    }, Map.of());

            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("delivered-by-r2"),
                            Map.of("trace.id", "E2-current")));
            r2.getTree("/").setProperty("scratch", "v");
            r2.commit();

            assertEquals("only E2 must be delivered; E1 must NOT survive the listener-churn around refresh",
                    1, received.size());
            assertEquals("E2-current",
                    received.get(0).getPayload().get("trace.id"));
        }
    }

    /**
     * Listener-churn variant for {@code rebase} — INVERTED like
     * {@link #rebasePreservesStagedEventsAcrossToggleFlicker}: rebase
     * preserves E1 across the listener churn; both E1 and E2 are delivered
     * to the re-registered listener on commit.
     */
    @Test
    public void rebasePreservesStagedEventsAcrossListenerChurn() throws Exception {
        try (ContentSession session = login()) {
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("staged-by-r1"),
                            Map.of("trace.id", "E1-survives-rebase-churn")));

            listenerRegistration.unregister();
            r1.rebase();
            listenerRegistration = whiteboard.register(AuditEventListener.class,
                    new AuditEventListener() {
                        @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
                        @Override public void onEvents(@NotNull List<AuditEvent> events) {
                            received.addAll(events);
                        }
                    }, Map.of());

            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("delivered-by-r2"),
                            Map.of("trace.id", "E2-current")));
            r2.getTree("/").setProperty("scratch", "v");
            r2.commit();

            assertEquals("rebase preserves E1 across the listener churn; E1 and E2 both delivered",
                    2, received.size());
            assertEquals("E1-survives-rebase-churn",
                    received.get(0).getPayload().get("trace.id"));
            assertEquals("E2-current",
                    received.get(1).getPayload().get("trace.id"));
        }
    }

    /**
     * Listener-churn variant for commit-failure. Same separate-Oak
     * pattern as {@link #commitFailureDiscardsStagedEventsAcrossToggleFlicker}.
     */
    @Test
    public void commitFailureDiscardsStagedEventsAcrossListenerChurn() throws Exception {
        MemoryNodeStore store2 = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        Closeable observer2 = store2.addObserver(auditConfig.getDrainObserver());
        ContentRepository repo2 = new Oak(store2)
                .with(securityProvider)
                .with(whiteboard)
                .with(new ThrowingValidatorProvider("trigger-failure"))
                .createContentRepository();
        try (ContentSession session = repo2.login(adminCredentials(), null)) {
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("staged-by-failed-r1"),
                            Map.of("trace.id", "E1-must-not-leak-via-listener-churn-failure")));

            listenerRegistration.unregister();
            r1.getTree("/").setProperty("trigger-failure", "boom");
            try {
                r1.commit();
                fail("Expected CommitFailedException from injected validator");
            } catch (CommitFailedException expected) {
                // expected
            }
            listenerRegistration = whiteboard.register(AuditEventListener.class,
                    new AuditEventListener() {
                        @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
                        @Override public void onEvents(@NotNull List<AuditEvent> events) {
                            received.addAll(events);
                        }
                    }, Map.of());

            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("delivered-by-r2"),
                            Map.of("trace.id", "E2-current")));
            r2.getTree("/").setProperty("scratch", "v");
            r2.commit();

            assertEquals("only E2 must be delivered; E1 must NOT survive the listener-churn around commit-failure",
                    1, received.size());
            assertEquals("E2-current",
                    received.get(0).getPayload().get("trace.id"));
        } finally {
            observer2.close();
            if (repo2 instanceof Closeable) {
                ((Closeable) repo2).close();
            }
        }
    }

    //----------------------------------------< toggle, grouping, isolation >---

    /**
     * With the feature toggle disabled, neither pipeline emits to listeners.
     * Pins the {@code if (!featureToggle.isEnabled()) return} early-return
     * in {@code AuditDrainObserver} as well as the toggle gate in
     * {@code BufferSink}.
     */
    @Test
    public void toggleDisabledShortCircuitsEntirePipeline() throws Exception {
        setToggle(false);

        // Fire-and-forget path:
        emitter.emit(eventFor(DOMAIN, AuditType.of("forget"), Map.of()));
        assertTrue("fire-and-forget must short-circuit with toggle disabled",
                received.isEmpty());

        // Commit-attached path:
        try (ContentSession session = login()) {
            Root root = session.getLatestRoot();
            AuditEvents.record(
                    root, eventFor(DOMAIN, AuditType.of("commit.type"), Map.of()));
            root.getTree("/").setProperty("scratch", "v");
            root.commit();
            assertTrue("commit-attached must short-circuit with toggle disabled",
                    received.isEmpty());
        }
    }

    /**
     * Three events recorded on two domains: listener-A (DOMAIN) receives the
     * two for its domain in capture order; listener-B (OTHER_DOMAIN) receives
     * only its one. Pins {@code groupByDomain} fan-out.
     */
    @Test
    public void multipleEventsAcrossDomainsGroupedCorrectly() throws Exception {
        List<AuditEvent> otherReceived = new CopyOnWriteArrayList<>();
        AuditEventListener otherListener = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return OTHER_DOMAIN; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                otherReceived.addAll(events);
            }
        };
        Registration otherReg = whiteboard.register(AuditEventListener.class,
                otherListener, Map.of());
        try (ContentSession session = login()) {
            Root root = session.getLatestRoot();
            AuditEvents.record(
                    root, eventFor(DOMAIN, AuditType.of("a-1"), Map.of()));
            AuditEvents.record(
                    root, eventFor(OTHER_DOMAIN, AuditType.of("b-1"), Map.of()));
            AuditEvents.record(
                    root, eventFor(DOMAIN, AuditType.of("a-2"), Map.of()));
            root.getTree("/").setProperty("scratch", "v");
            root.commit();

            assertEquals("DOMAIN listener receives 2 events in capture order",
                    2, received.size());
            assertEquals("a-1", received.get(0).getType().name());
            assertEquals("a-2", received.get(1).getType().name());

            assertEquals("OTHER_DOMAIN listener receives 1 event",
                    1, otherReceived.size());
            assertEquals("b-1", otherReceived.get(0).getType().name());
        } finally {
            otherReg.unregister();
        }
    }

    /**
     * After a successful commit, the per-thread {@link AuditBuffer}'s slot
     * for the session must be drained. Asserted behaviorally via a
     * second commit on the SAME session — if drain didn't run after the
     * first commit, the second snapshot would re-include E1 and we'd see
     * three deliveries total (E1 dispatched by commit#1, then E1+E2
     * re-dispatched by commit#2) instead of two.
     */
    @Test
    public void bufferDrainedAfterSuccessfulCommit() throws Exception {
        try (ContentSession session = login()) {
            // Commit #1: record E1, commit.
            Root r1 = session.getLatestRoot();
            AuditEvents.record(
                    r1, eventFor(DOMAIN, AuditType.of("e1"), Map.of("trace.id", "E1")));
            r1.getTree("/").setProperty("scratch1", "v");
            r1.commit();

            // Commit #2 on the same session: record E2, commit.
            Root r2 = session.getLatestRoot();
            AuditEvents.record(
                    r2, eventFor(DOMAIN, AuditType.of("e2"), Map.of("trace.id", "E2")));
            r2.getTree("/").setProperty("scratch2", "v");
            r2.commit();

            // Exactly two deliveries — E1 first, then E2. If drain were
            // broken after commit#1, we'd see [E1, E1, E2] = 3 events.
            assertEquals("buffer must be drained between commits", 2, received.size());
            assertEquals("first received is E1", "e1", received.get(0).getType().name());
            assertEquals("second received is E2", "e2", received.get(1).getType().name());
            // The marker is the regression-guard: a re-dispatch would
            // duplicate "E1" at position 1, not produce a fresh "E2".
            assertNotEquals("position 1 must not be a stale E1",
                    "E1", received.get(1).getPayload().get("trace.id"));
        }
    }

    /**
     * Listener that throws {@code RuntimeException} from {@code onEvents}
     * must not prevent other listeners on the same domain from receiving
     * the event. Pins per-listener isolation in
     * {@code AuditDrainObserver.dispatchOne} (commit-attached).
     */
    @Test
    public void listenerRuntimeExceptionDoesNotPreventOtherListeners() throws Exception {
        List<AuditEvent> bReceived = new CopyOnWriteArrayList<>();
        AuditEventListener throwingA = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
            @Override public int getRank() { return 10; } // dispatched first
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                throw new RuntimeException("synthetic-A");
            }
        };
        AuditEventListener okB = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
            @Override public int getRank() { return 5; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                bReceived.addAll(events);
            }
        };
        Registration regA = whiteboard.register(AuditEventListener.class, throwingA, Map.of());
        Registration regB = whiteboard.register(AuditEventListener.class, okB, Map.of());
        try (ContentSession session = login()) {
            Root root = session.getLatestRoot();
            AuditEvents.record(
                    root, eventFor(DOMAIN, AuditType.of("x"), Map.of()));
            root.getTree("/").setProperty("scratch", "v");
            root.commit();

            assertEquals("listener-B must receive despite listener-A throwing",
                    1, bReceived.size());
        } finally {
            regA.unregister();
            regB.unregister();
        }
    }

    /**
     * Listener that throws {@code NoClassDefFoundError} (an
     * {@link Error}, not an {@link Exception}) from {@code onEvents}
     * must not prevent other listeners from receiving the event. Pins
     * the catch-{@code Throwable} contract: any {@link Throwable}
     * subtype out of {@code onEvents} is isolated to the misbehaving
     * listener, never escaping into the dispatch loop or the surrounding
     * commit.
     */
    @Test
    public void listenerNoClassDefFoundErrorIsIsolated() throws Exception {
        List<AuditEvent> bReceived = new CopyOnWriteArrayList<>();
        AuditEventListener throwingA = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
            @Override public int getRank() { return 10; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                throw new NoClassDefFoundError("synthetic-A");
            }
        };
        AuditEventListener okB = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
            @Override public int getRank() { return 5; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                bReceived.addAll(events);
            }
        };
        Registration regA = whiteboard.register(AuditEventListener.class, throwingA, Map.of());
        Registration regB = whiteboard.register(AuditEventListener.class, okB, Map.of());
        try (ContentSession session = login()) {
            Root root = session.getLatestRoot();
            AuditEvents.record(
                    root, eventFor(DOMAIN, AuditType.of("x"), Map.of()));
            root.getTree("/").setProperty("scratch", "v");
            root.commit();

            assertEquals("listener-B must receive despite listener-A throwing NoClassDefFoundError",
                    1, bReceived.size());
        } finally {
            regA.unregister();
            regB.unregister();
        }
    }

    /**
     * Fire-and-forget variant of the runtime-exception isolation test.
     * Pins the same property in {@code BufferSink.dispatch}.
     */
    @Test
    public void fireAndForgetListenerRuntimeExceptionDoesNotPreventOthers() {
        List<AuditEvent> bReceived = new CopyOnWriteArrayList<>();
        AuditEventListener throwingA = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
            @Override public int getRank() { return 10; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                throw new RuntimeException("synthetic-A");
            }
        };
        AuditEventListener okB = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
            @Override public int getRank() { return 5; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                bReceived.addAll(events);
            }
        };
        Registration regA = whiteboard.register(AuditEventListener.class, throwingA, Map.of());
        Registration regB = whiteboard.register(AuditEventListener.class, okB, Map.of());
        try {
            emitter.emit(eventFor(DOMAIN, AuditType.of("x"), Map.of()));
            assertEquals("fire-and-forget: listener-B must receive despite A's RuntimeException",
                    1, bReceived.size());
        } finally {
            regA.unregister();
            regB.unregister();
        }
    }

    /**
     * Fire-and-forget variant of the Error isolation test.
     */
    @Test
    public void fireAndForgetListenerNoClassDefFoundErrorIsIsolated() {
        List<AuditEvent> bReceived = new CopyOnWriteArrayList<>();
        AuditEventListener throwingA = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
            @Override public int getRank() { return 10; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                throw new NoClassDefFoundError("synthetic-A");
            }
        };
        AuditEventListener okB = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() { return DOMAIN; }
            @Override public int getRank() { return 5; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                bReceived.addAll(events);
            }
        };
        Registration regA = whiteboard.register(AuditEventListener.class, throwingA, Map.of());
        Registration regB = whiteboard.register(AuditEventListener.class, okB, Map.of());
        try {
            emitter.emit(eventFor(DOMAIN, AuditType.of("x"), Map.of()));
            assertEquals("fire-and-forget: listener-B must receive despite A's NoClassDefFoundError",
                    1, bReceived.size());
        } finally {
            regA.unregister();
            regB.unregister();
        }
    }

    /**
     * Accessor variant of fire-and-forget isolation, with the published
     * {@link AuditEventEmitter#emit} contract at stake: listener failures
     * "never propagate back to the caller" — and {@code getDomain()} is
     * listener code just like {@code onEvents()}. A listener whose
     * {@code getDomain()} throws {@link LinkageError} (broken consumer-bundle
     * classpath) must neither escape {@code emit()} into the calling write
     * operation nor prevent the healthy listener from receiving the event.
     */
    @Test
    public void fireAndForgetBrokenGetDomainListenerDoesNotThrowToEmitter() {
        AuditEventListener brokenDomain = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() {
                throw new LinkageError("synthetic-getDomain");
            }
            @Override public int getRank() { return 10; } // consulted first
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                fail("a listener with a broken getDomain() must never receive events");
            }
        };
        Registration reg = whiteboard.register(AuditEventListener.class, brokenDomain, Map.of());
        try {
            emitter.emit(eventFor(DOMAIN, AuditType.of("x"), Map.of("key", "v")));
            assertEquals("healthy listener must receive despite peer's broken getDomain()",
                    1, received.size());
        } finally {
            reg.unregister();
        }
    }

    /**
     * Capture-gate variant: {@link AuditEventEmitter#isEnabledFor} — the
     * probe capture sites consult BEFORE staging an event — must tolerate a
     * broken listener too. Without the registry-level guard, the throwing
     * {@code getDomain()} escapes through {@code BufferSink.isEnabledFor}
     * into the capture site and fails the user-facing write operation.
     */
    @Test
    public void captureGateIsEnabledForToleratesBrokenListener() {
        AuditEventListener brokenDomain = new AuditEventListener() {
            @Override public @NotNull AuditDomain getDomain() {
                throw new LinkageError("synthetic-getDomain");
            }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                fail("a listener with a broken getDomain() must never receive events");
            }
        };
        Registration reg = whiteboard.register(AuditEventListener.class, brokenDomain, Map.of());
        try {
            // Unserved domain first — the full registry scan must consult
            // (and skip) the broken listener, never propagate its throw.
            assertFalse("gate must return false (not throw) for an unserved domain",
                    emitter.isEnabledFor(AuditDomain.of("no.such.domain")));
            assertTrue("gate must find the healthy fixture listener despite the broken peer",
                    emitter.isEnabledFor(DOMAIN));
        } finally {
            reg.unregister();
        }
    }

    //------------------------< masquerade-prevention (sage invariant I8) >---

    /**
     * End-to-end verification of the design rule that audit never
     * masquerades as a commit failure to the merge caller.
     * <p>
     * A poisoned {@link AuditEvent} whose {@code getDomain()} throws on the
     * SECOND call (i.e. at drain time, after capture-time
     * {@code BufferSink.record} successfully consulted it) drives the
     * {@link AuditDrainObserver} into its outer {@code catch (Throwable)}
     * barrier. The barrier swallows the throw and logs WARN; the merge
     * thread sees no exception and {@link Root#commit()} returns normally.
     * <p>
     * Without the outer barrier, this throw would propagate out of
     * {@code AuditDrainObserver.contentChanged} → through
     * {@code CompositeObserver.contentChanged} (no per-observer isolation
     * at {@code CompositeObserver.java:46-53}) → into the NodeStore impl's
     * post-merge observer dispatch → surfacing as a RuntimeException to
     * the merge caller despite the durable commit having succeeded. On
     * DocumentNodeStore, the inner catch at
     * {@code DocumentNodeStore.java:1130-1139} would even suppress
     * unrelated commit-apply failures.
     */
    @Test
    public void poisonedEventDoesNotMaskCommitAsFailure() throws Exception {
        LogCustomizer log = LogCustomizer.forLogger(AuditDrainObserver.class)
                .enable(Level.WARN).create();
        log.starting();
        try {
            // Counter-based poison: getDomain() returns DOMAIN on the FIRST
            // call (BufferSink.record's isEnabledFor probe — must succeed so
            // the event enters the buffer) and throws on all subsequent calls
            // (groupByDomain at drain time). Models a producer-side bug that
            // surfaces only at dispatch.
            AtomicInteger calls = new AtomicInteger();
            AuditEvent counterPoison = new AuditEvent() {
                @Override public @NotNull AuditDomain getDomain() {
                    if (calls.incrementAndGet() <= 1) {
                        return DOMAIN;
                    }
                    throw new RuntimeException("synthetic-drain-time-poison");
                }
                @Override public @NotNull AuditType getType() { return AuditType.of("poison.type"); }
                @Override public long getTimestamp() { return 0L; }
                @Override public @NotNull Map<String, Object> getPayload() {
                    return Map.of();
                }
            };

            try (ContentSession session = login()) {
                Root root = session.getLatestRoot();
                AuditEvents.record(root, counterPoison);
                root.getTree("/").setProperty("scratch-masquerade", "v");

                // The CORE assertion — Root.commit() MUST return normally.
                // A regression that removed the outer barrier would surface
                // the drain-time throw here as a CommitFailedException.
                root.commit();

                // Listener never received the poisoned event — groupByDomain
                // threw before dispatch.
                assertTrue("listener must not see the poisoned event",
                        received.isEmpty());

                // WARN log fired exactly once with the session id for
                // diagnostics. If this WARN ever fires in CI on a non-test
                // path, treat as a bug — the barrier is a safety net for
                // producer-side bugs, not a steady-state code path.
                List<String> logs = log.getLogs();
                assertEquals("outer Throwable barrier must log exactly one WARN line",
                        1, logs.size());
                assertTrue("WARN must include session id; was: " + logs.get(0),
                        logs.get(0).contains(session.toString()));
            }
        } finally {
            log.finished();
        }
    }

    //---------------------------------------------< migration-path no-op >---

    /**
     * Migration commits — the path {@code RepositoryUpgrade.java:549} and
     * {@code :569} use — drive {@code NodeStore.merge(...)} directly with
     * {@link CommitInfo#EMPTY}, bypassing {@code MutableRoot}. None of the
     * capture sites ({@code UserManagerImpl.recordSingleMembershipAuditEvent},
     * fire-and-forget {@link AuditEvents#dispatch}) are reached by such
     * commits, so the per-session buffer remains empty for the migration's
     * synthetic {@code CommitInfo.OAK_UNKNOWN} session id.
     * <p>
     * The drain observer is invoked by the NodeStore (the migration commit
     * succeeds) but its short-circuit at
     * {@code AuditDrainObserver.doContentChanged} — {@code buffer.drain(sessionId)}
     * returns {@code null}, drives no dispatch. Pins the migration-path
     * no-op behaviour.
     */
    @Test
    public void directMergeWithEmptyCommitInfoProducesNoListenerCalls() throws Exception {
        // Fresh MemoryNodeStore — the fixture's `store` is already wired
        // to the singleton drain observer for the @Before-driven Oak.
        // Using a separate Observable here keeps the test isolated and
        // makes the migration semantic explicit: this store is being
        // driven without MutableRoot in the picture.
        MemoryNodeStore migrationStore = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        Closeable observerHandle = migrationStore.addObserver(auditConfig.getDrainObserver());
        try {
            // Drive a direct merge — the RepositoryUpgrade pattern.
            NodeBuilder builder = migrationStore.getRoot().builder();
            builder.setProperty("migration.marker", "test-value");
            migrationStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            assertTrue("migration commit (CommitInfo.EMPTY) must produce no listener invocations",
                    received.isEmpty());
        } finally {
            observerHandle.close();
        }
    }

    //--------------------------------------------------< validator support >---

    /**
     * {@link ValidatorProvider} that injects a {@link Validator} which
     * fails the commit when it observes a specific marker property added
     * to the root. Used by {@link #commitFailureDiscardsStagedEvents()}
     * to force a deterministic commit failure.
     */
    private static final class ThrowingValidatorProvider extends ValidatorProvider {

        private final String triggerPropertyName;

        ThrowingValidatorProvider(@NotNull String triggerPropertyName) {
            this.triggerPropertyName = triggerPropertyName;
        }

        @NotNull
        @Override
        public Validator getRootValidator(NodeState before, NodeState after,
                                          CommitInfo info) {
            return new ThrowingValidator(triggerPropertyName);
        }
    }

    private static final class ThrowingValidator extends DefaultValidator {

        private final String triggerPropertyName;

        ThrowingValidator(@NotNull String triggerPropertyName) {
            this.triggerPropertyName = triggerPropertyName;
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            if (triggerPropertyName.equals(after.getName())) {
                throw new CommitFailedException(CommitFailedException.CONSTRAINT, 1,
                        "Injected validator failure: " + triggerPropertyName);
            }
        }
    }
}
