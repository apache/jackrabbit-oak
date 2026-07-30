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

import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditEventListener;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Behavioural coverage for {@link AuditConfigurationImpl} — both the
 * {@link AuditConfigurationImpl#isActive() isActive()} reporter and the
 * lifecycle / drain-observer accessor surface.
 * <p>
 * Two test layers:
 * <ul>
 *   <li><strong>{@code isActive()} state machine</strong> — 5 states
 *       reachable from the wiring lifecycle:
 *       <ol>
 *         <li>Not initialised — default NOOP sink → {@code false}.</li>
 *         <li>Initialised, toggle OFF — short-circuits on toggle.</li>
 *         <li>Initialised, toggle ON, no listener — short-circuits on
 *             {@code registry.hasAnyListener()}.</li>
 *         <li>Initialised, toggle ON, listener registered → {@code true}.</li>
 *         <li>After {@code dispose()} — sink reset to NOOP → {@code false}.</li>
 *       </ol></li>
 *   <li><strong>Lifecycle + drain-observer accessor</strong> — 7 cases (a)–(g):
 *       <ol>
 *         <li>(a) {@code @Activate} registers the {@link Observer} service.</li>
 *         <li>(b) {@code @Deactivate} unregisters it.</li>
 *         <li>(c) {@code getDrainObserver()} returns a non-null Observer
 *             post-{@code initialize}.</li>
 *         <li>(d) {@code getDrainObserver()} returns the SAME instance on
 *             repeat calls — singleton invariant. Guards against accidental
 *             factory revert: the {@code AuditBuffer} {@code ThreadLocal} is
 *             buffer-instance-scoped and the {@code drain(sessionId)}
 *             contract is destructive, so two observers sharing the same
 *             buffer would silently turn double-attach into double-dispatch
 *             under any future non-destructive drain refactor.</li>
 *         <li>(e) {@code getDrainObserver()} throws
 *             {@link IllegalStateException} pre-{@code initialize}.</li>
 *         <li>(f) {@code getDrainObserver()} throws ISE post-{@code dispose}.</li>
 *         <li>(g) {@code dispose()} throws ISE when {@code observerRegistration}
 *             is still non-null — defense-in-depth precondition guard.
 *             The @{@code Deactivate} path zeros the field
 *             before invoking {@code dispose()}; misuse paths (a test
 *             that calls {@code dispose()} directly without invoking
 *             {@code @Deactivate}) get a loud failure with an actionable
 *             message rather than a silent leak.</li>
 *       </ol></li>
 *   <li><strong>Tear-down ordering</strong> — Mockito {@link InOrder}
 *       verifies that {@code @Deactivate} runs
 *       {@code observerRegistration.unregister() → featureToggle.close()
 *       → registry.stop() → buffer.clearAll()} in that exact order. The
 *       "detach first, internals second" policy mirrors
 *       {@code ChangeProcessor.java:289-295}; the precondition guard in
 *       {@code dispose()} catches the misuse case separately (case (g)).</li>
 * </ul>
 * MockOsgi (org.apache.sling.testing.osgi-mock.junit4) drives the
 * {@code @Activate}/{@code @Deactivate} flow for cases (a)/(b). Unit-level
 * cases (c)–(g) call {@code initialize}/{@code dispose} directly. The
 * InOrder test injects spies via the package-private fields exposed for
 * test access (production callers MUST NOT touch those fields).
 */
public class AuditConfigurationImplTest {

    @Rule
    public final OsgiContext osgiContext = new OsgiContext();

    private Whiteboard whiteboard;
    private AuditConfigurationImpl config;

    @Before
    public void setUp() {
        whiteboard = new DefaultWhiteboard();
        config = new AuditConfigurationImpl();
    }

    @After
    public void tearDown() {
        // Always dispose to reset the static AuditEvents.sink to NOOP — keeps
        // tests isolated from each other even though they share the static
        // façade. Safe to call even if initialize() was never invoked
        // (each step in dispose() guards against null state).
        //
        // Special-case the (g) misuse test: dispose() throws if
        // observerRegistration is still non-null. The test's local
        // AuditConfigurationImpl instance is a different object — the
        // @Before-created config is untouched and disposes cleanly.
        config.dispose();
    }

    @Test
    public void isActiveReturnsFalseWhenNotInitialized() {
        // No initialize() call — the default NOOP sink reports isEnabled() == false.
        assertFalse("uninitialised pipeline must report inactive", config.isActive());
    }

    @Test
    public void isActiveReturnsFalseWhenToggleOff() {
        config.initialize(whiteboard);
        // Toggle defaults to disabled (FT_OAK-12331 is OFF by default per AGENTS.md).
        // BufferSink.isEnabled() short-circuits on toggle.isEnabled().
        assertFalse("toggle OFF must report inactive", config.isActive());
    }

    @Test
    public void isActiveReturnsFalseWhenToggleOnButNoListener() {
        config.initialize(whiteboard);
        setToggle(true);
        // Toggle ON but no AuditEventListener registered yet —
        // BufferSink.isEnabled() short-circuits on registry.hasAnyListener().
        assertFalse("toggle ON without listener must report inactive", config.isActive());
    }

    @Test
    public void isActiveReturnsTrueWhenToggleOnAndListenerRegistered() {
        config.initialize(whiteboard);
        setToggle(true);
        registerTestListener();
        // Both AND clauses satisfied — pipeline is active.
        assertTrue("toggle ON + listener registered must report active", config.isActive());
    }

    @Test
    public void isActiveReturnsFalseAfterDispose() {
        // First bring the pipeline up so it's known-active...
        config.initialize(whiteboard);
        setToggle(true);
        registerTestListener();
        assertTrue("precondition: pipeline must be active before dispose", config.isActive());

        // ...then dispose, which resets the static sink to NOOP.
        config.dispose();
        assertFalse("disposed pipeline must report inactive", config.isActive());
    }

    //----------------------------------< lifecycle + drain-observer accessor >---

    /**
     * Case (a) — {@code @Activate} registers an {@link Observer} service
     * via {@code BundleContext.registerService(Observer.class, ...)}.
     * After activation the {@code Observer} service is discoverable through
     * the {@code BundleContext}. {@code ObserverTracker} (in production,
     * instantiated per-NodeStoreService) is what then subscribes it to the
     * root NodeStore; we don't run {@code ObserverTracker} here — verifying
     * the service registration suffices for this case.
     */
    @Test
    public void activateRegistersObserverService() {
        AuditConfigurationImpl audit = osgiContext.registerInjectActivateService(
                new AuditConfigurationImpl());
        try {
            Observer registered = osgiContext.getService(Observer.class);
            assertNotNull("@Activate must register an Observer service", registered);
            // The service IS the singleton drain observer.
            assertSame("registered Observer must be the singleton drain observer",
                    audit.getDrainObserver(), registered);
        } finally {
            MockOsgi.deactivate(audit, osgiContext.bundleContext());
        }
    }

    /**
     * Case (b) — {@code @Deactivate} unregisters the {@link Observer}
     * service. After deactivation the service is no longer discoverable
     * through the {@code BundleContext}.
     */
    @Test
    public void deactivateUnregistersObserverService() {
        AuditConfigurationImpl audit = osgiContext.registerInjectActivateService(
                new AuditConfigurationImpl());
        assertNotNull("precondition: Observer service must be registered",
                osgiContext.getService(Observer.class));

        MockOsgi.deactivate(audit, osgiContext.bundleContext());

        assertNull("@Deactivate must unregister the Observer service",
                osgiContext.getService(Observer.class));
    }

    /**
     * Case (c) — {@link AuditConfigurationImpl#getDrainObserver()} returns
     * a non-null {@link Observer} after {@code initialize(...)} ran.
     */
    @Test
    public void getDrainObserverReturnsObserverAfterInitialize() {
        AuditConfigurationImpl audit = new AuditConfigurationImpl();
        try {
            audit.initialize(whiteboard);
            Observer observer = audit.getDrainObserver();
            assertNotNull("getDrainObserver() must return non-null post-initialize",
                    observer);
        } finally {
            audit.dispose();
        }
    }

    /**
     * Case (d) — {@link AuditConfigurationImpl#getDrainObserver()} returns
     * the SAME instance on repeat calls (singleton invariant). Regression
     * guard against accidental factory revert: the
     * {@link AuditBuffer#drain(String)} contract is destructive, so two
     * Observer instances sharing the same buffer would silently turn
     * double-attach into double-dispatch under any future non-destructive
     * drain refactor.
     */
    @Test
    public void getDrainObserverReturnsSameInstanceOnRepeatCalls() {
        AuditConfigurationImpl audit = new AuditConfigurationImpl();
        try {
            audit.initialize(whiteboard);
            Observer first = audit.getDrainObserver();
            Observer second = audit.getDrainObserver();
            Observer third = audit.getDrainObserver();
            assertSame("getDrainObserver() must return the same singleton on repeat calls",
                    first, second);
            assertSame("getDrainObserver() must be stable across multiple calls",
                    first, third);
        } finally {
            audit.dispose();
        }
    }

    /**
     * Case (e) — {@link AuditConfigurationImpl#getDrainObserver()} throws
     * {@link IllegalStateException} when called before
     * {@link AuditConfigurationImpl#initialize(Whiteboard)}.
     */
    @Test
    public void getDrainObserverThrowsBeforeInitialize() {
        AuditConfigurationImpl audit = new AuditConfigurationImpl();
        try {
            audit.getDrainObserver();
            fail("getDrainObserver() must throw IllegalStateException pre-initialize");
        } catch (IllegalStateException expected) {
            // Pinned: message must include actionable hint about initialize().
            assertTrue("ISE message must mention initialize() so the misuse is actionable; was: "
                            + expected.getMessage(),
                    expected.getMessage().contains("initialize"));
        }
    }

    /**
     * Case (f) — {@link AuditConfigurationImpl#getDrainObserver()} throws
     * {@link IllegalStateException} when called after
     * {@link AuditConfigurationImpl#dispose()}. The singleton field is
     * zeroed by {@code dispose()}'s step 5, so the same null-check that
     * pins case (e) covers this state too.
     */
    @Test
    public void getDrainObserverThrowsAfterDispose() {
        AuditConfigurationImpl audit = new AuditConfigurationImpl();
        audit.initialize(whiteboard);
        // Confirm precondition — pre-dispose call must succeed.
        assertNotNull(audit.getDrainObserver());
        audit.dispose();
        try {
            audit.getDrainObserver();
            fail("getDrainObserver() must throw IllegalStateException post-dispose");
        } catch (IllegalStateException expected) {
            // ISE shape identical to the pre-init case — same field-null check,
            // same message. That symmetry is the contract.
            assertTrue("ISE message must mention initialize() (same shape as pre-init); was: "
                            + expected.getMessage(),
                    expected.getMessage().contains("initialize"));
        }
    }

    /**
     * Case (g) — {@link AuditConfigurationImpl#dispose()} throws
     * {@link IllegalStateException} when called with
     * {@code observerRegistration} still non-null. Defense-in-depth
     * precondition guard:
     * <ul>
     *   <li>OSGi {@code @Deactivate} unregisters and zeros
     *       {@code observerRegistration} BEFORE invoking {@code dispose()} —
     *       precondition trivially satisfied.</li>
     *   <li>Embedded callers never set {@code observerRegistration}
     *       (no {@code @Activate} path) — precondition trivially satisfied.</li>
     *   <li>Misuse case (e.g. test calls {@code dispose()} directly after
     *       {@code @Activate} without going through {@code @Deactivate}):
     *       caught here, ISE with actionable message.</li>
     * </ul>
     */
    @Test
    public void disposeThrowsIfObserverRegistrationStillSet() {
        AuditConfigurationImpl audit = new AuditConfigurationImpl();
        audit.initialize(whiteboard);
        // Simulate the OSGi misuse path: observerRegistration was set by
        // @Activate but @Deactivate's unregister step never ran. The field
        // is package-private specifically so this test can set it directly
        // without invoking the full @Activate / OsgiContext flow.
        audit.observerRegistration = Mockito.mock(ServiceRegistration.class);
        try {
            audit.dispose();
            fail("dispose() must throw IllegalStateException when observerRegistration is still set");
        } catch (IllegalStateException expected) {
            assertTrue("ISE message must mention 'unregister' so the misuse is actionable; was: "
                            + expected.getMessage(),
                    expected.getMessage().contains("unregister"));
        } finally {
            // Cleanup — clear the registration field so the next dispose() (in
            // any subsequent state) passes the precondition. Don't dispose
            // here; the misuse-case path already left fields half-initialized
            // and another dispose call would compound the test's leak. Letting
            // the AuditConfigurationImpl instance go out of scope is enough —
            // the static AuditEvents/AuditBufferLifecycle sinks need explicit
            // cleanup though.
            audit.observerRegistration = null;
            audit.dispose();
        }
    }

    //--------------------------------------< InOrder dispose sequence pin >---

    /**
     * Pins the "detach first, internals second" sequence in
     * {@code @Deactivate}: {@code observerRegistration.unregister() →
     * featureToggle.close() → registry.stop() → buffer.clearAll()}.
     * Verifies the exact call order via Mockito {@link InOrder}.
     * <p>
     * Method-level analogue of {@code ChangeProcessor.java:289-295}'s
     * teardown precedent ({@code filteringObserver.close()} then
     * {@code executor.stop()}).
     */
    @Test
    public void deactivateRunsTearDownStepsInOrder() {
        AuditConfigurationImpl audit = new AuditConfigurationImpl();
        audit.initialize(whiteboard);

        // Replace internal collaborators with spies/mocks so InOrder can
        // verify the exact sequence of calls. The package-private fields
        // make this clean — no reflection needed.
        Feature toggleSpy = Mockito.spy(audit.featureToggle);
        AuditBuffer bufferSpy = Mockito.spy(audit.buffer);
        WhiteboardAuditEventListenerRegistry registrySpy = Mockito.spy(audit.registry);
        ServiceRegistration<?> regMock = Mockito.mock(ServiceRegistration.class);

        audit.featureToggle = toggleSpy;
        audit.buffer = bufferSpy;
        audit.registry = registrySpy;
        audit.observerRegistration = regMock;

        InOrder inOrder = Mockito.inOrder(regMock, toggleSpy, registrySpy, bufferSpy);
        audit.deactivate();

        // Pinned sequence: detach the Observer service FIRST so
        // ObserverTracker closes its subscription on the root NodeStore
        // before we tear down the internals it points at.
        inOrder.verify(regMock).unregister();
        inOrder.verify(toggleSpy).close();
        inOrder.verify(registrySpy).stop();
        inOrder.verify(bufferSpy).clearAll();

        // No further interactions on the ServiceRegistration mock — we don't
        // expect deactivate to touch it again.
        Mockito.verifyNoMoreInteractions(regMock);
    }

    //----------------------------------------------------------< fixtures >---

    /**
     * Flips the FT_OAK-12331 feature toggle by locating the {@link FeatureToggle}
     * service that {@link AuditConfigurationImpl#initialize(Whiteboard)
     * initialize} registered on the whiteboard.
     */
    private void setToggle(boolean enabled) {
        Tracker<FeatureToggle> tracker = whiteboard.track(FeatureToggle.class);
        try {
            for (FeatureToggle ft : tracker.getServices()) {
                if (AuditConfigurationImpl.FEATURE_TOGGLE_NAME.equals(ft.getName())) {
                    ft.setEnabled(enabled);
                }
            }
        } finally {
            tracker.stop();
        }
    }

    private void registerTestListener() {
        AuditEventListener listener = new AuditEventListener() {
            @NotNull
            @Override
            public String getDomain() {
                return "test.isActive.coverage";
            }

            @Override
            public void onEvents(@NotNull List<AuditEvent> events) {
                // not exercised — isActive() only needs the listener to be
                // registered, not invoked.
            }
        };
        whiteboard.register(AuditEventListener.class, listener, Map.of());
    }
}
