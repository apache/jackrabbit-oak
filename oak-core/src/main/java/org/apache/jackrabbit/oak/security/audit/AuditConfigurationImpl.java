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
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.audit.AuditBufferLifecycle;
import org.apache.jackrabbit.oak.spi.audit.AuditConfiguration;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditEventListener;
import org.apache.jackrabbit.oak.spi.audit.AuditEvents;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link AuditConfiguration} implementation. Contributes the
 * audit pipeline to Oak:
 * <ul>
 *     <li>Registers a {@link Feature} toggle gating capture and dispatch.</li>
 *     <li>Installs a per-session buffer ({@link AuditBuffer}) into
 *     {@link AuditBufferLifecycle}.</li>
 *     <li>Installs the {@link AuditEvents} sink that routes capture-site
 *     calls into the buffer.</li>
 *     <li>Tracks {@code AuditEventListener} services on the Whiteboard
 *     via {@link WhiteboardAuditEventListenerRegistry}.</li>
 *     <li>Registers an {@link AuditDrainObserver} as an OSGi {@link Observer}
 *     service; Oak's {@code ObserverTracker} (in {@code oak-jcr}'s
 *     {@code RepositoryManager}) picks it up and subscribes it to the root
 *     NodeStore. The observer drains the buffer on commit success and
 *     dispatches events to listeners.</li>
 * </ul>
 * Registered as {@link AuditConfiguration} only — not a
 * {@code SecurityConfiguration}, contributes no commit hooks, not reachable
 * via {@code SecurityProvider.getConfiguration(AuditConfiguration.class)}.
 * Embedded callers obtain the drain observer via {@link #getDrainObserver()}.
 * <p>
 * When the feature toggle is disabled, capture is a no-op and the observer
 * short-circuits — see {@link AuditEvents#isEnabled()} and
 * {@link AuditDrainObserver#contentChanged}.
 */
@Component(service = AuditConfiguration.class)
@Designate(ocd = AuditConfigurationImpl.Configuration.class)
public class AuditConfigurationImpl implements AuditConfiguration {

    /**
     * Feature toggle name, following the {@code FT_OAK-<issue>} convention
     * in {@code AGENTS.md}. Disabled by default: this is a new feature,
     * not a bug fix.
     * <p>
     * <strong>Why not on the public SPI interface
     * ({@link AuditConfiguration}):</strong> moving this constant to the
     * SPI would commit the literal value to the public surface forever.
     * It stays impl-local; promoting it later is a binary-additive change
     * if a need arises.
     */
    public static final String FEATURE_TOGGLE_NAME = "FT_OAK-12331";

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak AuditConfiguration",
            description = "Audit event pipeline. Capture and dispatch are " +
                    "gated by the '" + FEATURE_TOGGLE_NAME + "' feature toggle " +
                    "(disabled by default).")
    @interface Configuration {
        // Configuration is currently empty by design: capture/dispatch behavior
        // is controlled exclusively by the feature toggle. Listeners are
        // contributed via OSGi services / the Whiteboard.
    }

    private static final Logger log = LoggerFactory.getLogger(AuditConfigurationImpl.class);

    // Package-private (not private) on the internal-state fields so
    // AuditConfigurationImplTest can mock-replace them to verify the
    // dispose-order invariant via Mockito InOrder. Production callers MUST
    // NOT touch these fields directly — go through initialize() / dispose().
    //
    // JMM-safety. featureToggle, buffer, registry, and drainObserver are
    // package-private and non-volatile by design. The invariant they rely on:
    // they are mutated only by initialize(Whiteboard) and dispose(), which
    // the contract specifies must each run exactly once and on the same
    // thread; reads happen either
    //   (a) on that same thread — the static AuditEvents.record / dispatch
    //       facade reads through the AuditEvents.sink field (itself volatile,
    //       providing the publication barrier), and getDrainObserver() is
    //       called by activate() on the SCR thread AFTER initialize() on the
    //       SCR thread; or
    //   (b) on a commit thread that arrives via Observer.contentChanged,
    //       after the ServiceRegistration publication barrier established
    //       by BundleContext.registerService in activate().
    // Both paths satisfy the JMM happens-before contract without per-field
    // volatile. If a future change adds a "share the singleton across
    // pipelines" pattern OR cross-thread mutation of these fields, this
    // invariant breaks — at that point the fields MUST be made volatile (or
    // properly immutable via constructor injection).
    Feature featureToggle;
    AuditBuffer buffer;
    WhiteboardAuditEventListenerRegistry registry;

    /**
     * Singleton {@link AuditDrainObserver} instance constructed by
     * {@link #initialize(Whiteboard)} and zeroed by {@link #dispose()}.
     * Exposed via {@link #getDrainObserver()} as an {@link Observer}.
     * <p>
     * Singleton-not-factory by design: each {@code AuditConfigurationImpl}
     * owns at most one Observer because (a) the {@link AuditBuffer}
     * {@code ThreadLocal} is buffer-instance-scoped, so multiple Observer
     * instances would compete for the same drain, and (b) the destructive
     * {@code buffer.drain(sessionId)} contract is the cleanup mechanism —
     * double-attach would mask future non-destructive-drain refactors.
     */
    AuditDrainObserver drainObserver;

    /**
     * OSGi service registration for the {@link AuditDrainObserver}. Held
     * so {@link #deactivate} can unregister and let {@code ObserverTracker}
     * close its subscription on the root NodeStore. {@code null} outside
     * the OSGi-active window; embedded callers manage observer lifetime
     * through their own {@code ((Observable) store).addObserver(...)} call
     * (see class Javadoc).
     */
    ServiceRegistration<?> observerRegistration;

    public AuditConfigurationImpl() {
        super();
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(@NotNull Configuration configuration,
                          @NotNull BundleContext bundleContext,
                          @NotNull Map<String, Object> properties) {
        // Step 1-4: install sinks/registry/buffer/toggle. Capture-site
        // record(...) calls reach the buffer as soon as initialize returns.
        // The singleton AuditDrainObserver is also constructed inside
        // initialize() (step 5 below) so the impl is fully wired before
        // we publish anything externally.
        initialize(new OsgiWhiteboard(bundleContext));
        // Step 6 LAST: publish the Observer service. ObserverTracker
        // (oak-store-spi/.../spi/commit/ObserverTracker.java, instantiated
        // per-NodeStoreService in DocumentNodeStoreService, SegmentNodeStoreRegistrar,
        // CompositeNodeStoreService) subscribes it to the root NodeStore.
        // Any commit thread racing with activation that reaches step 6 before
        // ObserverTracker has noticed the service will simply miss the drain
        // on this one commit — events stay in the per-thread buffer until the
        // next commit on the same session. No correctness risk.
        observerRegistration = bundleContext.registerService(
                Observer.class.getName(), getDrainObserver(), null);
    }

    /**
     * Non-OSGi entry point for wiring up the audit pipeline. Called by
     * {@link #activate} in OSGi deployments after the {@code BundleContext}
     * has been unwrapped into an {@code OsgiWhiteboard}, and by embedded
     * callers (tests, {@code OakFixture}) directly.
     * <p>
     * <strong>Embedded callers must follow up with
     * {@link #getDrainObserver()}</strong> to obtain the Observer and attach
     * it to the root NodeStore. See {@link #getDrainObserver()} Javadoc for
     * the recommended attach pattern and the {@code Oak.with(Observer)}
     * caveat.
     * <p>
     * <strong>Must be called exactly once per instance.</strong> Calling
     * it more than once orphans the previous {@code Feature} toggle and
     * registry tracker, and silently overwrites the static
     * {@link AuditEvents} / {@link AuditBufferLifecycle} sinks. To rewire,
     * call {@link #dispose()} first.
     * <p>
     * <strong>Activation ordering rationale.</strong>
     * {@link AuditBufferLifecycle#install AuditBufferLifecycle.install(buffer)}
     * runs before
     * {@link AuditEvents#install AuditEvents.install(BufferSink)} so that any
     * concurrent capture arriving in the install window goes through the
     * NOOP sink (no buffer write) rather than through a live {@code BufferSink}
     * with an orphaned lifecycle handle. The inverse ordering would minimize
     * lifecycle bypass but maximize silent capture loss; we prefer the former.
     *
     * @param whiteboard the whiteboard to register the {@code Feature}
     *                   toggle and {@code AuditEventListener} tracker on;
     *                   non-null.
     */
    public void initialize(@NotNull Whiteboard whiteboard) {
        featureToggle = Feature.newFeature(FEATURE_TOGGLE_NAME, whiteboard);

        registry = new WhiteboardAuditEventListenerRegistry();
        registry.start(whiteboard);

        buffer = new AuditBuffer();
        AuditBufferLifecycle.install(buffer);

        AuditEvents.install(new BufferSink(featureToggle, registry, buffer));

        // Construct the singleton AuditDrainObserver LAST in initialize().
        // This closes a potential TOCTOU window in the OSGi @Activate flow:
        // ObserverTracker (which subscribes to Observer services on a
        // background thread) might fire the bootstrap CommitInfo.EMPTY_EXTERNAL
        // invocation before @Activate returns. With the singleton constructed
        // here (inside initialize, which @Activate calls FIRST), getDrainObserver()
        // is safe to call from anywhere in @Activate after this point.
        drainObserver = new AuditDrainObserver(featureToggle, buffer, registry);

        log.info("Audit pipeline activated. Toggle '{}' = {}.",
                FEATURE_TOGGLE_NAME, featureToggle.isEnabled());
    }

    /**
     * Returns the singleton {@link Observer} bound to this pipeline's
     * buffer, registry, and feature toggle. Constructed once by
     * {@link #initialize(Whiteboard)} and cached for the lifetime of this
     * {@code AuditConfigurationImpl} instance; zeroed by {@link #dispose()}.
     * <p>
     * The singleton shape is deliberate. Each {@code AuditConfigurationImpl}
     * owns at most one Observer because (a) the {@link AuditBuffer}
     * {@code ThreadLocal} is buffer-instance-scoped, so multiple Observer
     * instances would compete for the same drain on every commit thread,
     * and (b) the destructive {@code buffer.drain(sessionId)} contract is
     * the cleanup mechanism — a future non-destructive-drain refactor
     * would silently turn double-attach into double-dispatch.
     * <p>
     * Embedded callers pass the returned Observer to
     * {@code ((Observable) store).addObserver(...)}, holding the returned
     * {@code Closeable} for tear-down. {@code Oak.with(Observer)} is
     * <em>not</em> a reliable embedded path when the caller also passes
     * {@code Oak.with(Whiteboard)} to replace Oak's default whiteboard:
     * the auto-attach at {@code Oak.java:300-302} is wired to the default
     * whiteboard's anonymous override only.
     * <p>
     * OSGi callers never invoke this method directly — {@code @Activate}
     * does, then publishes the singleton via
     * {@code BundleContext.registerService(...)}.
     *
     * @return the singleton drain observer; never {@code null}.
     * @throws IllegalStateException when called before
     *         {@link #initialize(Whiteboard)} OR after {@link #dispose()}
     *         (both states leave {@code drainObserver == null}).
     */
    public @NotNull Observer getDrainObserver() {
        if (drainObserver == null) {
            throw new IllegalStateException(
                    "AuditConfigurationImpl.initialize(...) must be called first" +
                            " (or dispose() has already run)");
        }
        return drainObserver;
    }

    // Package-private (was private) so the test suite can invoke the
    // OSGi-shaped tear-down flow directly to verify the
    // "unregister before dispose internals" ordering invariant via
    // Mockito InOrder. OSGi DS resolves @Deactivate via reflection;
    // package-private access does not change DS binding.
    @Deactivate
    void deactivate() {
        // Step 0: unregister the Observer service FIRST. ObserverTracker
        // notices the service disappear → closes its subscription on the
        // root NodeStore → no further contentChanged calls reach our
        // AuditDrainObserver. A commit thread that's mid-way through
        // contentChanged when this runs is protected by the outer Throwable
        // barrier in AuditDrainObserver.contentChanged (defense in depth).
        if (observerRegistration != null) {
            try {
                observerRegistration.unregister();
            } catch (RuntimeException e) {
                log.warn("Audit deactivate: observerRegistration.unregister() failed; continuing.", e);
            } finally {
                observerRegistration = null;
            }
        }
        dispose();
    }

    /**
     * Non-OSGi tear-down entry point, paired with
     * {@link #initialize(Whiteboard)}. Called by {@link #deactivate} in
     * OSGi deployments (after observer unregistration) and directly by
     * tests / embedded callers. Safe to call when no pipeline was
     * previously initialized — each step guards against unset state.
     * <p>
     * Each cleanup step is wrapped in its own try/catch so an exception
     * at one step does not skip the rest: an OSGi deactivate that leaves
     * static façades pointing at half-torn-down state is worse than a
     * noisy log.
     */
    public void dispose() {
        // Order matters: close the feature toggle first so any racing capture
        // short-circuits before reaching state we're about to tear down; then
        // stop discovery; then NOOP the static façades; then drain the buffer.

        // Precondition: the Observer must be detached from the root NodeStore
        // BEFORE we tear down the pipeline state it references. The outer
        // Throwable barrier in AuditDrainObserver.contentChanged is the safety
        // net, but the dispose-order invariant is the policy.
        // - OSGi path: @Deactivate calls observerRegistration.unregister() then
        //   zeros the field before invoking dispose(); precondition trivially
        //   satisfied.
        // - Embedded path: observerRegistration is null (no OSGi
        //   registerService call); precondition trivially satisfied. The
        //   embedded caller is separately responsible for closing the
        //   Closeable returned by ((Observable) store).addObserver(...) BEFORE
        //   calling dispose() — that Closeable is owned by the caller, not by
        //   AuditConfigurationImpl, because tests/fixtures need explicit
        //   lifecycle control over their per-store subscriptions.
        // - Misuse case (e.g. test calls dispose() directly after @Activate
        //   without invoking @Deactivate): caught here, loud failure with
        //   actionable message.
        if (observerRegistration != null) {
            throw new IllegalStateException(
                    "Observer registration must be unregistered before dispose(). " +
                            "OSGi @Deactivate handles this automatically; " +
                            "direct callers must unregister first.");
        }

        // 1. Close the feature toggle FIRST. AuditEvents.isEnabled()
        //    immediately returns false, so any new capture-site call
        //    that races with deactivation short-circuits before reaching
        //    the buffer (which we're about to dismantle).
        if (featureToggle != null) {
            try {
                featureToggle.close();
            } catch (RuntimeException e) {
                log.warn("Audit deactivate: featureToggle.close() failed; continuing.", e);
            } finally {
                featureToggle = null;
            }
        }
        // 2. Stop discovery — listeners disappear from getServices().
        if (registry != null) {
            try {
                registry.stop();
            } catch (RuntimeException e) {
                log.warn("Audit deactivate: registry.stop() failed; continuing.", e);
            } finally {
                registry = null;
            }
        }
        // 3. Route AuditEvents/AuditBufferLifecycle to NOOP. Now even
        //    callers that already passed the isEnabled() gate land on
        //    no-ops.
        try {
            AuditEvents.install(null);
        } catch (RuntimeException e) {
            log.warn("Audit deactivate: AuditEvents.install(null) failed; continuing.", e);
        }
        try {
            AuditBufferLifecycle.install(null);
        } catch (RuntimeException e) {
            log.warn("Audit deactivate: AuditBufferLifecycle.install(null) failed; continuing.", e);
        }
        // 4. Drain the deactivator thread's ThreadLocal. Residual entries
        //    on other threads are bounded by worker-pool × in-flight
        //    sessions; acknowledged trade-off (no weak-reference machinery).
        if (buffer != null) {
            try {
                buffer.clearAll();
            } catch (RuntimeException e) {
                log.warn("Audit deactivate: buffer.clearAll() failed; continuing.", e);
            } finally {
                buffer = null;
            }
        }
        // 5. Zero the cached singleton observer. Subsequent getDrainObserver()
        //    calls throw IllegalStateException — same contract as pre-init.
        drainObserver = null;
        log.info("Audit pipeline deactivated.");
    }

    //------------------------------------------------< AuditConfiguration >---

    /**
     * Delegates to {@link AuditEvents#isEnabled()} — the single source of
     * truth for "is the audit pipeline up?". The static
     * {@code AuditEvents.sink} field is {@code volatile}, so any thread
     * reading {@code isActive()} sees a JMM-safe value without depending
     * on the OSGi activation publication barrier.
     * <p>
     * Pre-init, post-dispose, and NOOP-bound deployments all return
     * {@code false} for free: the NOOP sink installed by default reports
     * {@code isEnabled() == false}, {@link #initialize(Whiteboard)}
     * installs the active sink as its LAST step, and {@link #dispose()}
     * resets the sink to NOOP. No null-checks needed.
     */
    @Override
    public boolean isActive() {
        return AuditEvents.isEnabled();
    }

    //-----------------------------------------------------------< internal >---
    /**
     * Composite gate exposed to capture sites via {@link AuditEvents}.
     * Both predicates ({@link Feature#isEnabled()} and
     * {@link WhiteboardAuditEventListenerRegistry#hasAnyListener()}) are
     * single volatile reads; together they keep the disabled path free
     * of allocation.
     */
    private static final class BufferSink implements AuditEvents.Sink {

        private final Feature toggle;
        private final WhiteboardAuditEventListenerRegistry registry;
        private final AuditBuffer buffer;

        BufferSink(@NotNull Feature toggle,
                   @NotNull WhiteboardAuditEventListenerRegistry registry,
                   @NotNull AuditBuffer buffer) {
            this.toggle = toggle;
            this.registry = registry;
            this.buffer = buffer;
        }

        @Override
        public boolean isEnabled() {
            return toggle.isEnabled() && registry.hasAnyListener();
        }

        @Override
        public boolean isEnabledFor(@NotNull String domain) {
            return toggle.isEnabled() && registry.hasListenerFor(domain);
        }

        @Override
        public void record(@NotNull Root root, @NotNull AuditEvent event) {
            if (!isEnabledFor(event.getDomain())) {
                return;
            }
            buffer.record(root.getContentSession().toString(), event);
        }

        @Override
        public void dispatch(@NotNull AuditEvent event) {
            if (!toggle.isEnabled()) {
                return;
            }
            List<AuditEventListener> listeners = registry.getListeners();
            if (listeners.isEmpty()) {
                return;
            }
            // Fire-and-forget payloads are caller-supplied and undecorated:
            // strip the three Oak-attested commit.* keys so their presence in
            // any dispatched payload is a reliable "Oak-attested" signal —
            // see the AuditEvent.getPayload() trust contract.
            AuditEvent toDispatch = CommitMetadataDecorator.stripReservedCommitKeys(event);
            String domain = toDispatch.getDomain();
            List<AuditEvent> single = Collections.singletonList(toDispatch);
            for (AuditEventListener listener : listeners) {
                // The listener's getDomain() filter sits INSIDE the barrier —
                // it is listener code just like onEvents(), and a throw here
                // would otherwise escape into the emitter, breaching the
                // published AuditEventEmitter contract ("never propagates
                // back to the caller").
                try {
                    if (!domain.equals(listener.getDomain())) {
                        continue;
                    }
                    listener.onEvents(single);
                } catch (Throwable t) {
                    // Per-listener isolation: a misconfigured consumer bundle whose listener
                    // throws e.g. LinkageError must not fail the commit for unrelated work.
                    // JVM-level pathology (OutOfMemoryError) is caught here too but
                    // re-triggers on the next allocation and surfaces through normal channels.
                    // Do not narrow this catch to RuntimeException — listener Throwables
                    // (any kind) must not escape into the dispatch caller. `domain` is the
                    // EVENT's domain (computed before the loop), not a listener re-invocation.
                    log.warn("AuditEventListener {} threw {} on fire-and-forget dispatch in domain '{}'; isolating from other listeners.",
                            listener.getClass().getName(), t.getClass().getSimpleName(), domain, t);
                }
            }
        }
    }
}
