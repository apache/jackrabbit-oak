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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditEventListener;
import org.apache.jackrabbit.oak.spi.audit.AuditEvents;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

/**
 * Fixture-parameterized end-to-end test of the commit-attached audit
 * pipeline: record an audit event on a session, commit, and assert the
 * registered {@link AuditEventListener} fires with the correct
 * {@code commit.sessionId}.
 * <p>
 * <strong>Fixture coverage note (for turing).</strong> This module
 * ({@code oak-core}) only has in-process access to {@link MemoryNodeStore}
 * via {@code oak-store-spi}. It deliberately does <em>not</em> depend on
 * {@code oak-segment-tar} or {@code oak-store-document} — those modules
 * depend on {@code oak-core}, so adding them here would create a dependency
 * cycle. The test is therefore written as a {@link Parameterized} harness
 * with {@code MEMORY_NS} as the only in-module fixture, but structured so
 * additional fixtures slot in trivially (add a row to {@link #fixtures()}).
 * <p>
 * The {@code SEGMENT_TAR} (synchronous, in-process) and {@code DOCUMENT_NS}
 * (asynchronous observation, MongoDB-backed, must be guarded behind a
 * Mongo-available check) variants belong in a module that already depends on
 * those stores — {@code oak-jcr} or {@code oak-it}. The audit wiring used
 * here is fully public ({@link AuditConfigurationImpl#initialize},
 * {@link AuditConfigurationImpl#getDrainObserver}, {@link AuditEvents},
 * {@link AuditEventListener}), so this class can be lifted there as-is and
 * the extra fixtures added to {@link #fixtures()}. The DOCUMENT_NS row will
 * additionally need to await asynchronous dispatch (the drain observer is
 * synchronous, but external/async observation on DocumentNodeStore is not),
 * e.g. poll {@code received} with a bounded timeout.
 */
@RunWith(Parameterized.class)
public class AuditFixtureTest {

    private static final String DOMAIN = "test.domain";
    private static final String FEATURE_TOGGLE_NAME = AuditConfigurationImpl.FEATURE_TOGGLE_NAME;

    /**
     * Supplies a fresh {@link NodeStore} for each test run of a fixture.
     */
    @FunctionalInterface
    interface NodeStoreFactory {
        @NotNull NodeStore create();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> fixtures() {
        // MEMORY_NS only — see the class Javadoc "Fixture coverage note".
        return Arrays.asList(new Object[][]{
                {"MEMORY_NS",
                        (NodeStoreFactory) () -> new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT)}
        });
    }

    private final String fixtureName;
    private final NodeStoreFactory storeFactory;

    private Whiteboard whiteboard;
    private AuditConfigurationImpl auditConfig;
    private Closeable drainObserverSubscription;
    private List<AuditEvent> received;
    private ContentRepository repository;
    private SecurityProvider securityProvider;

    public AuditFixtureTest(@NotNull String fixtureName, @NotNull NodeStoreFactory storeFactory) {
        this.fixtureName = fixtureName;
        this.storeFactory = storeFactory;
    }

    @Before
    public void setUp() {
        whiteboard = new DefaultWhiteboard();
        received = new CopyOnWriteArrayList<>();

        auditConfig = new AuditConfigurationImpl();
        auditConfig.initialize(whiteboard);
        securityProvider = SecurityProviderBuilder.newBuilder()
                .withWhiteboard(whiteboard)
                .build();

        Configuration.setConfiguration(
                ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY));

        setToggle(true);

        AuditEventListener listener = new AuditEventListener() {
            @Override public @NotNull String getDomain() { return DOMAIN; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                received.addAll(events);
            }
        };
        whiteboard.register(AuditEventListener.class, listener, Map.of());

        NodeStore store = storeFactory.create();
        // The drain observer is attached directly to the store's Observable —
        // .with(whiteboard) below replaces Oak's default whiteboard and bypasses
        // the auto-attach at Oak.java:300-302 (see AuditPipelineTest Javadoc).
        drainObserverSubscription = ((Observable) store).addObserver(auditConfig.getDrainObserver());

        repository = new Oak(store)
                .with(securityProvider)
                .with(whiteboard)
                .createContentRepository();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (drainObserverSubscription != null) {
                drainObserverSubscription.close();
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

    private static Credentials adminCredentials() {
        return new SimpleCredentials("admin", "admin".toCharArray());
    }

    private static AuditEvent eventFor(@NotNull String type, @NotNull Map<String, Object> payload) {
        return new AuditEvent() {
            @Override public @NotNull String getDomain() { return DOMAIN; }
            @Override public @NotNull String getType() { return type; }
            @Override public long getTimestamp() { return System.currentTimeMillis(); }
            @Override public @NotNull Map<String, Object> getPayload() { return payload; }
        };
    }

    @Test
    public void recordedEventReachesListenerWithCommitSessionId() throws Exception {
        try (ContentSession session = repository.login(adminCredentials(), null)) {
            Root root = session.getLatestRoot();
            AuditEvents.record(root, eventFor("commit.type", Map.of("note", "v")));
            root.getTree("/").setProperty("scratch", "value");
            root.commit();

            assertEquals("[" + fixtureName + "] exactly one event must reach the listener",
                    1, received.size());
            AuditEvent e = received.get(0);
            assertEquals("commit.type", e.getType());
            assertEquals("[" + fixtureName + "] commit.sessionId must match the committing session",
                    session.toString(), e.getPayload().get("commit.sessionId"));
            assertEquals("v", e.getPayload().get("note"));
        }
    }
}
