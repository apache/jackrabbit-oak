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

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.security.audit.AuditConfigurationImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditEventListener;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.audit.SecurityAuditDomain;
import org.apache.jackrabbit.oak.spi.security.user.UserAuditTypes;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end integration test for the AUDIT-SPI production wiring path.
 * <p>
 * Unlike {@link AuditPipelineTest} which records events directly via
 * {@code AuditEvents.record}, this test exercises the path that real Oak
 * consumers traverse:
 * <ol>
 *   <li>JCR {@link UserManager#createGroup(String)} → {@link Group#addMember(org.apache.jackrabbit.api.security.user.Authorizable)}.</li>
 *   <li>{@code UserManagerImpl.recordSingleMembershipAuditEvent} →
 *       {@code AuditEvents.record(root, UserAuditEvents.memberAdded(...))}.</li>
 *   <li>{@code AuditDrainObserver} (fires on commit success) → the registered
 *       listener.</li>
 * </ol>
 * Asserts the entire chain: capture-site → buffer → observer drain →
 * decorator → listener.
 */
public class AuditWiringTest {

    private Whiteboard whiteboard;
    private AuditConfigurationImpl auditConfig;
    private Closeable drainObserverSubscription;
    private List<AuditEvent> received;
    private ContentRepository repository;
    private SecurityProvider securityProvider;

    @Before
    public void setUp() {
        whiteboard = new DefaultWhiteboard();
        received = new CopyOnWriteArrayList<>();

        auditConfig = new AuditConfigurationImpl();
        // initialize() installs sinks/registry/buffer/toggle. The drain Observer
        // is attached to the MemoryNodeStore directly below; we can't rely on
        // Oak.with(Observer)'s auto-attach because .with(whiteboard) replaces
        // Oak's default whiteboard and bypasses the auto-attach at
        // Oak.java:300-302.
        auditConfig.initialize(whiteboard);
        securityProvider = SecurityProviderBuilder.newBuilder()
                .withWhiteboard(whiteboard)
                .build();

        Configuration.setConfiguration(
                ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY));

        setToggle(true);

        // Listener for the security domain — that's where the member-added event lands.
        AuditEventListener securityListener = new AuditEventListener() {
            @Override public @NotNull String getDomain() { return SecurityAuditDomain.NAME; }
            @Override public void onEvents(@NotNull List<AuditEvent> events) {
                received.addAll(events);
            }
        };
        whiteboard.register(AuditEventListener.class, securityListener, Map.of());

        MemoryNodeStore store = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        drainObserverSubscription = store.addObserver(auditConfig.getDrainObserver());

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
                if (AuditConfigurationImpl.FEATURE_TOGGLE_NAME.equals(ft.getName())) {
                    ft.setEnabled(enabled);
                }
            }
        } finally {
            toggleTracker.stop();
        }
    }

    private ContentSession adminLogin() throws Exception {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
    }

    private UserManager userManager(@NotNull Root root) {
        return securityProvider.getConfiguration(UserConfiguration.class)
                .getUserManager(root, NamePathMapper.DEFAULT);
    }

    /**
     * The capture-site in {@code UserManagerImpl.addMember} fires an audit
     * event with domain {@link SecurityAuditDomain#NAME} and type
     * {@link UserAuditTypes#MEMBER_ADDED} on successful group
     * update; the event must traverse the entire pipeline to the
     * registered listener with the commit metadata decorated.
     */
    @Test
    public void groupAddMemberFiresUserMemberAddedEndToEnd() throws Exception {
        try (ContentSession session = adminLogin()) {
            Root root = session.getLatestRoot();
            UserManager um = userManager(root);

            Group testGroup = um.createGroup("auditTestGroup");
            User testUser = um.createUser("auditTestUser", "pwd");
            root.commit();
            // The createGroup/createUser commits above do not emit member
            // events. Reset received and exercise addMember below.
            received.clear();

            // Re-fetch from a fresh root post-commit.
            root = session.getLatestRoot();
            um = userManager(root);
            testGroup = (Group) um.getAuthorizable("auditTestGroup");
            testUser = (User) um.getAuthorizable("auditTestUser");
            assertNotNull(testGroup);
            assertNotNull(testUser);
            String groupPath = testGroup.getPath();
            String memberId = testUser.getID();
            String memberPath = testUser.getPath();

            assertTrue("addMember must succeed", testGroup.addMember(testUser));
            root.commit();

            // Exactly one membership.added event must have traversed the
            // entire pipeline.
            assertEquals("exactly one member-added audit event must arrive",
                    1, received.size());
            AuditEvent event = received.get(0);
            assertEquals(SecurityAuditDomain.NAME, event.getDomain());
            assertEquals(UserAuditTypes.MEMBER_ADDED, event.getType());

            Map<String, Object> payload = event.getPayload();
            // Commit metadata decorated by AuditDrainObserver (via CommitMetadataDecorator).
            assertTrue("commit.sessionId must be decorated",
                    payload.containsKey("commit.sessionId"));
            assertTrue("commit.userId must be decorated",
                    payload.containsKey("commit.userId"));
            assertTrue("commit.timestamp must be decorated",
                    payload.containsKey("commit.timestamp"));
            // Event-specific payload — values, not just key presence,
            // so a future refactor that left the keys but lost the values
            // (e.g. wrong getPath() variable in the capture site) is caught.
            assertEquals(groupPath, payload.get(UserAuditTypes.PAYLOAD_GROUP_PATH));
            assertEquals(List.of(memberId), payload.get(UserAuditTypes.PAYLOAD_MEMBER_IDS));
            assertEquals(List.of(memberPath), payload.get(UserAuditTypes.PAYLOAD_MEMBER_PATHS));
        }
    }

    /**
     * Symmetric to {@link #groupAddMemberFiresUserMemberAddedEndToEnd()}:
     * the capture-site in {@code UserManagerImpl.removeMember} fires an
     * audit event with type
     * {@link UserAuditTypes#MEMBER_REMOVED} on successful group
     * update. Exercises the {@code isRemove=true} branch of
     * {@code recordSingleMembershipAuditEvent} end-to-end through the
     * entire pipeline.
     */
    @Test
    public void groupRemoveMemberFiresUserMemberRemovedEndToEnd() throws Exception {
        try (ContentSession session = adminLogin()) {
            Root root = session.getLatestRoot();
            UserManager um = userManager(root);

            // Setup: create group + user, add user as member, commit.
            Group testGroup = um.createGroup("auditRemoveGroup");
            User testUser = um.createUser("auditRemoveUser", "pwd");
            root.commit();
            root = session.getLatestRoot();
            um = userManager(root);
            testGroup = (Group) um.getAuthorizable("auditRemoveGroup");
            testUser = (User) um.getAuthorizable("auditRemoveUser");
            assertNotNull(testGroup);
            assertNotNull(testUser);
            assertTrue("addMember setup must succeed", testGroup.addMember(testUser));
            root.commit();

            // Clear received — the setup-commit emits membership.added,
            // not the event we want to pin here.
            received.clear();
            root = session.getLatestRoot();
            um = userManager(root);
            testGroup = (Group) um.getAuthorizable("auditRemoveGroup");
            testUser = (User) um.getAuthorizable("auditRemoveUser");
            assertNotNull(testGroup);
            assertNotNull(testUser);
            String groupPath = testGroup.getPath();
            String memberId = testUser.getID();
            String memberPath = testUser.getPath();

            // Act: remove the member and commit.
            assertTrue("removeMember must succeed", testGroup.removeMember(testUser));
            root.commit();

            assertEquals("exactly one member-removed audit event must arrive",
                    1, received.size());
            AuditEvent event = received.get(0);
            assertEquals(SecurityAuditDomain.NAME, event.getDomain());
            assertEquals(UserAuditTypes.MEMBER_REMOVED, event.getType());

            Map<String, Object> payload = event.getPayload();
            assertTrue("commit.sessionId must be decorated",
                    payload.containsKey("commit.sessionId"));
            assertEquals(groupPath, payload.get(UserAuditTypes.PAYLOAD_GROUP_PATH));
            assertEquals(List.of(memberId), payload.get(UserAuditTypes.PAYLOAD_MEMBER_IDS));
            assertEquals(List.of(memberPath), payload.get(UserAuditTypes.PAYLOAD_MEMBER_PATHS));
        }
    }

    /**
     * With the feature toggle disabled, the capture-site
     * {@code AuditEvents.isEnabled()} check in
     * {@code UserManagerImpl.recordSingleMembershipAuditEvent} short-circuits;
     * no event is delivered even though the group update succeeds.
     */
    @Test
    public void toggleDisabledSkipsCaptureSite() throws Exception {
        setToggle(false);
        try (ContentSession session = adminLogin()) {
            Root root = session.getLatestRoot();
            UserManager um = userManager(root);
            Group testGroup = um.createGroup("auditOffGroup");
            User testUser = um.createUser("auditOffUser", "pwd");
            root.commit();
            received.clear();

            root = session.getLatestRoot();
            um = userManager(root);
            testGroup = (Group) um.getAuthorizable("auditOffGroup");
            testUser = (User) um.getAuthorizable("auditOffUser");
            assertTrue(testGroup.addMember(testUser));
            root.commit();

            assertTrue("no event must be delivered with toggle disabled",
                    received.isEmpty());
        }
    }
}
