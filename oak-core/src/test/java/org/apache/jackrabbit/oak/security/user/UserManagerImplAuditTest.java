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
package org.apache.jackrabbit.oak.security.user;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditEvents;
import org.apache.jackrabbit.oak.spi.audit.AuditType;
import org.apache.jackrabbit.oak.spi.security.audit.SecurityAuditDomain;
import org.apache.jackrabbit.oak.spi.security.user.UserAuditTypes;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.event.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Branch-coverage tests for the audit-event capture sites in
 * {@link UserManagerImpl#onGroupUpdate}.
 * <p>
 * End-to-end audit dispatch (commit-attached drain + listener invocation)
 * is exercised by {@code AuditWiringTest}/{@code AuditPipelineTest} which build
 * a SecurityProvider with audit hooks wired into the commit chain. Those
 * tests live in {@code org.apache.jackrabbit.oak.security.audit}, so they do
 * not satisfy the unit-test coverage gate that applies to
 * {@code org.apache.jackrabbit.oak.security.user}.
 * <p>
 * This test installs a stub {@link AuditEvents.Sink} so the capture sites
 * exercise their on-path branches (toggle-on, isRemove true/false, single
 * vs bulk, RepositoryException catch) directly. No commit hooks needed.
 * <p>
 * The sink captures the actual {@link AuditEvent} instances — every "happy
 * path" test asserts {@code event.getDomain()} against
 * {@link SecurityAuditDomain#NAME} and {@code event.getType()} against the
 * matching {@link UserAuditTypes} constant. This guards against a
 * silent type-string rename in {@code UserAuditEvents.member*} factories
 * or in {@code UserAuditTypes}: a typo would break the assertion here
 * before downstream listeners would see broken events in production.
 */
public class UserManagerImplAuditTest extends AbstractSecurityTest {

    private final List<AuditEvent> recordedEvents = new CopyOnWriteArrayList<>();

    @Before
    public void installStubSink() {
        recordedEvents.clear();
        AuditEvents.install(new AuditEvents.Sink() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public boolean isEnabledFor(@NotNull AuditDomain domain) {
                return true;
            }

            @Override
            public void record(@NotNull Root r, @NotNull AuditEvent event) {
                recordedEvents.add(event);
            }

            @Override
            public void dispatch(@NotNull AuditEvent event) {
                // not used by the capture sites under test
            }
        });
    }

    @After
    public void resetSink() {
        AuditEvents.install(null);
    }

    @Test
    public void singleMemberAddRecordsEvent() throws Exception {
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        User user = getTestUser();
        Group group = userMgr.createGroup("auditTestGroup1");
        try {
            userMgr.onGroupUpdate(group, false, user);
            assertEquals(1, recordedEvents.size());
            AuditEvent e = recordedEvents.get(0);
            assertEquals(SecurityAuditDomain.DOMAIN, e.getDomain());
            assertEquals(UserAuditTypes.MEMBER_ADDED, e.getType());
            Map<String, Object> payload = e.getPayload();
            assertEquals(group.getPath(), payload.get(UserAuditTypes.PAYLOAD_GROUP_PATH));
            assertEquals(List.of(user.getID()), payload.get(UserAuditTypes.PAYLOAD_MEMBER_IDS));
            assertEquals(List.of(user.getPath()), payload.get(UserAuditTypes.PAYLOAD_MEMBER_PATHS));
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void singleMemberRemoveRecordsEvent() throws Exception {
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        User user = getTestUser();
        Group group = userMgr.createGroup("auditTestGroup2");
        try {
            userMgr.onGroupUpdate(group, true, user);
            assertEquals(1, recordedEvents.size());
            AuditEvent e = recordedEvents.get(0);
            assertEquals(SecurityAuditDomain.DOMAIN, e.getDomain());
            assertEquals(UserAuditTypes.MEMBER_REMOVED, e.getType());
            Map<String, Object> payload = e.getPayload();
            assertEquals(group.getPath(), payload.get(UserAuditTypes.PAYLOAD_GROUP_PATH));
            assertEquals(List.of(user.getID()), payload.get(UserAuditTypes.PAYLOAD_MEMBER_IDS));
            assertEquals(List.of(user.getPath()), payload.get(UserAuditTypes.PAYLOAD_MEMBER_PATHS));
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void bulkMemberAddRecordsEvent() throws Exception {
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        Group group = userMgr.createGroup("auditTestGroup3");
        try {
            userMgr.onGroupUpdate(group, false, false,
                    new HashSet<>(Collections.singleton("memberId")),
                    Collections.emptySet());
            assertEquals(1, recordedEvents.size());
            AuditEvent e = recordedEvents.get(0);
            assertEquals(SecurityAuditDomain.DOMAIN, e.getDomain());
            assertEquals(UserAuditTypes.MEMBER_ADDED, e.getType());
            Map<String, Object> payload = e.getPayload();
            assertEquals(group.getPath(), payload.get(UserAuditTypes.PAYLOAD_GROUP_PATH));
            assertEquals(UserAuditTypes.MEMBERSHIP_SOURCE_STATIC,
                    payload.get(UserAuditTypes.PAYLOAD_MEMBERSHIP_SOURCE));
            assertEquals(Boolean.FALSE, payload.get(UserAuditTypes.PAYLOAD_IS_CONTENT_ID));
            assertEquals(List.of("memberId"), payload.get(UserAuditTypes.PAYLOAD_MEMBER_IDS));
            assertEquals(List.of(), payload.get(UserAuditTypes.PAYLOAD_FAILED_IDS));
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void bulkMemberRemoveRecordsEvent() throws Exception {
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        Group group = userMgr.createGroup("auditTestGroup4");
        try {
            userMgr.onGroupUpdate(group, true, false,
                    new HashSet<>(Collections.singleton("memberId")),
                    Collections.emptySet());
            assertEquals(1, recordedEvents.size());
            AuditEvent e = recordedEvents.get(0);
            assertEquals(SecurityAuditDomain.DOMAIN, e.getDomain());
            assertEquals(UserAuditTypes.MEMBER_REMOVED, e.getType());
            Map<String, Object> payload = e.getPayload();
            assertEquals(group.getPath(), payload.get(UserAuditTypes.PAYLOAD_GROUP_PATH));
            assertEquals(Boolean.FALSE, payload.get(UserAuditTypes.PAYLOAD_IS_CONTENT_ID));
            assertEquals(List.of("memberId"), payload.get(UserAuditTypes.PAYLOAD_MEMBER_IDS));
            assertEquals(List.of(), payload.get(UserAuditTypes.PAYLOAD_FAILED_IDS));
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void auditDisabledShortCircuitsCapture() throws Exception {
        // Sink reports disabled — capture sites must short-circuit before record().
        AuditEvents.install(new AuditEvents.Sink() {
            @Override public boolean isEnabled() { return false; }
            @Override public boolean isEnabledFor(@NotNull AuditDomain domain) { return false; }
            @Override public void record(@NotNull Root r, @NotNull AuditEvent event) {
                recordedEvents.add(event);
            }
            @Override public void dispatch(@NotNull AuditEvent event) { /* unused */ }
        });
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        User user = getTestUser();
        Group group = userMgr.createGroup("auditTestGroup5");
        try {
            userMgr.onGroupUpdate(group, false, user);
            userMgr.onGroupUpdate(group, false, false,
                    new HashSet<>(Collections.singleton("memberId")),
                    Collections.emptySet());
            assertEquals("toggle-off must short-circuit before record()", 0, recordedEvents.size());
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void domainPreciseGateSkipsCaptureWhenNoSecurityListener() throws Exception {
        // A4 regression guard: isEnabled()==true (a listener exists for SOME
        // domain) but isEnabledFor("oak.security")==false (none for the security
        // domain). The capture guard is domain-precise (isEnabledFor), so it must
        // skip entirely — no event built, no path resolution, no record().
        // Reverting the guard to the coarse isEnabled() would capture here, so
        // this test fails on such a regression.
        AuditEvents.install(new AuditEvents.Sink() {
            @Override public boolean isEnabled() { return true; }
            @Override public boolean isEnabledFor(@NotNull AuditDomain domain) { return false; }
            @Override public void record(@NotNull Root r, @NotNull AuditEvent event) {
                recordedEvents.add(event);
            }
            @Override public void dispatch(@NotNull AuditEvent event) { /* unused */ }
        });
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        User user = getTestUser();
        Group group = userMgr.createGroup("auditTestGroupDomainGate");
        try {
            userMgr.onGroupUpdate(group, false, user);
            userMgr.onGroupUpdate(group, false, false,
                    new HashSet<>(Collections.singleton("memberId")),
                    Collections.emptySet());
            assertEquals("domain-precise gate must skip capture when no security-domain listener",
                    0, recordedEvents.size());
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void singleMemberPathResolutionFailureSwallowsEvent() throws Exception {
        // Force RepositoryException from member.getPath() to exercise the catch
        // branch in recordSingleMembershipAuditEvent. record() must never be called.
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        Group group = userMgr.createGroup("auditTestGroup6");
        Authorizable failing = Mockito.mock(Authorizable.class);
        Mockito.when(failing.getPath()).thenThrow(new RepositoryException("boom"));
        try {
            userMgr.onGroupUpdate(group, false, failing);
            assertEquals("RepositoryException must not produce an audit event",
                    0, recordedEvents.size());
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void repeatedPathResolutionFailureWarnsOnceThenSuppresses() throws Exception {
        // Two failures on the SAME UserManagerImpl: the first logs a WARN
        // (audit-completeness signal); the second is suppressed to DEBUG. Pins
        // the rate-limit branch in UserManagerImpl.warnAuditPathResolutionFailed.
        // Both still swallow the event — capture never fails the group update.
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        Group group = userMgr.createGroup("auditTestGroupRepeat");
        Authorizable failing = Mockito.mock(Authorizable.class);
        Mockito.when(failing.getPath()).thenThrow(new RepositoryException("boom"));
        LogCustomizer logCustomizer = LogCustomizer.forLogger(UserManagerImpl.class)
                .enable(Level.WARN).create();
        logCustomizer.starting();
        try {
            userMgr.onGroupUpdate(group, false, failing);
            userMgr.onGroupUpdate(group, false, failing);
            assertEquals("path-resolution failure must produce no audit events",
                    0, recordedEvents.size());
            assertEquals("exactly one WARN — the second occurrence is suppressed to DEBUG",
                    1, logCustomizer.getLogs().size());
        } finally {
            logCustomizer.finished();
            group.remove();
            root.commit();
        }
    }

    @Test
    public void bulkEmptyMemberIdsShortCircuitsCapture() throws Exception {
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        Group group = userMgr.createGroup("auditTestGroup7");
        try {
            // memberIds empty (e.g. all member adds failed upstream) — early-return.
            userMgr.onGroupUpdate(group, false, false,
                    Collections.emptySet(),
                    new HashSet<>(Collections.singleton("failed-id")));
            assertEquals("empty memberIds must not produce a bulk audit event",
                    0, recordedEvents.size());
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void bulkPathResolutionFailureSwallowsEvent() throws Exception {
        // Force RepositoryException from group.getPath() to exercise the bulk
        // catch branch. The mocked Group also fails the GroupAction iteration,
        // which propagates — but the audit-record path's catch is exercised first.
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        Group failingGroup = Mockito.mock(Group.class);
        Mockito.when(failingGroup.getPath()).thenThrow(new RepositoryException("boom"));
        try {
            userMgr.onGroupUpdate(failingGroup, false, false,
                    new HashSet<>(Collections.singleton("memberId")),
                    Collections.emptySet());
        } catch (RepositoryException expected) {
            // GroupAction.onMemberAdded may also propagate after audit's catch handled.
        }
        assertTrue("audit must have swallowed before any record() call",
                recordedEvents.isEmpty());
    }

    @Test
    public void bulkContentIdFlagPropagatesToPayload() throws Exception {
        // Pins the isContentId=true branch of recordBulkMembershipAuditEvent:
        // capture sites in MembershipWriter pass isContentId=true when member IDs
        // are content IDs (rep:members UUIDs) rather than authorizable IDs. The
        // flag must surface in the event payload so listeners can interpret
        // PAYLOAD_MEMBER_IDS correctly.
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        Group group = userMgr.createGroup("auditTestGroup8");
        try {
            userMgr.onGroupUpdate(group, false, true,
                    new HashSet<>(Collections.singleton("content-id-1")),
                    Collections.emptySet());
            assertEquals(1, recordedEvents.size());
            AuditEvent e = recordedEvents.get(0);
            assertEquals(UserAuditTypes.MEMBER_ADDED, e.getType());
            assertEquals(Boolean.TRUE,
                    e.getPayload().get(UserAuditTypes.PAYLOAD_IS_CONTENT_ID));
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void bulkFailedIdsCarryThroughToPayload() throws Exception {
        // Pins that non-empty failedIds surface in the event payload — listeners
        // need this to distinguish "happened" vs "rejected" entries for audit
        // completeness. Per the contract in UserAuditEvents.membersAddedBulk
        // Javadoc, failedIds is defensively copied into an immutable List in the
        // event payload.
        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        Group group = userMgr.createGroup("auditTestGroup9");
        try {
            userMgr.onGroupUpdate(group, false, false,
                    new HashSet<>(Collections.singleton("ok-id")),
                    new HashSet<>(Collections.singleton("failed-id")));
            assertEquals(1, recordedEvents.size());
            AuditEvent e = recordedEvents.get(0);
            Map<String, Object> payload = e.getPayload();
            assertEquals(List.of("ok-id"), payload.get(UserAuditTypes.PAYLOAD_MEMBER_IDS));
            assertEquals(List.of("failed-id"), payload.get(UserAuditTypes.PAYLOAD_FAILED_IDS));
        } finally {
            group.remove();
            root.commit();
        }
    }
}
