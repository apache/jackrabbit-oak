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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DefaultSyncContextTest extends AbstractSecurityTest {

    private TestIdentityProvider idp = new TestIdentityProvider();
    private DefaultSyncConfig config = new DefaultSyncConfig();

    private DefaultSyncContext syncCtx;

    private List<String> authorizableIds = new ArrayList<String>();

    @Before
    public void before() throws Exception {
        super.before();
        syncCtx = new DefaultSyncContext(config, idp, getUserManager(root), getValueFactory());
    }

    @After
    public void after() throws Exception {
        try {
            syncCtx.close();
            root.refresh();
            UserManager umgr = getUserManager(root);
            Iterator<ExternalIdentity> ids = Iterators.concat(idp.listGroups(), idp.listUsers());
            while (ids.hasNext()) {
                Authorizable a = umgr.getAuthorizable(ids.next().getId());
                if (a != null) {
                    a.remove();
                }
            }
            for (String id : authorizableIds) {
                Authorizable a = umgr.getAuthorizable(id);
                if (a != null) {
                    a.remove();
                }
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    private Group createTestGroup() throws Exception {
        Group gr = getUserManager(root).createGroup("group" + UUID.randomUUID());
        authorizableIds.add(gr.getID());
        return gr;
    }

    private void setExternalID(@Nonnull Authorizable authorizable, @Nullable String idpName) throws RepositoryException {
        authorizable.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, getValueFactory().createValue(authorizable.getID() + ';' + idpName));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSyncInvalidExternalIdentity() throws Exception {
        syncCtx.sync(new TestExternalIdentity());
    }

    @Test
    public void testSyncExternalUser() throws Exception {
        ExternalUser user = idp.listUsers().next();
        assertNotNull(user);

        SyncResult result = syncCtx.sync(user);
        assertEquals(SyncResult.Status.ADD, result.getStatus());

        result = syncCtx.sync(user);
        assertEquals(SyncResult.Status.NOP, result.getStatus());

        syncCtx.setForceUserSync(true);
        result = syncCtx.sync(user);
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());

    }

    @Test
    public void testSyncExternalGroup() throws Exception {
        ExternalGroup gr = idp.listGroups().next();
        assertNotNull(gr);

        SyncResult result = syncCtx.sync(gr);
        assertEquals(SyncResult.Status.ADD, result.getStatus());

        result = syncCtx.sync(gr);
        assertEquals(SyncResult.Status.NOP, result.getStatus());

        syncCtx.setForceGroupSync(true);
        result = syncCtx.sync(gr);
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());
    }

    @Test
    public void testSyncUserById() throws Exception {
        ExternalIdentity externalId = idp.listUsers().next();

        // no initial sync -> sync-by-id doesn't succeed
        SyncResult result = syncCtx.sync(externalId.getId());
        assertEquals(SyncResult.Status.NO_SUCH_AUTHORIZABLE, result.getStatus());

        // force sync
        syncCtx.sync(externalId);

        // try again
        syncCtx.setForceUserSync(true);
        result = syncCtx.sync(externalId.getId());
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());
    }

    @Test
    public void testSyncRemovedUserById() throws Exception {
        // mark a regular repo user as external user from the test IDP
        User u = getUserManager(root).createUser("test" + UUID.randomUUID(), null);
        String userId = u.getID();
        authorizableIds.add(userId);

        setExternalID(u, idp.getName());

        // test sync with 'keepmissing' = true
        syncCtx.setKeepMissing(true);
        SyncResult result = syncCtx.sync(userId);
        assertEquals(SyncResult.Status.MISSING, result.getStatus());
        assertNotNull(getUserManager(root).getAuthorizable(userId));

        // test sync with 'keepmissing' = false
        syncCtx.setKeepMissing(false);
        result = syncCtx.sync(userId);
        assertEquals(SyncResult.Status.DELETE, result.getStatus());

        assertNull(getUserManager(root).getAuthorizable(userId));
    }

    @Test
    public void testSyncGroupById() throws Exception {
        ExternalIdentity externalId = idp.listGroups().next();

        // no initial sync -> sync-by-id doesn't succeed
        SyncResult result = syncCtx.sync(externalId.getId());
        assertEquals(SyncResult.Status.NO_SUCH_AUTHORIZABLE, result.getStatus());

        // force sync
        syncCtx.sync(externalId);

        // try again
        syncCtx.setForceGroupSync(true);
        result = syncCtx.sync(externalId.getId());
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());
    }

    @Test
    public void testSyncRemovedGroupById() throws Exception {
        // mark a regular repo user as external user from the test IDP
        Group gr = createTestGroup();
        String groupId = gr.getID();

        setExternalID(gr, idp.getName());

        // test sync with 'keepmissing' = true
        syncCtx.setKeepMissing(true);
        SyncResult result = syncCtx.sync(groupId);
        assertEquals(SyncResult.Status.MISSING, result.getStatus());
        assertNotNull(getUserManager(root).getAuthorizable(groupId));

        // test sync with 'keepmissing' = false
        syncCtx.setKeepMissing(false);
        result = syncCtx.sync(groupId);
        assertEquals(SyncResult.Status.DELETE, result.getStatus());

        assertNull(getUserManager(root).getAuthorizable(groupId));
    }

    @Test
    public void testSyncRemovedGroupWithMembers() throws Exception {
        // mark a regular repo user as external user from the test IDP
        Group gr = createTestGroup();
        gr.addMember(getTestUser());

        String groupId = gr.getID();
        setExternalID(gr, idp.getName());

        // test sync with 'keepmissing' = true
        syncCtx.setKeepMissing(true);
        SyncResult result = syncCtx.sync(groupId);
        assertEquals(SyncResult.Status.NOP, result.getStatus());
        assertNotNull(getUserManager(root).getAuthorizable(groupId));

        // test sync with 'keepmissing' = false
        syncCtx.setKeepMissing(false);
        result = syncCtx.sync(groupId);
        assertEquals(SyncResult.Status.NOP, result.getStatus());

        assertNotNull(getUserManager(root).getAuthorizable(groupId));
    }

    @Test
    public void testSyncByForeignId() throws Exception {
        SyncResult result = syncCtx.sync(getTestUser().getID());
        assertEquals(SyncResult.Status.FOREIGN, result.getStatus());
    }

    @Test
    public void testSyncByForeignId2() throws Exception {
        User u = getTestUser();
        setExternalID(u, "differentIDP");

        SyncResult result = syncCtx.sync(u.getID());
        assertEquals(SyncResult.Status.FOREIGN, result.getStatus());
    }

    @Test
    public void testSyncAutoMembership() throws Exception {
        Group gr = createTestGroup();

        config.user().setAutoMembership(gr.getID());

        SyncResult result = syncCtx.sync(idp.listUsers().next());
        assertEquals(SyncResult.Status.ADD, result.getStatus());

        Authorizable a = getUserManager(root).getAuthorizable(result.getIdentity().getId());
        assertTrue(gr.isDeclaredMember(a));
    }

    @Test
    public void testSyncAutoMembershipListsNonExistingGroup() throws Exception {
        config.user().setAutoMembership("nonExistingGroup");

        SyncResult result = syncCtx.sync(idp.listUsers().next());
        assertEquals(SyncResult.Status.ADD, result.getStatus());
    }

    @Test
    public void testSyncAutoMembershipListsUser() throws Exception {
        // set auto-membership config to point to a user instead a group
        config.user().setAutoMembership(getTestUser().getID());
        syncCtx.sync(idp.listUsers().next());
    }

    @Test
    public void testLostMembership() throws Exception {
        // create a group in the repository which is marked as being external
        // and associated with the test-IDP to setup the situation that a
        // repository group is no longer listed in the IDP.
        Group gr = createTestGroup();
        setExternalID(gr, idp.getName());

        // sync an external user from the IDP into the repo and make it member
        // of the test group
        SyncResult result = syncCtx.sync(idp.listUsers().next());
        User user = getUserManager(root).getAuthorizable(result.getIdentity().getId(), User.class);
        gr.addMember(user);
        root.commit();

        // enforce synchronization of the user and it's group membership
        syncCtx.setForceUserSync(true);
        config.user().setMembershipExpirationTime(-1);

        // 1. membership nesting is < 0 => membership not synchronized
        config.user().setMembershipNestingDepth(-1);
        syncCtx.sync(user.getID()).getStatus();
        assertTrue(gr.isDeclaredMember(user));

        // 2. membership nesting is > 0 => membership gets synchronized
        config.user().setMembershipNestingDepth(1);
        assertEquals(SyncResult.Status.UPDATE, syncCtx.sync(user.getID()).getStatus());

        assertFalse(gr.isDeclaredMember(user));
    }

    @Test
    public void testLostMembershipDifferentIDP() throws Exception {
        // create a group in the repository which is marked as being external
        // and associated with another IPD.
        Group gr = createTestGroup();
        setExternalID(gr, "differentIDP");

        // sync an external user from the IDP into the repo and make it member
        // of the test group
        SyncResult result = syncCtx.sync(idp.listUsers().next());
        User user = getUserManager(root).getAuthorizable(result.getIdentity().getId(), User.class);
        gr.addMember(user);
        root.commit();

        // enforce synchronization of the user and it's group membership
        syncCtx.setForceUserSync(true);
        config.user().setMembershipExpirationTime(-1);
        config.user().setMembershipNestingDepth(1);

        assertEquals(SyncResult.Status.UPDATE, syncCtx.sync(user.getID()).getStatus());

        // since the group is not associated with the test-IDP the group-membership
        // must NOT be modified during the sync.
        assertTrue(gr.isDeclaredMember(user));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4231">OAK-4231</a>
     */
    @Test
    public void testCreateValueFromBinary() throws Exception {
        byte[] bytes = new byte[]{'a', 'b'};
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        Binary binary = getValueFactory().createBinary(is);

        Value v = syncCtx.createValue(binary);
        assertNotNull(v);
        assertEquals(PropertyType.BINARY, v.getType());
        assertEquals(binary, v.getBinary());
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4231">OAK-4231</a>
     */
    @Test
    public void testCreateValueFromInputStream() throws Exception {
        byte[] bytes = new byte[]{'a', 'b'};
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        Binary binary = getValueFactory().createBinary(is);

        Value v = syncCtx.createValue(is);
        assertNotNull(v);
        assertEquals(PropertyType.BINARY, v.getType());
        assertEquals(binary, v.getBinary());
    }

    /**
     * ExternalIdentity implementation that is neither user nor group.
     */
    private final class TestExternalIdentity implements ExternalIdentity {

        @Nonnull
        @Override
        public ExternalIdentityRef getExternalId() {
            return new ExternalIdentityRef(getId(), idp.getName());
        }

        @Nonnull
        @Override
        public String getId() {
            return "externalId";
        }

        @Nonnull
        @Override
        public String getPrincipalName() {
            return "principalName";
        }

        @CheckForNull
        @Override
        public String getIntermediatePath() {
            return null;
        }

        @Nonnull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredGroups() {
            return ImmutableSet.of();
        }

        @Nonnull
        @Override
        public Map<String, ?> getProperties() {
            return ImmutableMap.of();
        }
    }
}