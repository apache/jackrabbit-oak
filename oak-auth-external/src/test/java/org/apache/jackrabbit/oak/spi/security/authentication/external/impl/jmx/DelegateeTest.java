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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx;

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncResultImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.DEFAULT_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_TEST_USER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class DelegateeTest extends AbstractJmxTest {

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { 100, "BatchSize 100" },
                new Object[] { 1, "BatchSize 1" },
                new Object[] { 2, "BatchSize 2" });
    }

    private final int batchSize;

    private Delegatee delegatee;

    private static final String[] TEST_IDS = new String[] {
            ID_TEST_USER,
            TestIdentityProvider.ID_SECOND_USER,
            TestIdentityProvider.ID_WILDCARD_USER};

    public DelegateeTest(int batchSize, String name) {
        this.batchSize = batchSize;
    }

    @Before
    public void before() throws Exception {
        super.before();

        delegatee = createDelegatee(idp, new DefaultSyncHandler(syncConfig));
    }

    @Override
    public void after() throws Exception {
        try {
            if (delegatee != null) {
                delegatee.close();
            }
        } finally {
            super.after();
        }
    }

    private Delegatee createDelegatee(@NotNull ExternalIdentityProvider idp, @NotNull SyncHandler syncHandler) {
        return Delegatee.createInstance(getContentRepository(), getSecurityProvider(), syncHandler, idp, batchSize);
    }

    private static Root preventRootCommit(@NotNull Delegatee delegatee) throws Exception {
        Field rootField = Delegatee.class.getDeclaredField("root");
        rootField.setAccessible(true);

        Root r = (Root) rootField.get(delegatee);
        r.refresh();
        rootField.set(delegatee, new ThrowingRoot(r));
        return r;
    }

    @Test
    public void testDoubleClose() throws Exception {
        ContentSession cs = mock(ContentSession.class);
        when(cs.getLatestRoot()).thenReturn(root);
        when(cs.getAuthInfo()).thenReturn(AuthInfo.EMPTY);
        
        ContentRepository repo = mock(ContentRepository.class);
        when(repo.login(null, null)).thenReturn(cs);
        
        Delegatee dg = Delegatee.createInstance(repo, getSecurityProvider(), new DefaultSyncHandler(syncConfig), new TestIdentityProvider());
        dg.close();
        dg.close();
        
        verify(repo).login(null, null);
        verifyNoMoreInteractions(repo);
        verify(cs, times(2)).close();
    }

    @Test
    public void testCloseFails() throws Exception {
        ContentSession cs = mock(ContentSession.class);
        doThrow(new IOException()).when(cs).close();
        when(cs.getLatestRoot()).thenReturn(root);
        when(cs.getAuthInfo()).thenReturn(AuthInfo.EMPTY);

        ContentRepository repo = mock(ContentRepository.class);
        when(repo.login(null, null)).thenReturn(cs);

        Delegatee dg = Delegatee.createInstance(repo, getSecurityProvider(), new DefaultSyncHandler(syncConfig), new TestIdentityProvider());
        dg.close();

        verify(repo).login(null, null);
        verifyNoMoreInteractions(repo);
        verify(cs).close();
    }

    @Test
    public void testSyncUsersBeforeSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);

        String[] result = delegatee.syncUsers(TEST_IDS, false);
        assertResultMessages(result, ImmutableMap.of(
                ID_TEST_USER, "nsa",
                TestIdentityProvider.ID_SECOND_USER, "nsa",
                TestIdentityProvider.ID_WILDCARD_USER, "nsa"));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncUsersSaveError() throws Exception {
        sync(idp, ID_TEST_USER, false);
        sync(foreignIDP, TestIdentityProvider.ID_SECOND_USER, false);
        // don't sync ID_WILDCARD_USER

        Root r = preventRootCommit(delegatee);

        String[] result = delegatee.syncUsers(new String[] {
                ID_TEST_USER,
                TestIdentityProvider.ID_SECOND_USER,
                TestIdentityProvider.ID_WILDCARD_USER}, false);
        assertResultMessages(result, ImmutableMap.of(
                ID_TEST_USER, "ERR",
                TestIdentityProvider.ID_SECOND_USER, "for",
                TestIdentityProvider.ID_WILDCARD_USER, "nsa"));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncAllUsersBeforeSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);

        String[] result = delegatee.syncAllUsers(false);
        assertResultMessages(result, ImmutableMap.of());
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncAllUsersSaveError() throws Exception {
        sync(idp, ID_TEST_USER, false);
        sync(idp, TestIdentityProvider.ID_SECOND_USER, false);
        sync(new TestIdentityProvider.TestUser("third", idp.getName()), idp);
        sync(foreignIDP, TestIdentityProvider.ID_WILDCARD_USER, false);

        Root r = preventRootCommit(delegatee);

        ImmutableMap<String, String> expected = ImmutableMap.<String, String>builder()
                .put(ID_TEST_USER, "ERR")
                .put("a", "ERR")
                .put("b", "ERR")
                .put("c", "ERR")
                .put(TestIdentityProvider.ID_SECOND_USER, "ERR")
                .put("secondGroup", "ERR")
                .put("third", "mis").build();

        String[] result = delegatee.syncAllUsers(false);
        assertResultMessages(result, expected);
        // NOTE: foreign user is not included in the results
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncAllUsersPurgeSaveError() throws Exception {
        sync(idp, ID_TEST_USER, false);
        sync(idp, TestIdentityProvider.ID_SECOND_USER, false);
        sync(new TestIdentityProvider.TestUser("third", idp.getName()), idp);
        sync(foreignIDP, TestIdentityProvider.ID_WILDCARD_USER, false);

        Root r = preventRootCommit(delegatee);

        ImmutableMap<String, String> expected = ImmutableMap.<String, String>builder()
                .put(ID_TEST_USER, "ERR")
                .put("a", "ERR")
                .put("b", "ERR")
                .put("c", "ERR")
                .put(TestIdentityProvider.ID_SECOND_USER, "ERR")
                .put("secondGroup", "ERR")
                .put("third", "ERR").build();

        String[] result = delegatee.syncAllUsers(true);
        assertResultMessages(result, expected);
        // NOTE: foreign user is not included in the results
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncNonExistingExternalUserSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);

        String[] result = delegatee.syncExternalUsers(new String[] {new ExternalIdentityRef("nonExisting", idp.getName()).getString()});
        assertResultMessages(result, "", "nsi");
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncForeignExternalUserSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);

        String[] result = delegatee.syncExternalUsers(new String[] {new ExternalIdentityRef(ID_TEST_USER, foreignIDP.getName()).getString()});
        assertResultMessages(result, ID_TEST_USER, "for");
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncThrowingExternalUserSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);

        String[] result = delegatee.syncExternalUsers(new String[] {new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()});
        assertResultMessages(result, TestIdentityProvider.ID_EXCEPTION, "ERR");
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncExternalUsersSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);

        List<String> externalIds = new ArrayList<>();
        for (String id : TEST_IDS) {
                externalIds.add(new ExternalIdentityRef(id, idp.getName()).getString());
        }
        String[] result = delegatee.syncExternalUsers(externalIds.toArray(new String[0]));
        assertResultMessages(result, ImmutableMap.of(
                ID_TEST_USER, "ERR",
                TestIdentityProvider.ID_SECOND_USER, "ERR",
                TestIdentityProvider.ID_WILDCARD_USER, "ERR"));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncExternalUsersGeneratesNullIdentity() throws Exception {
        SyncContext ctx = mock(SyncContext.class);
        when(ctx.sync(any(ExternalIdentity.class))).thenReturn(new DefaultSyncResultImpl(null, SyncResult.Status.NOP));
        when(ctx.setForceGroupSync(anyBoolean())).thenReturn(ctx);
        SyncHandler syncHandler = when(mock(SyncHandler.class).createContext(any(ExternalIdentityProvider.class), any(UserManager.class), any(ValueFactory.class))).thenReturn(ctx).getMock();

        Delegatee d = createDelegatee(new TestIdentityProvider(), syncHandler);
        ExternalIdentityRef ref = new ExternalIdentityRef(ID_TEST_USER, DEFAULT_IDP_NAME);
        String[] res = d.syncExternalUsers(new String[] {ref.getString()});

        assertResultMessages(res, ID_TEST_USER, "nsi");
    }

    @Test
    public void testSyncAllExternalUsersSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);

        String[] result = delegatee.syncAllExternalUsers();
        assertResultMessages(result, ImmutableMap.of(
                ID_TEST_USER, "ERR",
                TestIdentityProvider.ID_SECOND_USER, "ERR",
                TestIdentityProvider.ID_WILDCARD_USER, "ERR"));
        assertFalse(r.hasPendingChanges());
    }


    @Test(expected = SyncRuntimeException.class)
    public void testSyncAllExternalUsersThrowingIDP() {
        Delegatee dg = createDelegatee(new TestIdentityProvider("throwing") {

            @NotNull
            @Override
            public Iterator<ExternalUser> listUsers() throws ExternalIdentityException {
                throw new ExternalIdentityException();
            }
        }, new DefaultSyncHandler(syncConfig));

        dg.syncAllExternalUsers();
    }

    @Test
    public void testListOrphanedUsersFiltersNullSyncIdentity() throws Exception {
        Iterator<SyncedIdentity> it = Collections.singletonList((SyncedIdentity) null).iterator();
        SyncHandler syncHandler = mock(SyncHandler.class);
        when(syncHandler.listIdentities(any(UserManager.class))).thenReturn(it);

        Delegatee dg = createDelegatee(new TestIdentityProvider(), syncHandler);
        assertEquals(0, dg.listOrphanedUsers().length);
    }

    @Test
    public void testListOrphanedUsersFiltersForeignSyncIdentity() throws Exception {
        SyncedIdentity foreign = new DefaultSyncedIdentity(ID_TEST_USER, null, false, -1);
        SyncedIdentity foreign2 = new DefaultSyncedIdentity(ID_TEST_USER, new ExternalIdentityRef(ID_TEST_USER, null), false, -1);
        SyncedIdentity foreign3 = new DefaultSyncedIdentity(ID_TEST_USER, new ExternalIdentityRef(ID_TEST_USER, "other"), false, -1);
        SyncedIdentity emptyIdpName = new DefaultSyncedIdentity(ID_TEST_USER, new ExternalIdentityRef(ID_TEST_USER, ""), false, -1);
        Iterator<SyncedIdentity> it = Arrays.asList(foreign, foreign2, foreign3, emptyIdpName).iterator();
        SyncHandler syncHandler = mock(SyncHandler.class);
        when(syncHandler.listIdentities(any(UserManager.class))).thenReturn(it);

        Delegatee dg = createDelegatee(new TestIdentityProvider(), syncHandler);
        assertEquals(0, dg.listOrphanedUsers().length);
    }

    @Test
    public void testPurgeOrphanedSaveError() throws Exception {
        sync(new TestIdentityProvider.TestUser("third", idp.getName()), idp);
        sync(new TestIdentityProvider.TestUser("forth", idp.getName()), idp);
        sync(idp, ID_TEST_USER, false);

        Root r = preventRootCommit(delegatee);

        String[] result = delegatee.purgeOrphanedUsers();
        assertResultMessages(result, ImmutableMap.of(
                "third", "ERR",
                "forth", "ERR"));
        assertFalse(r.hasPendingChanges());
    }
    
    @Test
    public void testConvertToDynamicMembershipFailsWithRepositoryException() throws Exception {
        syncConfig.user().setDynamicMembership(true);
        sync(idp.getUser(ID_TEST_USER), idp);
        
        UserManager um = spy(getUserManager(root));
        when(um.getAuthorizable(any(String.class))).thenThrow(new RepositoryException());
        
        UserConfiguration uc = mock(UserConfiguration.class);
        when(uc.getUserManager(any(Root.class), any(NamePathMapper.class))).thenReturn(um);
        SecurityProvider sp = spy(securityProvider);
        when(sp.getConfiguration(UserConfiguration.class)).thenReturn(uc);
        
        Delegatee delegatee = Delegatee.createInstance(getContentRepository(), sp, new DefaultSyncHandler(syncConfig), idp);
        try {
            delegatee.convertToDynamicMembership();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // success
        }
    }

    @Test
    public void testConvertToDynamicMembershipUserNotFound() throws Exception {
        syncConfig.user().setDynamicMembership(true);
        sync(idp.getUser(ID_TEST_USER), idp);

        UserManager um = spy(getUserManager(root));
        when(um.getAuthorizable(any(String.class))).thenReturn(null);

        UserConfiguration uc = mock(UserConfiguration.class);
        when(uc.getUserManager(any(Root.class), any(NamePathMapper.class))).thenReturn(um);
        SecurityProvider sp = spy(securityProvider);
        when(sp.getConfiguration(UserConfiguration.class)).thenReturn(uc);

        Delegatee delegatee = Delegatee.createInstance(getContentRepository(), sp, new DefaultSyncHandler(syncConfig), idp);
        String[] result = delegatee.convertToDynamicMembership();
        
        ResultMessages expected = new ResultMessages();
        DefaultSyncedIdentity dsi = DefaultSyncContext.createSyncedIdentity(getUserManager().getAuthorizable(ID_TEST_USER));
        expected.append(Collections.singletonList(new DefaultSyncResultImpl(dsi, SyncResult.Status.NO_SUCH_AUTHORIZABLE)));

        assertArrayEquals(expected.getMessages(), result);
    }

    private static final class ThrowingRoot implements Root {

        private final Root base;

        private ThrowingRoot(@NotNull Root base) {
            this.base = base;
        }

        @Override
        public boolean move(String srcAbsPath, String destAbsPath) {
            return base.move(srcAbsPath, destAbsPath);
        }

        @NotNull
        @Override
        public Tree getTree(@NotNull String path) {
            return base.getTree(path);
        }

        @Override
        public void rebase() {
            base.rebase();
        }

        @Override
        public void refresh() {
            base.refresh();
        }

        @Override
        public void commit() throws CommitFailedException {
            commit(ImmutableMap.of());
        }

        @Override
        public void commit(@NotNull Map<String, Object> info) throws CommitFailedException {
            throw new CommitFailedException(CommitFailedException.OAK, 0, "failed");
        }

        @Override
        public boolean hasPendingChanges() {
            return base.hasPendingChanges();
        }

        @NotNull
        @Override
        public QueryEngine getQueryEngine() {
            return base.getQueryEngine();
        }

        @NotNull
        @Override
        public Blob createBlob(@NotNull InputStream stream) throws IOException {
            return base.createBlob(stream);
        }

        @Nullable
        @Override
        public Blob getBlob(@NotNull String reference) {
            return base.getBlob(reference);
        }

        @NotNull
        @Override
        public ContentSession getContentSession() {
            return base.getContentSession();
        }
    }
}
