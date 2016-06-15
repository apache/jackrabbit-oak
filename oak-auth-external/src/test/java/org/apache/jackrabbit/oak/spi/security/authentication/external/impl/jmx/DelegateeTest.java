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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class DelegateeTest extends AbstractJmxTest {

    private Delegatee delegatee;

    private static final String[] TEST_IDS = new String[] {
            TestIdentityProvider.ID_TEST_USER,
            TestIdentityProvider.ID_SECOND_USER,
            TestIdentityProvider.ID_WILDCARD_USER};

    @Before
    public void before() throws Exception {
        super.before();

        delegatee = createDelegatee(new TestIdentityProvider());
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

    int getBatchSize() {
        return 100;
    }

    private Delegatee createDelegatee(@Nonnull ExternalIdentityProvider idp) {
        return Delegatee.createInstance(getContentRepository(), getSecurityProvider(), new DefaultSyncHandler(syncConfig), idp, getBatchSize());
    }

    private static Root preventRootCommit(@Nonnull Delegatee delegatee) throws Exception {
        Field rootField = Delegatee.class.getDeclaredField("root");
        rootField.setAccessible(true);

        Root r = (Root) rootField.get(delegatee);
        r.refresh();
        rootField.set(delegatee, new ThrowingRoot(r));
        return r;
    }

    @Test
    public void testDoubleClose() {
        delegatee.close();
        delegatee.close();
    }

    @Test
    public void testSyncUsersBeforeSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);;

        String[] result = delegatee.syncUsers(TEST_IDS, false);
        assertResultMessages(result, ImmutableMap.of(
                TestIdentityProvider.ID_TEST_USER, "nsa",
                TestIdentityProvider.ID_SECOND_USER, "nsa",
                TestIdentityProvider.ID_WILDCARD_USER, "nsa"));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncUsersSaveError() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);
        sync(foreignIDP, TestIdentityProvider.ID_SECOND_USER, false);
        // don't sync ID_WILDCARD_USER

        Root r = preventRootCommit(delegatee);

        String[] result = delegatee.syncUsers(new String[] {
                TestIdentityProvider.ID_TEST_USER,
                TestIdentityProvider.ID_SECOND_USER,
                TestIdentityProvider.ID_WILDCARD_USER}, false);
        assertResultMessages(result, ImmutableMap.of(
                TestIdentityProvider.ID_TEST_USER, "ERR",
                TestIdentityProvider.ID_SECOND_USER, "for",
                TestIdentityProvider.ID_WILDCARD_USER, "nsa"));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncAllUsersBeforeSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);;

        String[] result = delegatee.syncAllUsers(false);
        assertResultMessages(result, ImmutableMap.<String,String>of());
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncAllUsersSaveError() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);
        sync(idp, TestIdentityProvider.ID_SECOND_USER, false);
        sync(new TestIdentityProvider.TestUser("third", idp.getName()), idp);
        sync(foreignIDP, TestIdentityProvider.ID_WILDCARD_USER, false);

        Root r = preventRootCommit(delegatee);;

        ImmutableMap<String, String> expected = ImmutableMap.<String, String>builder()
                .put(TestIdentityProvider.ID_TEST_USER, "ERR")
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
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);
        sync(idp, TestIdentityProvider.ID_SECOND_USER, false);
        sync(new TestIdentityProvider.TestUser("third", idp.getName()), idp);
        sync(foreignIDP, TestIdentityProvider.ID_WILDCARD_USER, false);

        Root r = preventRootCommit(delegatee);;

        ImmutableMap<String, String> expected = ImmutableMap.<String, String>builder()
                .put(TestIdentityProvider.ID_TEST_USER, "ERR")
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
        Root r = preventRootCommit(delegatee);;

        String[] result = delegatee.syncExternalUsers(new String[] {new ExternalIdentityRef("nonExisting", idp.getName()).getString()});
        assertResultMessages(result, "", "nsi");
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncForeignExternalUserSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);;

        String[] result = delegatee.syncExternalUsers(new String[] {new ExternalIdentityRef(TestIdentityProvider.ID_TEST_USER, foreignIDP.getName()).getString()});
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "for");
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncThrowingExternalUserSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);;

        String[] result = delegatee.syncExternalUsers(new String[] {new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()});
        assertResultMessages(result, TestIdentityProvider.ID_EXCEPTION, "ERR");
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncExternalUsersSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);;

        List<String> externalIds = new ArrayList();
        for (String id : TEST_IDS) {
                externalIds.add(new ExternalIdentityRef(id, idp.getName()).getString());
        }
        String[] result = delegatee.syncExternalUsers(externalIds.toArray(new String[externalIds.size()]));
        assertResultMessages(result, ImmutableMap.of(
                TestIdentityProvider.ID_TEST_USER, "ERR",
                TestIdentityProvider.ID_SECOND_USER, "ERR",
                TestIdentityProvider.ID_WILDCARD_USER, "ERR"));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncAllExternalUsersSaveError() throws Exception {
        Root r = preventRootCommit(delegatee);;

        String[] result = delegatee.syncAllExternalUsers();
        assertResultMessages(result, ImmutableMap.of(
                TestIdentityProvider.ID_TEST_USER, "ERR",
                TestIdentityProvider.ID_SECOND_USER, "ERR",
                TestIdentityProvider.ID_WILDCARD_USER, "ERR"));
        assertFalse(r.hasPendingChanges());
    }


    @Test(expected = SyncRuntimeException.class)
    public void testSyncAllExternalUsersThrowingIDP() {
        Delegatee dg = createDelegatee(new TestIdentityProvider("throwing") {

            @Nonnull
            @Override
            public Iterator<ExternalUser> listUsers() throws ExternalIdentityException {
                throw new ExternalIdentityException();
            }
        });

        dg.syncAllExternalUsers();
    }

    @Test
    public void testPurgeOrphanedSaveError() throws Exception {
        sync(new TestIdentityProvider.TestUser("third", idp.getName()), idp);
        sync(new TestIdentityProvider.TestUser("forth", idp.getName()), idp);
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);

        Root r = preventRootCommit(delegatee);;

        String[] result = delegatee.purgeOrphanedUsers();
        assertResultMessages(result, ImmutableMap.of(
                "third", "ERR",
                "forth", "ERR"));
        assertFalse(r.hasPendingChanges());
    }

    private static final class ThrowingRoot implements Root {

        private Root base;

        private ThrowingRoot(@Nonnull Root base) {
            this.base = base;
        }

        @Override
        public boolean move(String srcAbsPath, String destAbsPath) {
            return base.move(srcAbsPath, destAbsPath);
        }

        @Nonnull
        @Override
        public Tree getTree(@Nonnull String path) {
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
            commit(ImmutableMap.<String, Object>of());
        }

        @Override
        public void commit(@Nonnull Map<String, Object> info) throws CommitFailedException {
            throw new CommitFailedException(CommitFailedException.OAK, 0, "failed");
        }

        @Override
        public boolean hasPendingChanges() {
            return base.hasPendingChanges();
        }

        @Nonnull
        @Override
        public QueryEngine getQueryEngine() {
            return base.getQueryEngine();
        }

        @Nonnull
        @Override
        public Blob createBlob(@Nonnull InputStream stream) throws IOException {
            return base.createBlob(stream);
        }

        @CheckForNull
        @Override
        public Blob getBlob(@Nonnull String reference) {
            return base.getBlob(reference);
        }

        @Nonnull
        @Override
        public ContentSession getContentSession() {
            return base.getContentSession();
        }
    }
}