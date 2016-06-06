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
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.retention.RetentionManager;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

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
        return Delegatee.createInstance(REPOSITORY, new DefaultSyncHandler(syncConfig), idp, getBatchSize());
    }

    private static Session preventSessionSave(@Nonnull Delegatee delegatee) throws Exception {
        Field sessionField = Delegatee.class.getDeclaredField("systemSession");
        sessionField.setAccessible(true);

        JackrabbitSession s = (JackrabbitSession) sessionField.get(delegatee);
        s.refresh(false);
        sessionField.set(delegatee, new ThrowingSession(s));
        return s;
    }

    @Test
    public void testDoubleClose() {
        delegatee.close();
        delegatee.close();
    }

    @Test
    public void testSyncUsersBeforeSaveError() throws Exception {
        Session s = preventSessionSave(delegatee);;

        String[] result = delegatee.syncUsers(TEST_IDS, false);
        assertResultMessages(result, ImmutableMap.of(
                TestIdentityProvider.ID_TEST_USER, "nsa",
                TestIdentityProvider.ID_SECOND_USER, "nsa",
                TestIdentityProvider.ID_WILDCARD_USER, "nsa"));
        assertFalse(s.hasPendingChanges());
    }

    @Test
    public void testSyncUsersSaveError() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);
        sync(foreignIDP, TestIdentityProvider.ID_SECOND_USER, false);
        // don't sync ID_WILDCARD_USER

        Session s = preventSessionSave(delegatee);

        String[] result = delegatee.syncUsers(new String[] {
                TestIdentityProvider.ID_TEST_USER,
                TestIdentityProvider.ID_SECOND_USER,
                TestIdentityProvider.ID_WILDCARD_USER}, false);
        assertResultMessages(result, ImmutableMap.of(
                TestIdentityProvider.ID_TEST_USER, "ERR",
                TestIdentityProvider.ID_SECOND_USER, "for",
                TestIdentityProvider.ID_WILDCARD_USER, "nsa"));
        assertFalse(s.hasPendingChanges());
    }

    @Test
    public void testSyncAllUsersBeforeSaveError() throws Exception {
        Session s = preventSessionSave(delegatee);;

        String[] result = delegatee.syncAllUsers(false);
        assertResultMessages(result, ImmutableMap.<String,String>of());
        assertFalse(s.hasPendingChanges());
    }

    @Test
    public void testSyncAllUsersSaveError() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);
        sync(idp, TestIdentityProvider.ID_SECOND_USER, false);
        sync(new TestIdentityProvider.TestUser("third", idp.getName()), idp);
        sync(foreignIDP, TestIdentityProvider.ID_WILDCARD_USER, false);

        Session s = preventSessionSave(delegatee);;

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
        assertFalse(s.hasPendingChanges());
    }

    @Test
    public void testSyncAllUsersPurgeSaveError() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);
        sync(idp, TestIdentityProvider.ID_SECOND_USER, false);
        sync(new TestIdentityProvider.TestUser("third", idp.getName()), idp);
        sync(foreignIDP, TestIdentityProvider.ID_WILDCARD_USER, false);

        Session s = preventSessionSave(delegatee);;

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
        assertFalse(s.hasPendingChanges());
    }

    @Test
    public void testSyncNonExistingExternalUserSaveError() throws Exception {
        Session s = preventSessionSave(delegatee);;

        String[] result = delegatee.syncExternalUsers(new String[] {new ExternalIdentityRef("nonExisting", idp.getName()).getString()});
        assertResultMessages(result, "", "nsi");
        assertFalse(s.hasPendingChanges());
    }

    @Test
    public void testSyncForeignExternalUserSaveError() throws Exception {
        Session s = preventSessionSave(delegatee);;

        String[] result = delegatee.syncExternalUsers(new String[] {new ExternalIdentityRef(TestIdentityProvider.ID_TEST_USER, foreignIDP.getName()).getString()});
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "for");
        assertFalse(s.hasPendingChanges());
    }

    @Test
    public void testSyncThrowingExternalUserSaveError() throws Exception {
        Session s = preventSessionSave(delegatee);;

        String[] result = delegatee.syncExternalUsers(new String[] {new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()});
        assertResultMessages(result, TestIdentityProvider.ID_EXCEPTION, "ERR");
        assertFalse(s.hasPendingChanges());
    }

    @Test
    public void testSyncExternalUsersSaveError() throws Exception {
        Session s = preventSessionSave(delegatee);;

        List<String> externalIds = new ArrayList();
        for (String id : TEST_IDS) {
                externalIds.add(new ExternalIdentityRef(id, idp.getName()).getString());
        }
        String[] result = delegatee.syncExternalUsers(externalIds.toArray(new String[externalIds.size()]));
        assertResultMessages(result, ImmutableMap.of(
                TestIdentityProvider.ID_TEST_USER, "ERR",
                TestIdentityProvider.ID_SECOND_USER, "ERR",
                TestIdentityProvider.ID_WILDCARD_USER, "ERR"));
        assertFalse(s.hasPendingChanges());
    }

    @Test
    public void testSyncAllExternalUsersSaveError() throws Exception {
        Session s = preventSessionSave(delegatee);;

        String[] result = delegatee.syncAllExternalUsers();
        assertResultMessages(result, ImmutableMap.of(
                TestIdentityProvider.ID_TEST_USER, "ERR",
                TestIdentityProvider.ID_SECOND_USER, "ERR",
                TestIdentityProvider.ID_WILDCARD_USER, "ERR"));
        assertFalse(s.hasPendingChanges());
    }


    @Test(expected = SyncRuntimeException.class)
    public void testSyncAllExternalUsersThrowingIDP() {
        Delegatee dg = Delegatee.createInstance(REPOSITORY, new DefaultSyncHandler(syncConfig), new TestIdentityProvider("throwing") {

            @Nonnull
            @Override
            public Iterator<ExternalUser> listUsers() throws ExternalIdentityException {
                throw new ExternalIdentityException();
            }
        }, getBatchSize());

        dg.syncAllExternalUsers();
    }

    @Test
    public void testPurgeOrphanedSaveError() throws Exception {
        sync(new TestIdentityProvider.TestUser("third", idp.getName()), idp);
        sync(new TestIdentityProvider.TestUser("forth", idp.getName()), idp);
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);

        Session s = preventSessionSave(delegatee);;

        String[] result = delegatee.purgeOrphanedUsers();
        assertResultMessages(result, ImmutableMap.of(
                "third", "ERR",
                "forth", "ERR"));
        assertFalse(s.hasPendingChanges());
    }

    private static final class ThrowingSession implements JackrabbitSession {

        private JackrabbitSession base;

        private ThrowingSession(@Nonnull JackrabbitSession session) {
            this.base = session;
        }
        @Override
        public boolean hasPermission(@Nonnull String s, @Nonnull String... strings) throws RepositoryException {
            return base.hasPermission(s, strings);
        }

        @Override
        public PrincipalManager getPrincipalManager() throws RepositoryException {
            return base.getPrincipalManager();
        }

        @Override
        public UserManager getUserManager() throws RepositoryException {
            return base.getUserManager();
        }

        @Override
        public Item getItemOrNull(String s) throws RepositoryException {
            return base.getItemOrNull(s);
        }

        @Override
        public Property getPropertyOrNull(String s) throws RepositoryException {
            return base.getPropertyOrNull(s);
        }

        @Override
        public Node getNodeOrNull(String s) throws RepositoryException {
            return getNodeOrNull(s);
        }

        @Override
        public Repository getRepository() {
            return base.getRepository();
        }

        @Override
        public String getUserID() {
            return base.getUserID();
        }

        @Override
        public String[] getAttributeNames() {
            return base.getAttributeNames();
        }

        @Override
        public Object getAttribute(String name) {
            return base.getAttribute(name);
        }

        @Override
        public Workspace getWorkspace() {
            return base.getWorkspace();
        }

        @Override
        public Node getRootNode() throws RepositoryException {
            return base.getRootNode();
        }

        @Override
        public Session impersonate(Credentials credentials) throws RepositoryException {
            return base.impersonate(credentials);
        }

        @Override
        public Node getNodeByUUID(String uuid) throws RepositoryException {
            return base.getNodeByUUID(uuid);
        }

        @Override
        public Node getNodeByIdentifier(String id) throws RepositoryException {
            return base.getNodeByIdentifier(id);
        }

        @Override
        public Item getItem(String absPath) throws RepositoryException {
            return base.getItem(absPath);
        }

        @Override
        public Node getNode(String absPath) throws RepositoryException {
            return base.getNode(absPath);
        }

        @Override
        public Property getProperty(String absPath) throws RepositoryException {
            return base.getProperty(absPath);
        }

        @Override
        public boolean itemExists(String absPath) throws RepositoryException {
            return base.itemExists(absPath);
        }

        @Override
        public boolean nodeExists(String absPath) throws RepositoryException {
            return base.nodeExists(absPath);
        }

        @Override
        public boolean propertyExists(String absPath) throws RepositoryException {
            return base.propertyExists(absPath);
        }

        @Override
        public void move(String srcAbsPath, String destAbsPath) throws RepositoryException {
            base.move(srcAbsPath, destAbsPath);
        }

        @Override
        public void removeItem(String absPath) throws RepositoryException {
            base.removeItem(absPath);
        }

        @Override
        public void save() throws RepositoryException {
            throw new RepositoryException();

        }

        @Override
        public void refresh(boolean keepChanges) throws RepositoryException {
            base.refresh(keepChanges);
        }

        @Override
        public boolean hasPendingChanges() throws RepositoryException {
            return base.hasPendingChanges();
        }

        @Override
        public ValueFactory getValueFactory() throws RepositoryException {
            return base.getValueFactory();
        }

        @Override
        public boolean hasPermission(String absPath, String actions) throws RepositoryException {
            return base.hasPermission(absPath, actions);
        }

        @Override
        public void checkPermission(String absPath, String actions) throws AccessControlException, RepositoryException {
            base.checkPermission(absPath, actions);
        }

        @Override
        public boolean hasCapability(String methodName, Object target, Object[] arguments) throws RepositoryException {
            return base.hasCapability(methodName, target, arguments);
        }

        @Override
        public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior) throws RepositoryException {
            return base.getImportContentHandler(parentAbsPath, uuidBehavior);
        }

        @Override
        public void importXML(String parentAbsPath, InputStream in, int uuidBehavior) throws IOException, RepositoryException {
            base.importXML(parentAbsPath, in, uuidBehavior);
        }

        @Override
        public void exportSystemView(String absPath, ContentHandler contentHandler, boolean skipBinary, boolean noRecurse) throws SAXException, RepositoryException {
            base.exportSystemView(absPath, contentHandler, skipBinary, noRecurse);
        }

        @Override
        public void exportSystemView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse) throws IOException, RepositoryException {
            base.exportSystemView(absPath, out, skipBinary, noRecurse);
        }

        @Override
        public void exportDocumentView(String absPath, ContentHandler contentHandler, boolean skipBinary, boolean noRecurse) throws SAXException, RepositoryException {
            base.exportDocumentView(absPath, contentHandler, skipBinary, noRecurse);
        }

        @Override
        public void exportDocumentView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse) throws IOException, RepositoryException {
            base.exportDocumentView(absPath, out, skipBinary, noRecurse);
        }

        @Override
        public void setNamespacePrefix(String prefix, String uri) throws RepositoryException {
            base.setNamespacePrefix(prefix, uri);
        }

        @Override
        public String[] getNamespacePrefixes() throws RepositoryException {
            return base.getNamespacePrefixes();
        }

        @Override
        public String getNamespaceURI(String prefix) throws RepositoryException {
            return base.getNamespaceURI(prefix);
        }

        @Override
        public String getNamespacePrefix(String uri) throws RepositoryException {
            return base.getNamespacePrefix(uri);
        }

        @Override
        public void logout() {
            base.logout();
        }

        @Override
        public boolean isLive() {
            return base.isLive();
        }

        @Override
        public void addLockToken(String lt) {
            base.addLockToken(lt);
        }

        @Override
        public String[] getLockTokens() {
            return base.getLockTokens();
        }

        @Override
        public void removeLockToken(String lt) {
            base.removeLockToken(lt);
        }

        @Override
        public AccessControlManager getAccessControlManager() throws RepositoryException {
            return base.getAccessControlManager();
        }

        @Override
        public RetentionManager getRetentionManager() throws RepositoryException {
            return base.getRetentionManager();
        }
    }
}