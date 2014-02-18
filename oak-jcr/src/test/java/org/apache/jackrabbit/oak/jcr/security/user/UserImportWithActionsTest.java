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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.junit.Ignore;

/**
 * UserImportTest...
 */
@Ignore("OAK-414") // TODO: OAK-414
public class UserImportWithActionsTest extends AbstractUserTest {

    private static final String USERPATH = UserConstants.DEFAULT_USER_PATH;
    private static final String GROUPPATH = UserConstants.DEFAULT_GROUP_PATH;

    private JackrabbitSession jrSession;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        // avoid collision with testing a-folders that may have been created
        // with another test (but not removed as user/groups got removed)
        String path = USERPATH + "/t";
        if (superuser.nodeExists(path)) {
            superuser.getNode(path).remove();
        }
        path = GROUPPATH + "/g";
        if (superuser.nodeExists(path)) {
            superuser.getNode(path).remove();
        }
        superuser.save();
        jrSession = (JackrabbitSession) superuser;
    }

    private void setAuthorizableActions(AuthorizableAction action) {
        // TODO clarify how to test AuthorizableActions in Oak
        // userMgr.setAuthorizableActions(new AuthorizableAction[] {testAction});
    }

    public void testActionExecutionForUser() throws Exception {
        TestAction testAction = new TestAction();
        setAuthorizableActions(testAction);

        // import user
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>pw</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>tPrincipal</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(USERPATH, xml);
            assertEquals(testAction.id, "t");
            assertEquals(testAction.pw, "pw");
        } finally {
            jrSession.refresh(false);
        }
    }

    public void testActionExecutionForGroup() throws Exception {
        TestAction testAction = new TestAction();
        setAuthorizableActions(testAction);

        // import group
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>gPrincipal</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(GROUPPATH, xml);
            assertEquals(testAction.id, "g");
            assertNull(testAction.pw);
        } finally {
            jrSession.refresh(false);
        }
    }

    public void testAccessControlActionExecutionForUser() throws Exception {
        AccessControlAction a1 = new AccessControlAction();
        //a1.setUserPrivilegeNames(Privilege.JCR_ALL);

        setAuthorizableActions(a1);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>tPrincipal</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(USERPATH, xml);

            Authorizable a = userMgr.getAuthorizable("t");
            assertNotNull(a);
            assertFalse(a.isGroup());

            AccessControlManager acMgr = jrSession.getAccessControlManager();
            AccessControlPolicy[] policies = acMgr.getPolicies(a.getPath());
            assertNotNull(policies);
            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof AccessControlList);

            AccessControlEntry[] aces = ((AccessControlList) policies[0]).getAccessControlEntries();
            assertEquals(1, aces.length);
            assertEquals("tPrincipal", aces[0].getPrincipal().getName());

        } finally {
            jrSession.refresh(false);
        }
    }

    public void testAccessControlActionExecutionForUser2() throws Exception {
        AccessControlAction a1 = new AccessControlAction();
        //a1.setUserPrivilegeNames(Privilege.JCR_ALL);

        setAuthorizableActions(a1);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>tPrincipal</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(USERPATH, xml);

            Authorizable a = userMgr.getAuthorizable("t");
            assertNotNull(a);
            assertFalse(a.isGroup());

            AccessControlManager acMgr = jrSession.getAccessControlManager();
            AccessControlPolicy[] policies = acMgr.getPolicies(a.getPath());
            assertNotNull(policies);
            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof AccessControlList);

            AccessControlEntry[] aces = ((AccessControlList) policies[0]).getAccessControlEntries();
            assertEquals(1, aces.length);
            assertEquals("tPrincipal", aces[0].getPrincipal().getName());

        } finally {
            jrSession.refresh(false);
        }
    }

    public void testAccessControlActionExecutionForGroup() throws Exception {
        AccessControlAction a1 = new AccessControlAction();
        //a1.setGroupPrivilegeNames(Privilege.JCR_READ);

        setAuthorizableActions(a1);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>gPrincipal</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(GROUPPATH, xml);

            Authorizable a = userMgr.getAuthorizable("g");
            assertNotNull(a);
            assertTrue(a.isGroup());

            AccessControlManager acMgr = jrSession.getAccessControlManager();
            AccessControlPolicy[] policies = acMgr.getPolicies(a.getPath());
            assertNotNull(policies);
            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof AccessControlList);

            AccessControlEntry[] aces = ((AccessControlList) policies[0]).getAccessControlEntries();
            assertEquals(1, aces.length);
            assertEquals("gPrincipal", aces[0].getPrincipal().getName());

        } finally {
            jrSession.refresh(false);
        }
    }

    private void doImport(String parentPath, String xml) throws IOException, RepositoryException {
        InputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
        superuser.importXML(parentPath, in, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
    }

    private final class TestAction implements AuthorizableAction {
        private String id;
        private String pw;

        @Override
        public void init(SecurityProvider securityProvider, ConfigurationParameters config) {
        }

        @Override
        public void onCreate(Group group, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            id = group.getID();
        }

        @Override
        public void onCreate(User user, String password, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            id = user.getID();
            pw = password;
        }

        @Override
        public void onRemove(Authorizable authorizable, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            // ignore
        }

        @Override
        public void onPasswordChange(User user, String newPassword, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            pw = newPassword;
        }
    }
}
