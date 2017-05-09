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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class UserImportWithActionsTest extends AbstractImportTest {

    private final TestActionProvider actionProvider = new TestActionProvider();

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }

    @Override
    protected String getTargetPath() {
        throw new UnsupportedOperationException();
    }

    private void setAuthorizableActions(AuthorizableAction action) {
        actionProvider.addAction(action);
    }

    @Override
    protected ConfigurationParameters getConfigurationParameters() {
        Map<String, Object> userParams = new HashMap<String, Object>();
        userParams.put(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider);
        return ConfigurationParameters.of(UserConfiguration.NAME, ConfigurationParameters.of(userParams));
    }

    @Test
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

        doImport(USERPATH, xml);
        assertEquals(testAction.id, "t");
        assertEquals(testAction.pw, "pw");
    }

    @Test
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

        doImport(GROUPPATH, xml);
        assertEquals(testAction.id, "g");
        assertNull(testAction.pw);
    }

    @Test
    public void testAccessControlActionExecutionForUser() throws Exception {
        AccessControlAction a1 = new AccessControlAction();
        a1.init(securityProvider, ConfigurationParameters.of(AccessControlAction.USER_PRIVILEGE_NAMES, new String[] {Privilege.JCR_ALL}));

        setAuthorizableActions(a1);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>tPrincipal</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable a = getUserManager().getAuthorizable("t");
        assertNotNull(a);
        assertFalse(a.isGroup());

        AccessControlManager acMgr = getImportSession().getAccessControlManager();
        AccessControlPolicy[] policies = acMgr.getPolicies(a.getPath());
        assertNotNull(policies);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof AccessControlList);

        AccessControlEntry[] aces = ((AccessControlList) policies[0]).getAccessControlEntries();
        assertEquals(1, aces.length);
        assertEquals("tPrincipal", aces[0].getPrincipal().getName());
    }

    @Test
    public void testAccessControlActionExecutionForUser2() throws Exception {
        AccessControlAction a1 = new AccessControlAction();
        a1.init(securityProvider, ConfigurationParameters.of(AccessControlAction.USER_PRIVILEGE_NAMES, new String[] {Privilege.JCR_ALL}));
        setAuthorizableActions(a1);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>tPrincipal</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable a = getUserManager().getAuthorizable("t");
        assertNotNull(a);
        assertFalse(a.isGroup());

        AccessControlManager acMgr = getImportSession().getAccessControlManager();
        AccessControlPolicy[] policies = acMgr.getPolicies(a.getPath());
        assertNotNull(policies);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof AccessControlList);

        AccessControlEntry[] aces = ((AccessControlList) policies[0]).getAccessControlEntries();
        assertEquals(1, aces.length);
        assertEquals("tPrincipal", aces[0].getPrincipal().getName());
    }

    @Test
    public void testAccessControlActionExecutionForGroup() throws Exception {
        AccessControlAction a1 = new AccessControlAction();
        a1.init(securityProvider, ConfigurationParameters.of(AccessControlAction.GROUP_PRIVILEGE_NAMES, new String[] {Privilege.JCR_READ}));
        setAuthorizableActions(a1);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>gPrincipal</sv:value></sv:property>" +
                "</sv:node>";

        doImport(GROUPPATH, xml);

        Authorizable a = getUserManager().getAuthorizable("g");
        assertNotNull(a);
        assertTrue(a.isGroup());

        AccessControlManager acMgr = getImportSession().getAccessControlManager();
        AccessControlPolicy[] policies = acMgr.getPolicies(a.getPath());
        assertNotNull(policies);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof AccessControlList);

        AccessControlEntry[] aces = ((AccessControlList) policies[0]).getAccessControlEntries();
        assertEquals(1, aces.length);
        assertEquals("gPrincipal", aces[0].getPrincipal().getName());
    }

    @Test
    public void testAccessControlActionExecutionForSystemUser() throws Exception {
        AccessControlAction a1 = new AccessControlAction();
        a1.init(securityProvider, ConfigurationParameters.of(AccessControlAction.USER_PRIVILEGE_NAMES, new String[] {Privilege.JCR_ALL}));

        setAuthorizableActions(a1);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>tSystemPrincipal</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable a = getUserManager().getAuthorizable("t");
        assertNotNull(a);
        assertFalse(a.isGroup());
        assertTrue(((User) a).isSystemUser());

        AccessControlManager acMgr = getImportSession().getAccessControlManager();
        AccessControlPolicy[] policies = acMgr.getPolicies(a.getPath());
        assertNotNull(policies);
        assertEquals(0, policies.length);
    }

    private final class TestActionProvider implements AuthorizableActionProvider {

        private final List<AuthorizableAction> actions = new ArrayList();

        private void addAction(AuthorizableAction action) {
            actions.add(action);
        }
        @Nonnull
        @Override
        public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
            return actions;
        }
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
        public void onRemove(Authorizable authorizable, Root root, NamePathMapper namePathMapper) {
            // ignore
        }

        @Override
        public void onPasswordChange(User user, String newPassword, Root root, NamePathMapper namePathMapper) {
            pw = newPassword;
        }
    }
}
