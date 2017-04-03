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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.GroupAction;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class UserImporterTest extends AbstractSecurityTest implements UserConstants {

    private static final String TEST_USER_ID = "uid";

    private TestAction testAction;
    AuthorizableActionProvider actionProvider = new AuthorizableActionProvider() {
        @Nonnull
        @Override
        public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
            return (testAction == null) ? ImmutableList.<AuthorizableAction>of() : ImmutableList.of(testAction);
        }
    };
    private User testUser;

    UserImporter importer;

    @Override
    public void before() throws Exception {
        super.before();

        testUser = getTestUser();
        importer = new UserImporter(getImportConfig());
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    ConfigurationParameters getImportConfig() {
        return getSecurityConfigParameters().getConfigValue(UserConfiguration.NAME, ConfigurationParameters.EMPTY);
    }

    String getImportBehavior() {
        return ImportBehavior.NAME_IGNORE;
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userParams = ConfigurationParameters.of(
                UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider,
                ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior()
        );
        return ConfigurationParameters.of(UserConfiguration.NAME, userParams);
    }

    Session mockJackrabbitSession() throws Exception {
        JackrabbitSession s = Mockito.mock(JackrabbitSession.class);
        when(s.getUserManager()).thenReturn(getUserManager(root));
        return s;
    }

    boolean isWorkspaceImport() {
        return false;
    }


    boolean init() throws Exception {
        return init(false);
    }

    boolean init(boolean createAction) throws Exception {
        if (createAction) {
            testAction = new TestAction();
        }
        return importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider());
    }

    private Tree createUserTree() {
        Tree folder = root.getTree(getUserConfiguration().getParameters().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH));
        Tree userTree = folder.addChild("userTree");
        userTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
        userTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID(TEST_USER_ID));
        return userTree;
    }

    private Tree createGroupTree() throws Exception {
        String groupPath = getUserConfiguration().getParameters().getConfigValue(PARAM_GROUP_PATH, DEFAULT_GROUP_PATH);

        NodeUtil node = new NodeUtil(root.getTree(PathUtils.ROOT_PATH));
        NodeUtil groupRoot = node.getOrAddTree(PathUtils.relativize(PathUtils.ROOT_PATH, groupPath), NT_REP_AUTHORIZABLE_FOLDER);

        return groupRoot.addChild("testGroup", NT_REP_GROUP).getTree();
    }

    private PropInfo createPropInfo(@Nonnull String name, final String value) {
        return new PropInfo(name, PropertyType.STRING, new TextValue() {
            @Override
            public String getString() {
                return value;
            }

            @Override
            public Value getValue(int targetType) throws RepositoryException {
                return getValueFactory(root).createValue(value, targetType);
            }

            @Override
            public void dispose() {
                //nop
            }
        });
    }

    private PropertyDefinition mockPropertyDefinition(@Nonnull String declaringNt, boolean mv) throws Exception {
        PropertyDefinition def = Mockito.mock(PropertyDefinition.class);
        when(def.isMultiple()).thenReturn(mv);
        when(def.getDeclaringNodeType()).thenReturn(ReadOnlyNodeTypeManager.getInstance(root, getNamePathMapper()).getNodeType(declaringNt));
        return def;
    }

    //---------------------------------------------------------------< init >---
    @Test
    public void testInitNoJackrabbitSession() throws Exception {
        Session s = Mockito.mock(Session.class);
        assertFalse(importer.init(s, root, getNamePathMapper(), false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test(expected = IllegalStateException.class)
    public void testInitAlreadyInitialized() throws Exception {
        init();
        importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider());
    }

    @Test
    public void testInitImportUUIDBehaviorRemove() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider()));
    }


    @Test
    public void testInitImportUUIDBehaviorReplace() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testInitImportUUIDBehaviorThrow() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testInitImportUUIDBehaviourCreateNew() throws Exception {
        assertFalse(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test(expected = IllegalStateException.class)
    public void testHandlePropInfoNotInitialized() throws Exception {
        importer.handlePropInfo(createUserTree(), Mockito.mock(PropInfo.class), Mockito.mock(PropertyDefinition.class));
    }

    //-----------------------------------------------------< handlePropInfo >---

    @Test
    public void testHandlePropInfoParentNotAuthorizable() throws Exception {
        init();
        assertFalse(importer.handlePropInfo(root.getTree("/"), Mockito.mock(PropInfo.class), Mockito.mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandleAuthorizableId() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, TEST_USER_ID), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));
        assertEquals(TEST_USER_ID, userTree.getProperty(REP_AUTHORIZABLE_ID).getValue(Type.STRING));
        assertEquals(userTree.getPath(), getUserManager(root).getAuthorizable(TEST_USER_ID).getPath());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testHandleAuthorizableIdMismatch() throws Exception {
        init();
        Tree userTree = createUserTree();
        importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, "mismatch"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false));
    }

    @Test(expected = AuthorizableExistsException.class)
    public void testHandleAuthorizableIdConflictExisting() throws Exception {
        init();
        Tree userTree = createUserTree();
        importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, testUser.getID()), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false));
    }

    @Test
    public void testHandleAuthorizableIdMvPropertyDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, TEST_USER_ID), mockPropertyDefinition(NT_REP_AUTHORIZABLE, true)));
        assertNull(userTree.getProperty(REP_AUTHORIZABLE_ID));
    }

    @Test
    public void testHandleAuthorizableIdOtherDeclNtDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, TEST_USER_ID), mockPropertyDefinition(NT_REP_AUTHORIZABLE_FOLDER, false)));
        assertNull(userTree.getProperty(REP_AUTHORIZABLE_ID));
    }

    @Test
    public void testHandleAuthorizableIdDeclNtDefSubtype() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, TEST_USER_ID), mockPropertyDefinition(NT_REP_USER, false)));
    }

    //--------------------------------------------------< processReferences >---

    @Test(expected = IllegalStateException.class)
    public void testProcessReferencesNotInitialized() throws Exception {
        importer.processReferences();
    }

    //------------------------------------------------< propertiesCompleted >---

    @Test
    public void testPropertiesCompletedClearsCache() throws Exception {
        Tree userTree = createUserTree();
        Tree cacheTree = userTree.addChild(CacheConstants.REP_CACHE);
        cacheTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, CacheConstants.NT_REP_CACHE);

        importer.propertiesCompleted(cacheTree);
        assertFalse(cacheTree.exists());
        assertFalse(userTree.hasChild(CacheConstants.REP_CACHE));
    }

    @Test
    public void testPropertiesCompletedParentNotAuthorizable() throws Exception {
        init();
        importer.propertiesCompleted(root.getTree("/"));
    }

    @Test
    public void testPropertiesCompletedIdMissing() throws Exception {
        init();
        Tree userTree = createUserTree();
        importer.propertiesCompleted(userTree);

        assertTrue(userTree.hasProperty(REP_AUTHORIZABLE_ID));
    }

    @Test
    public void testPropertiesCompletedIdPresent() throws Exception {
        init();
        testAction = new TestAction();

        Tree userTree = createUserTree();
        userTree.setProperty(REP_AUTHORIZABLE_ID, "userId");

        importer.propertiesCompleted(userTree);

        // property must not be touched
        assertEquals("userId", userTree.getProperty(REP_AUTHORIZABLE_ID).getValue(Type.STRING));
    }

    @Test
    public void testPropertiesCompletedNewUser() throws Exception {
        init(true);
        importer.propertiesCompleted(createUserTree());
        testAction.checkMethods("onCreate-User");
    }

    @Test
    public void testPropertiesCompletedNewGroup() throws Exception {
        Tree groupTree = createGroupTree();

        init(true);
        importer.propertiesCompleted(groupTree);
        testAction.checkMethods("onCreate-Group");
    }

    @Test
    public void testPropertiesCompletedExistingUser() throws Exception {
        init(true);
        importer.propertiesCompleted(root.getTree(testUser.getPath()));
        testAction.checkMethods();
    }

    //--------------------------------------------------------------------------

    private final class TestAction implements AuthorizableAction, GroupAction {

        private List<String> methodCalls = new ArrayList();

        private void clear() {
            methodCalls.clear();
        }

        private void checkMethods(String... expected) {
            assertEquals(ImmutableList.copyOf(expected), methodCalls);
        }

        @Override
        public void init(SecurityProvider securityProvider, ConfigurationParameters config) {
        }

        @Override
        public void onCreate(Group group, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            methodCalls.add("onCreate-Group");
        }

        @Override
        public void onCreate(User user, String password, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            methodCalls.add("onCreate-User");
        }

        @Override
        public void onRemove(Authorizable authorizable, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            methodCalls.add("onRemove");
        }

        @Override
        public void onPasswordChange(User user, String newPassword, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            methodCalls.add("onPasswordChange");
        }

        @Override
        public void onMemberAdded(Group group, Authorizable member, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            methodCalls.add("onMemberAdded");
        }

        @Override
        public void onMembersAdded(Group group, Iterable<String> memberIds, Iterable<String> failedIds, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            methodCalls.add("onMembersAdded");
        }

        @Override
        public void onMembersAddedContentId(Group group, Iterable<String> memberContentIds, Iterable<String> failedIds, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            methodCalls.add("onMembersAddedContentId");
        }

        @Override
        public void onMemberRemoved(Group group, Authorizable member, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            methodCalls.add("onMemberRemoved");
        }

        @Override
        public void onMembersRemoved(Group group, Iterable<String> memberIds, Iterable<String> failedIds, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            methodCalls.add("onMembersRemoved");
        }
    }
}