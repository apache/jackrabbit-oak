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
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
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
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public abstract class UserImporterBaseTest extends AbstractSecurityTest implements UserConstants {

    static final String TEST_USER_ID = "uid";
    static final String TEST_GROUP_ID = "gid";

    TestAction testAction;
    AuthorizableActionProvider actionProvider = new AuthorizableActionProvider() {
        @Nonnull
        @Override
        public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
            return (testAction == null) ? ImmutableList.<AuthorizableAction>of() : ImmutableList.of(testAction);
        }
    };

    User testUser;

    ReferenceChangeTracker refTracker = new ReferenceChangeTracker();

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
            refTracker.clear();
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
        return importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, refTracker, getSecurityProvider());
    }

    Tree createUserTree() {
        Tree folder = root.getTree(getUserConfiguration().getParameters().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH));
        Tree userTree = folder.addChild("userTree");
        userTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
        userTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID(TEST_USER_ID));
        return userTree;
    }

    Tree createSystemUserTree() {
        Tree folder = root.getTree(getUserConfiguration().getParameters().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH));
        Tree userTree = folder.addChild("systemUserTree");
        userTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_SYSTEM_USER, Type.NAME);
        userTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID(TEST_USER_ID));
        return userTree;
    }

    Tree createGroupTree() throws Exception {
        String groupPath = getUserConfiguration().getParameters().getConfigValue(PARAM_GROUP_PATH, DEFAULT_GROUP_PATH);

        NodeUtil node = new NodeUtil(root.getTree(PathUtils.ROOT_PATH));
        NodeUtil groupRoot = node.getOrAddTree(PathUtils.relativize(PathUtils.ROOT_PATH, groupPath), NT_REP_AUTHORIZABLE_FOLDER);

        Tree groupTree = groupRoot.addChild("testGroup", NT_REP_GROUP).getTree();
        groupTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID(TEST_GROUP_ID));
        return groupTree;
    }

    PropInfo createPropInfo(@Nonnull String name, final String... values) {
        List<TextValue> txtValues = Lists.newArrayList();
        for (final String v : values) {
            txtValues.add(new TextValue() {
                @Override
                public String getString() {
                    return v;
                }

                @Override
                public Value getValue(int targetType) throws RepositoryException {
                    return getValueFactory(root).createValue(v, targetType);
                }

                @Override
                public void dispose() {
                    //nop
                }
            });
        }
        return new PropInfo(name, PropertyType.STRING, txtValues);
    }

    PropertyDefinition mockPropertyDefinition(@Nonnull String declaringNt, boolean mv) throws Exception {
        PropertyDefinition def = Mockito.mock(PropertyDefinition.class);
        when(def.isMultiple()).thenReturn(mv);
        when(def.getDeclaringNodeType()).thenReturn(ReadOnlyNodeTypeManager.getInstance(root, getNamePathMapper()).getNodeType(declaringNt));
        return def;
    }

    NodeInfo createNodeInfo(@Nonnull String name, @Nonnull String primaryTypeName) {
        return new NodeInfo(name, primaryTypeName, ImmutableList.<String>of(), null);
    }

    //--------------------------------------------------------------------------

    final class TestAction implements AuthorizableAction, GroupAction {

        private List<String> methodCalls = new ArrayList();

        private void clear() {
            methodCalls.clear();
        }

        void checkMethods(String... expected) {
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