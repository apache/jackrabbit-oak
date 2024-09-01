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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.PropertyDefinition;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public abstract class UserImporterBaseTest extends AbstractSecurityTest implements UserConstants {

    static final String TEST_USER_ID = "uid";
    private static final String TEST_GROUP_ID = "gid";

    AuthorizableAction testAction;
    AuthorizableActionProvider actionProvider = new AuthorizableActionProvider() {
        @NotNull
        @Override
        public List<? extends AuthorizableAction> getAuthorizableActions(@NotNull SecurityProvider securityProvider) {
            return (testAction == null) ? ImmutableList.of() : ImmutableList.of(testAction);
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

    @NotNull
    private ConfigurationParameters getImportConfig() {
        return getSecurityConfigParameters().getConfigValue(UserConfiguration.NAME, ConfigurationParameters.EMPTY);
    }

    @NotNull
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

    @NotNull
    Session mockJackrabbitSession() throws Exception {
        JackrabbitSession s = mock(JackrabbitSession.class);
        when(s.getUserManager()).thenReturn(getUserManager(root));
        return s;
    }

    boolean isWorkspaceImport() {
        return false;
    }

    boolean isAutosave() {
        return false;
    }

    boolean init() throws Exception {
        return init(false);
    }

    boolean init(boolean createAction, Class<?>... extraInterfaces) throws Exception {
        if (createAction) {
            if (extraInterfaces.length > 0) {
                testAction = mock(AuthorizableAction.class, withSettings().extraInterfaces(extraInterfaces));
            } else {
                testAction = mock(AuthorizableAction.class);
            }
        }
        return importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, refTracker, getSecurityProvider());
    }

    @NotNull
    Tree createUserTree() {
        Tree folder = root.getTree(getUserConfiguration().getParameters().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH));
        Tree userTree = folder.addChild("userTree");
        userTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
        userTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID(TEST_USER_ID));
        return userTree;
    }

    @NotNull
    Tree createSystemUserTree() {
        Tree folder = root.getTree(getUserConfiguration().getParameters().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH));
        Tree userTree = folder.addChild("systemUserTree");
        userTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_SYSTEM_USER, Type.NAME);
        userTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID(TEST_USER_ID));
        return userTree;
    }

    @NotNull
    Tree createGroupTree() throws Exception {
        String groupPath = getUserConfiguration().getParameters().getConfigValue(PARAM_GROUP_PATH, DEFAULT_GROUP_PATH);

        Tree node = root.getTree(PathUtils.ROOT_PATH);
        node = Utils.getOrAddTree(node, PathUtils.relativize(PathUtils.ROOT_PATH, groupPath), NT_REP_AUTHORIZABLE_FOLDER);

        Tree groupTree = TreeUtil.addChild(node,"testGroup", NT_REP_GROUP);
        groupTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID(TEST_GROUP_ID));
        return groupTree;
    }

    @NotNull
    PropInfo createPropInfo(@Nullable String name, final String... values) {
        List<TextValue> txtValues = new ArrayList<>();
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

    @NotNull
    PropertyDefinition mockPropertyDefinition(@NotNull String declaringNt, boolean mv) throws Exception {
        PropertyDefinition def = mock(PropertyDefinition.class);
        when(def.isMultiple()).thenReturn(mv);
        when(def.getDeclaringNodeType()).thenReturn(ReadOnlyNodeTypeManager.getInstance(root, getNamePathMapper()).getNodeType(declaringNt));
        return def;
    }

    @NotNull
    NodeInfo createNodeInfo(@NotNull String name, @NotNull String primaryTypeName) {
        return new NodeInfo(name, primaryTypeName, ImmutableList.of(), null);
    }
}
