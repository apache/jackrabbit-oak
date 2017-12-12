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
package org.apache.jackrabbit.oak;

import static com.google.common.collect.Lists.newArrayList;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.impl.RootProviderService;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeProviderService;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.plugins.version.VersionHook;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.junit.After;
import org.junit.Before;

/**
 * AbstractOakTest is the base class for oak test execution.
 */
public abstract class AbstractSecurityTest {

    private ContentRepository contentRepository;
    private UserManager userManager;
    private User testUser;

    protected NamePathMapper namePathMapper = NamePathMapper.DEFAULT;
    protected SecurityProvider securityProvider;
    protected ContentSession adminSession;
    protected Root root;

    protected QueryEngineSettings querySettings;
    private final RootProvider rootProvider = new RootProviderService(); 
    private final TreeProvider treeProvider = new TreeProviderService();

    @Before
    public void before() throws Exception {
        Oak oak = new Oak()
                .with(new InitialContent())
                .with(new VersionHook())
                .with(JcrConflictHandler.createJcrConflictHandler())
                .with(new NamespaceEditorProvider())
                .with(new ReferenceEditorProvider())
                .with(new ReferenceIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider())
                .with(new TypeEditorProvider())
                .with(new ConflictValidatorProvider())
                .with(getQueryEngineSettings())
                .with(getSecurityProvider());
        withEditors(oak);
        contentRepository = oak.createContentRepository();

        adminSession = login(getAdminCredentials());
        root = adminSession.getLatestRoot();

        Configuration.setConfiguration(getConfiguration());
    }

    @After
    public void after() throws Exception {
        try {
            if (testUser != null) {
                testUser.remove();
                root.commit();
            }
        } finally {
            if (adminSession != null) {
                adminSession.close();
            }
            Configuration.setConfiguration(null);
        }
    }

    protected ContentRepository getContentRepository() {
        return contentRepository;
    }

    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = initSecurityProvider();
        }
        return securityProvider;
    }

    protected SecurityProvider initSecurityProvider() {
        return new SecurityProviderBuilder().with(getSecurityConfigParameters()).build();
    }

    protected Oak withEditors(Oak oak) {
        return oak;
    }

    protected QueryEngineSettings getQueryEngineSettings() {
        if (querySettings == null) {
            querySettings = new QueryEngineSettings();
            querySettings.setFailTraversal(true);
        }
        return querySettings;
    }

    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.EMPTY;
    }

    protected Configuration getConfiguration() {
        return ConfigurationUtil.getDefaultConfiguration(getSecurityConfigParameters());
    }

    protected ContentSession login(@Nullable Credentials credentials)
            throws LoginException, NoSuchWorkspaceException {
        return contentRepository.login(credentials, null);
    }

    protected Credentials getAdminCredentials() {
        String adminId = UserUtil.getAdminId(getUserConfiguration().getParameters());
        return new SimpleCredentials(adminId, adminId.toCharArray());
    }

    protected NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    protected UserConfiguration getUserConfiguration() {
        return getConfig(UserConfiguration.class);
    }

    protected UserManager getUserManager(Root root) {
        if (this.root == root) {
            if (userManager == null) {
                userManager = getConfig(UserConfiguration.class).getUserManager(root, getNamePathMapper());
            }
            return userManager;
        } else {
            return getConfig(UserConfiguration.class).getUserManager(root, getNamePathMapper());
        }
    }

    protected PrincipalManager getPrincipalManager(Root root) {
        return getConfig(PrincipalConfiguration.class).getPrincipalManager(root, getNamePathMapper());
    }
    
    protected JackrabbitAccessControlManager getAccessControlManager(Root root) {
        AccessControlManager acMgr = getConfig(AuthorizationConfiguration.class).getAccessControlManager(root, NamePathMapper.DEFAULT);
        if (acMgr instanceof JackrabbitAccessControlManager) {
            return (JackrabbitAccessControlManager) acMgr;
        } else {
            throw new UnsupportedOperationException("Expected JackrabbitAccessControlManager found " + acMgr.getClass());
        }
    }

    protected Privilege[] privilegesFromNames(String... privilegeNames) throws RepositoryException {
        return privilegesFromNames(Arrays.asList(privilegeNames));
    }

    protected Privilege[] privilegesFromNames(Iterable<String> privilegeNames) throws RepositoryException {
        PrivilegeManager manager = getPrivilegeManager(root);
        List<Privilege> privs = newArrayList();
        for (String name : privilegeNames) {
            privs.add(manager.getPrivilege(name));
        }
        return privs.toArray(new Privilege[privs.size()]);
    }

    protected PrivilegeManager getPrivilegeManager(Root root) {
        return getConfig(PrivilegeConfiguration.class).getPrivilegeManager(root, getNamePathMapper());
    }

    protected ValueFactory getValueFactory() {
        return getValueFactory(root);
    }

    protected ValueFactory getValueFactory(@Nonnull Root root) {
        return new ValueFactoryImpl(root, getNamePathMapper());
    }

    protected long waitForSystemTimeIncrement(long old) {
        while (old == System.currentTimeMillis()) {
            // wait for system timer to move
        }
        return System.currentTimeMillis();
    }

    protected User getTestUser() throws Exception {
        if (testUser == null) {
            String uid = "testUser" + UUID.randomUUID();
            testUser = getUserManager(root).createUser(uid, uid);
            root.commit();
        }
        return testUser;
    }

    protected ContentSession createTestSession() throws Exception {
        String uid = getTestUser().getID();
        return login(new SimpleCredentials(uid, uid.toCharArray()));
    }

    protected <T> T getConfig(Class<T> configClass) {
        return getSecurityProvider().getConfiguration(configClass);
    }

    public RootProvider getRootProvider() {
        return rootProvider;
    }

    public TreeProvider getTreeProvider() {
        return treeProvider;
    }
}
