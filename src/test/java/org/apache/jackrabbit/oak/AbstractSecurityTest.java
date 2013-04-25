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

import java.util.UUID;
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
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.RegistrationEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.junit.After;
import org.junit.Before;

/**
 * AbstractOakTest is the base class for oak test execution.
 */
public abstract class AbstractSecurityTest {

    private ContentRepository contentRepository;
    private UserManager userManager;
    private User testUser;
    private PrivilegeManager privMgr;

    protected NamePathMapper namePathMapper = NamePathMapper.DEFAULT;
    protected SecurityProvider securityProvider;
    protected ContentSession adminSession;
    protected Root root;

    @Before
    public void before() throws Exception {
        contentRepository = new Oak()
                .with(new InitialContent())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider())
                .with(new RegistrationEditorProvider())
                .with(getSecurityProvider())
                .createContentRepository();

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
            adminSession.close();
            Configuration.setConfiguration(null);
        }
    }

    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = new SecurityProviderImpl(getSecurityConfigParameters());
        }
        return securityProvider;
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
        String adminId = UserUtility.getAdminId(getUserConfiguration().getConfigurationParameters());
        return new SimpleCredentials(adminId, adminId.toCharArray());
    }

    protected NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    protected UserConfiguration getUserConfiguration() {
        return getSecurityProvider().getUserConfiguration();
    }

    protected UserManager getUserManager() {
        if (userManager == null) {
            userManager = getUserConfiguration().getUserManager(root, namePathMapper);
        }
        return userManager;
    }

    protected PrincipalManager getPrincipalManager() {
        return getSecurityProvider().getPrincipalConfiguration().getPrincipalManager(root, getNamePathMapper());
    }
    
    protected JackrabbitAccessControlManager getAccessControlManager(Root root) {
        AccessControlManager acMgr = securityProvider.getAccessControlConfiguration().getAccessControlManager(root, NamePathMapper.DEFAULT);
        if (acMgr instanceof JackrabbitAccessControlManager) {
            return (JackrabbitAccessControlManager) acMgr;
        } else {
            throw new UnsupportedOperationException("Expected JackrabbitAccessControlManager found " + acMgr.getClass());
        }
    }

    protected Privilege[] privilegesFromNames(String... privilegeNames) throws RepositoryException {
        Privilege[] privs = new Privilege[privilegeNames.length];
        for (int i = 0; i < privilegeNames.length; i++) {
            privs[i] = getPrivilegeManager().getPrivilege(privilegeNames[i]);
        }
        return privs;
    }

    protected PrivilegeManager getPrivilegeManager() {
        if (privMgr == null) {
            privMgr = getSecurityProvider().getPrivilegeConfiguration().getPrivilegeManager(root, getNamePathMapper());
        }
        return privMgr;
    }

    protected ValueFactory getValueFactory() {
        return new ValueFactoryImpl(root.getBlobFactory(), getNamePathMapper());
    }

    protected User getTestUser() throws Exception {
        if (testUser == null) {
            String uid = "testUser" + UUID.randomUUID();
            testUser = getUserManager().createUser(uid, uid);
            root.commit();
        }
        return testUser;
    }

    protected ContentSession createTestSession() throws Exception {
        String uid = getTestUser().getID();
        return login(new SimpleCredentials(uid, uid.toCharArray()));
    }
}
