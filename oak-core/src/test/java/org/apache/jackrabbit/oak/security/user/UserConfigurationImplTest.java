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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.plugins.value.jcr.PartialValueFactory;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.PARAM_DEFAULT_DEPTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class UserConfigurationImplTest extends AbstractSecurityTest {

    private static final String USER_PATH = "/this/is/a/user/test";
    private static final String GROUP_PATH = "/this/is/a/group/test";
    private static final Integer DEFAULT_DEPTH = 10;
    private static final String IMPORT_BEHAVIOR = ImportBehavior.NAME_BESTEFFORT;
    private static final String HASH_ALGORITHM = "MD5";
    private static final Integer HASH_ITERATIONS = 500;
    private static final Integer SALT_SIZE = 6;
    private static final boolean SUPPORT_AUTOSAVE = true;
    private static final Integer MAX_AGE = 10;
    private static final boolean INITIAL_PASSWORD_CHANGE = true;
    private static final Integer PASSWORD_HISTORY_SIZE = 12;
    private static final boolean ENABLE_RFC7613_USERCASE_MAPPED_PROFILE = true;

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME, getParams());
    }

    @Test
    public void testActivate() {
        UserConfiguration userConfiguration = new UserConfigurationImpl(getSecurityProvider());
        context.registerInjectActivateService(userConfiguration, ImmutableMap.of(PARAM_DEFAULT_DEPTH, "8"));

        ConfigurationParameters params = userConfiguration.getParameters();
        assertEquals(8, params.getConfigValue(PARAM_DEFAULT_DEPTH, UserConstants.DEFAULT_DEPTH).intValue());
    }

    @Test
    public void testValidators() {
        UserConfigurationImpl configuration = new UserConfigurationImpl(getSecurityProvider());
        configuration.setRootProvider(getRootProvider());
        configuration.setTreeProvider(getTreeProvider());

        List<? extends ValidatorProvider> validators = configuration.getValidators(adminSession.getWorkspaceName(), Collections.<Principal>emptySet(), new MoveTracker());
        assertEquals(2, validators.size());

        List<String> clNames = Lists.newArrayList(
                UserValidatorProvider.class.getName(),
                CacheValidatorProvider.class.getName());

        for (ValidatorProvider vp : validators) {
            clNames.remove(vp.getClass().getName());
        }

        assertTrue(clNames.isEmpty());
    }

    @Test
    public void testGetConflictHandlers() {
        UserConfigurationImpl configuration = new UserConfigurationImpl();

        List<ThreeWayConflictHandler> conflictHandlers = configuration.getConflictHandlers();
        assertEquals(1, conflictHandlers.size());
        assertTrue(conflictHandlers.get(0) instanceof RepMembersConflictHandler);
    }

    @Test
    public void testGetProtectedItemImporters() {
        UserConfigurationImpl configuration = new UserConfigurationImpl(getSecurityProvider());

        List<ProtectedItemImporter> importers = configuration.getProtectedItemImporters();
        assertEquals(1, importers.size());
        assertTrue(importers.get(0) instanceof UserImporter);
    }

    @Test
    public void testBlobAccessProviderFromNullWhiteboard() throws Exception {
        SecurityProvider sp = mock(SecurityProvider.class, withSettings().extraInterfaces(WhiteboardAware.class));

        UserConfigurationImpl uc = new UserConfigurationImpl(sp);
        uc.setParameters(ConfigurationParameters.EMPTY);
        uc.setRootProvider(getRootProvider());
        uc.setTreeProvider(getTreeProvider());

        when(sp.getConfiguration(UserConfiguration.class)).thenReturn(uc);

        UserManager um = uc.getUserManager(root, getNamePathMapper());
        assertTrue(um instanceof UserManagerImpl);

        PartialValueFactory vf = ((UserManagerImpl) um).getPartialValueFactory();
        Field f = PartialValueFactory.class.getDeclaredField("blobAccessProvider");
        f.setAccessible(true);
        assertSame(PartialValueFactory.DEFAULT_BLOB_ACCESS_PROVIDER, f.get(vf));
    }

    @Test
    public void testUserConfigurationWithConstructor() {
        UserConfigurationImpl userConfiguration = new UserConfigurationImpl(getSecurityProvider());
        testConfigurationParameters(userConfiguration.getParameters());
    }

    @Test
    public void testUserConfigurationWithSetParameters() {
        UserConfigurationImpl userConfiguration = new UserConfigurationImpl();
        userConfiguration.setParameters(getParams());
        testConfigurationParameters(userConfiguration.getParameters());
    }
    
    private void testConfigurationParameters(ConfigurationParameters parameters) {
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_USER_PATH, UserConstants.DEFAULT_USER_PATH), USER_PATH);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_GROUP_PATH, UserConstants.DEFAULT_GROUP_PATH), GROUP_PATH);
        assertEquals(parameters.getConfigValue(PARAM_DEFAULT_DEPTH, UserConstants.DEFAULT_DEPTH), DEFAULT_DEPTH);
        assertEquals(parameters.getConfigValue(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_IGNORE), IMPORT_BEHAVIOR);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_PASSWORD_HASH_ALGORITHM, PasswordUtil.DEFAULT_ALGORITHM), HASH_ALGORITHM);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_PASSWORD_HASH_ITERATIONS, PasswordUtil.DEFAULT_ITERATIONS), HASH_ITERATIONS);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_PASSWORD_SALT_SIZE, PasswordUtil.DEFAULT_SALT_SIZE), SALT_SIZE);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_SUPPORT_AUTOSAVE, false), SUPPORT_AUTOSAVE);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_PASSWORD_MAX_AGE, UserConstants.DEFAULT_PASSWORD_MAX_AGE), MAX_AGE);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_PASSWORD_INITIAL_CHANGE, UserConstants.DEFAULT_PASSWORD_INITIAL_CHANGE), INITIAL_PASSWORD_CHANGE);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_PASSWORD_HISTORY_SIZE, UserConstants.PASSWORD_HISTORY_DISABLED_SIZE), PASSWORD_HISTORY_SIZE);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE, UserConstants.DEFAULT_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE), ENABLE_RFC7613_USERCASE_MAPPED_PROFILE);
    }

    private ConfigurationParameters getParams() {
        ConfigurationParameters params = ConfigurationParameters.of(new HashMap<String, Object>() {{
            put(UserConstants.PARAM_USER_PATH, USER_PATH);
            put(UserConstants.PARAM_GROUP_PATH, GROUP_PATH);
            put(PARAM_DEFAULT_DEPTH, DEFAULT_DEPTH);
            put(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, IMPORT_BEHAVIOR);
            put(UserConstants.PARAM_PASSWORD_HASH_ALGORITHM, HASH_ALGORITHM);
            put(UserConstants.PARAM_PASSWORD_HASH_ITERATIONS, HASH_ITERATIONS);
            put(UserConstants.PARAM_PASSWORD_SALT_SIZE, SALT_SIZE);
            put(UserConstants.PARAM_SUPPORT_AUTOSAVE, SUPPORT_AUTOSAVE);
            put(UserConstants.PARAM_PASSWORD_MAX_AGE, MAX_AGE);
            put(UserConstants.PARAM_PASSWORD_INITIAL_CHANGE, INITIAL_PASSWORD_CHANGE);
            put(UserConstants.PARAM_PASSWORD_HISTORY_SIZE, PASSWORD_HISTORY_SIZE);
            put(UserConstants.PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE, ENABLE_RFC7613_USERCASE_MAPPED_PROFILE);
        }});
        return params;
    }
}
