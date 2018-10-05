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

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME, getParams());
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
    public void testUserConfigurationWithConstructor() throws Exception {
        UserConfigurationImpl userConfiguration = new UserConfigurationImpl(getSecurityProvider());
        testConfigurationParameters(userConfiguration.getParameters());
    }

    @Test
    public void testUserConfigurationWithSetParameters() throws Exception {
        UserConfigurationImpl userConfiguration = new UserConfigurationImpl();
        userConfiguration.setParameters(getParams());
        testConfigurationParameters(userConfiguration.getParameters());
    }
    
    private void testConfigurationParameters(ConfigurationParameters parameters) throws Exception {
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_USER_PATH, UserConstants.DEFAULT_USER_PATH), USER_PATH);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_GROUP_PATH, UserConstants.DEFAULT_GROUP_PATH), GROUP_PATH);
        assertEquals(parameters.getConfigValue(UserConstants.PARAM_DEFAULT_DEPTH, UserConstants.DEFAULT_DEPTH), DEFAULT_DEPTH);
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
            put(UserConstants.PARAM_DEFAULT_DEPTH, DEFAULT_DEPTH);
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
