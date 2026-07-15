/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.run.osgi;

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG;
import static org.junit.Assert.assertNotNull;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.jcr.Credentials;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;
import org.apache.felix.jaas.LoginModuleFactory;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class JaasConfigSpiTest extends AbstractRepositoryFactoryTest {

    @Before
    public void setupRepo() {

        Map<String, String> loginModuleConfMap = Map.of(
                "jaas.controlFlag", "required",
                "jaas.classname", "org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl");

        Map<String, String> jaasConfigMap = Map.of("jaas.defaultRealmName", "jackrabbit.oak",
                "jaas.configProviderName", "FelixJaasProvider");

        Map<String, String> map3 = Map.of(AuthenticationConfiguration.PARAM_CONFIG_SPI_NAME, "FelixJaasProvider");

        Map<String, Map<String, String>> repoConfig = new LinkedHashMap<>(5);
        repoConfig.put("org.apache.felix.jaas.Configuration.factory-LoginModuleImpl", loginModuleConfMap);
        repoConfig.put("org.apache.felix.jaas.ConfigurationSpi", jaasConfigMap);
        repoConfig.put("org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl", map3);
        repoConfig.put("org.apache.jackrabbit.oak.jcr.osgi.RepositoryManager", Collections.emptyMap());
        repoConfig.put("org.apache.jackrabbit.oak.segment.SegmentNodeStoreService", Collections.emptyMap());
        config.put(REPOSITORY_CONFIG, repoConfig);
    }

    @Test
    public void defaultConfigSpiAuth() throws Exception {
        repository = repositoryFactory.getRepository(config);
        getRegistry().registerService(LoginModuleFactory.class.getName(),
                (LoginModuleFactory) TestLoginModule::new,
                new Hashtable<>(Map.of(
                        "jaas.controlFlag", "sufficient",
                        "jaas.realmName", "jackrabbit.oak",
                        "jaas.ranking", "150"
                )));

        Session session = repository.login(new SimpleCredentials("batman", "password".toCharArray()));
        assertNotNull(session);
        session.logout();
    }

    public static class TestLoginModule extends AbstractLoginModule {

        private Credentials credentials;
        private Set<? extends Principal> principals;
        private String userId;

        @NotNull
        @Override
        protected Set<Class> getSupportedCredentials() {
            return Set.of(SimpleCredentials.class);
        }

        @Override
        public boolean login() {
            credentials = getCredentials();
            if (credentials instanceof SimpleCredentials) {
                SimpleCredentials scred = (SimpleCredentials) credentials;
                if (Arrays.equals("password".toCharArray(), scred.getPassword()) && "batman".equals(scred.getUserID())) {
                    userId = "admin";
                    principals = getPrincipals(userId);
                    sharedState.put(SHARED_KEY_CREDENTIALS, credentials);
                    sharedState.put(SHARED_KEY_LOGIN_NAME, userId);
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean commit() {
            if (credentials == null || principals == null) {
                // login attempt in this login module was not successful
                clearState();
                return false;
            } else if (!subject.isReadOnly()) {
                subject.getPrincipals().addAll(principals);
                subject.getPublicCredentials().add(credentials);
                return true;
            }

            return false;
        }

        @Override
        public boolean logout() throws LoginException {
            return super.logout(Set.of(credentials), principals);
        }
    }
}
