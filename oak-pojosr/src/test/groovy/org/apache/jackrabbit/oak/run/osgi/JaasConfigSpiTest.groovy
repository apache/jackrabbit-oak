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

package org.apache.jackrabbit.oak.run.osgi

import com.google.common.collect.ImmutableSet
import org.apache.felix.jaas.LoginModuleFactory
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration
import org.junit.Before
import org.junit.Ignore
import org.junit.Test

import javax.annotation.Nonnull
import javax.jcr.Credentials
import javax.jcr.Session
import javax.jcr.SimpleCredentials
import javax.security.auth.login.LoginException
import javax.security.auth.spi.LoginModule
import java.security.Principal

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG


class JaasConfigSpiTest extends AbstractRepositoryFactoryTest{
    @Before
    void setupRepo(){
        config[REPOSITORY_CONFIG] = [
                'org.apache.felix.jaas.Configuration.factory-LoginModuleImpl' : [
                        'jaas.controlFlag' : 'required',
                        'jaas.classname' : 'org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl'
                ],
                'org.apache.felix.jaas.ConfigurationSpi' : [
                        //jaas.globalConfigPolicy defaults to 'default'
                        'jaas.defaultRealmName' : 'jackrabbit.oak',
                        'jaas.configProviderName' : 'FelixJaasProvider',
                ],
                'org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl' :[
                        (AuthenticationConfiguration.PARAM_CONFIG_SPI_NAME) : 'FelixJaasProvider'
                ],
                'org.apache.jackrabbit.oak.jcr.osgi.RepositoryManager' : [:],
                'org.apache.jackrabbit.oak.segment.SegmentNodeStoreService' : [:]
        ]
    }

    @Test
    public void defaultConfigSpiAuth() throws Exception{
        repository = repositoryFactory.getRepository(config)
        registry.registerService(LoginModuleFactory.class.name, new LoginModuleFactory() {
            @Override
            LoginModule createLoginModule() {
                return new TestLoginModule();
            }
        }, [
                'jaas.controlFlag' : 'sufficient',
                'jaas.realmName' : 'jackrabbit.oak',
                'jaas.ranking' : '150',

        ] as Hashtable)



        Session session = repository.login(new SimpleCredentials("batman", "password".toCharArray()))
        assert session
        session.logout()
    }

    public static class TestLoginModule extends AbstractLoginModule {
        private Credentials credentials;
        private Set<? extends Principal> principals;
        private String userId;

        @Nonnull
        @Override
        protected Set<Class> getSupportedCredentials() {
            return ImmutableSet.of(SimpleCredentials.class)
        }

        @Override
        boolean login() throws LoginException {
            credentials = getCredentials();
            if(credentials instanceof SimpleCredentials){
                SimpleCredentials scred = (SimpleCredentials)credentials;
                if(scred.getPassword() == "password".toCharArray()
                    && scred.getUserID() == 'batman'){
                    userId = 'admin';
                    principals = getPrincipals(userId);
                    sharedState.put(SHARED_KEY_CREDENTIALS, credentials);
                    sharedState.put(SHARED_KEY_LOGIN_NAME, userId);
                    return true;
                }
            }
            return false
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
    }
}