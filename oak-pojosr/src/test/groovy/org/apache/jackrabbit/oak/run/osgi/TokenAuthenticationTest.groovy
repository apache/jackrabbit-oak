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

import com.google.common.collect.Sets
import groovy.util.logging.Slf4j
import org.apache.felix.jaas.LoginModuleFactory
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin
import org.junit.Before
import org.junit.Ignore
import org.junit.Test

import javax.annotation.Nonnull
import javax.jcr.Credentials
import javax.jcr.Session
import javax.jcr.SimpleCredentials
import javax.security.auth.login.LoginException
import javax.security.auth.spi.LoginModule

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE


class TokenAuthenticationTest extends AbstractRepositoryFactoryTest{

    @Before
    void setupRepo(){
        config[REPOSITORY_CONFIG_FILE] = createConfigValue("oak-base-config.json", "oak-tar-config.json")
    }

    @Ignore
    @Test
    public void tokenCreationWithPreAuth() throws Exception{
        repository = repositoryFactory.getRepository(config)
        registry.registerService(LoginModuleFactory.class.name, new PreAuthLoginModuleFactory(), [
                'jaas.controlFlag' : 'sufficient',
                'jaas.realmName' : 'jackrabbit.oak',
                'jaas.ranking' : '150',

        ] as Hashtable)

        MyCredential myCred = new MyCredential("admin")
        Session session = repository.login(myCred)
//        assert session.getAttribute(".token")
        assert myCred.getAttribute(".token")
    }

    private static class PreAuthLoginModuleFactory implements LoginModuleFactory {
        @Override
        LoginModule createLoginModule() {
            return new PreAuthLoginModule()
        }
    }

    @Slf4j
    private static class PreAuthLoginModule extends AbstractLoginModule {
        private MyCredential credential
        @Nonnull
        @Override
        protected Set<Class> getSupportedCredentials() {
            return Sets.newHashSet(MyCredential.class)
        }

        @Override
        boolean login() throws LoginException {
            Credentials credentials = getCredentials();
            if (credentials instanceof MyCredential) {
                credential = ((MyCredential) credentials)
                String userId = credential.userID;
                if (userId == null) {
                    log.debug("Could not extract userId/credentials");
                } else {
                    SimpleCredentials sc = new SimpleCredentials(userId, new char[0])
                    sc.setAttribute(".token","")

                    // we just set the login name and rely on the following login modules to populate the subject
                    sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(userId));
                    sharedState.put(SHARED_KEY_CREDENTIALS, sc);
                    sharedState.put(SHARED_KEY_LOGIN_NAME, userId);
                    log.debug("login succeeded with trusted user: {}", userId);
                }
            }
            return false;
        }

        @Override
        boolean commit() throws LoginException {
            Credentials sharedCreds = getSharedCredentials()
            if(sharedCreds instanceof SimpleCredentials) {
                credential.setAttribute(".token", ((SimpleCredentials)sharedCreds).getAttribute(".token"))
            }
            return false
        }
    }

    private static class MyCredential implements Credentials {
        final String userID
        private final HashMap attributes = new HashMap();

        MyCredential(String userID) {
            this.userID = userID
        }

        public void setAttribute(String name, Object value) {
            // name cannot be null
            if (name == null) {
                throw new IllegalArgumentException("name cannot be null");
            }

            // null value is the same as removeAttribute()
            if (value == null) {
                removeAttribute(name);
                return;
            }

            synchronized (attributes) {
                attributes.put(name, value);
            }
        }

        public void removeAttribute(String name) {
            synchronized (attributes) {
                attributes.remove(name);
            }
        }

        public Object getAttribute(String name) {
            synchronized (attributes) {
                return (attributes.get(name));
            }
        }

    }
}
