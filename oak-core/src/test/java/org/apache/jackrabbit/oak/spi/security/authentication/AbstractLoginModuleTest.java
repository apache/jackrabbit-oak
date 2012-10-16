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
package org.apache.jackrabbit.oak.spi.security.authentication;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

/**
 * AbstractLoginModuleTest...
 */
public class AbstractLoginModuleTest {

    private AbstractLoginModule initLoginModule(Class supportedCredentials, Map sharedState) {
        AbstractLoginModule lm = new TestLoginModule(supportedCredentials);
        lm.initialize(new Subject(), null, sharedState, null);
        return lm;
    }


    private AbstractLoginModule initLoginModule(Class supportedCredentials, CallbackHandler cbh) {
        AbstractLoginModule lm = new TestLoginModule(supportedCredentials);
        lm.initialize(new Subject(), cbh, Collections.<String, Object>emptyMap(), null);
        return lm;
    }

    @Test
    public void testGetSharedLoginName() {
        Map<String, String> sharedState = new HashMap<String, String>();

        sharedState.put(AbstractLoginModule.SHARED_KEY_LOGIN_NAME, "test");
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, sharedState);
        assertEquals("test", lm.getSharedLoginName());

        sharedState.clear();
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getSharedLoginName());
    }

    @Test
    public void testGetSharedCredentials() {
        Map<String, Object> sharedState = new HashMap<String, Object>();

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new TestCredentials());
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, sharedState);
        assertTrue(lm.getSharedCredentials() instanceof TestCredentials);

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new SimpleCredentials("test", "test".toCharArray()));
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertTrue(lm.getSharedCredentials() instanceof SimpleCredentials);

        lm = initLoginModule(SimpleCredentials.class, sharedState);
        assertTrue(lm.getSharedCredentials() instanceof SimpleCredentials);

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, "no credentials object");
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getSharedCredentials());

        sharedState.clear();
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getSharedCredentials());
    }

    @Test
    public void testGetCredentialsFromSharedState() {
        Map<String, Credentials> sharedState = new HashMap<String, Credentials>();

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new TestCredentials());
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, sharedState);
        assertTrue(lm.getCredentials() instanceof TestCredentials);

        SimpleCredentials sc = new SimpleCredentials("test", "test".toCharArray());
        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, sc);
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getCredentials());

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, sc);
        lm = initLoginModule(SimpleCredentials.class, sharedState);
        assertTrue(lm.getCredentials() instanceof SimpleCredentials);

        sharedState.clear();
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getCredentials());
    }

    public void testGetCredentialsFromCallbackHandler() {
        CallbackHandler cbh = new CallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) {
                for (Callback cb : callbacks) {
                    if (cb instanceof CredentialsCallback) {
                        ((CredentialsCallback) cb).setCredentials(new TestCredentials());
                    }
                }
            }
        };

        AbstractLoginModule lm = initLoginModule(TestCredentials.class, cbh);
        assertTrue(lm.getCredentials() instanceof TestCredentials);

        lm = initLoginModule(SimpleCredentials.class, cbh);
        assertNull(lm.getCredentials());
    }

    //--------------------------------------------------------------------------

    private final class TestCredentials implements Credentials {}

    private final class TestLoginModule extends AbstractLoginModule {

        private Class supportedCredentialsClass;

        private TestLoginModule(Class supportedCredentialsClass) {
            this.supportedCredentialsClass = supportedCredentialsClass;
        }

        @Nonnull
        @Override
        protected Set<Class> getSupportedCredentials() {
            return Collections.singleton(supportedCredentialsClass);
        }

        @Override
        public boolean login() throws LoginException {
            return true;
        }

        @Override
        public boolean commit() throws LoginException {
            return true;
        }
    }
}