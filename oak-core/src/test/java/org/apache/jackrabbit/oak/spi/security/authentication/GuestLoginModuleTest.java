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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * GuestLoginModuleTest...
 */
public class GuestLoginModuleTest {

    private LoginModule guestLoginModule = new GuestLoginModule();

    @Test
    public void testNullLogin() throws LoginException {
        Subject subject = new Subject();
        CallbackHandler cbh = new TestCallbackHandler(null);
        Map sharedState = new HashMap();
        guestLoginModule.initialize(subject, cbh, sharedState, Collections.<String, Object>emptyMap());

        assertTrue(guestLoginModule.login());
        Object sharedCreds = sharedState.get(AbstractLoginModule.SHARED_KEY_CREDENTIALS);
        assertNotNull(sharedCreds);
        assertTrue(sharedCreds instanceof GuestCredentials);

        assertTrue(guestLoginModule.commit());
        assertFalse(subject.getPrincipals(EveryonePrincipal.class).isEmpty());
        assertFalse(subject.getPublicCredentials(GuestCredentials.class).isEmpty());

        assertTrue(guestLoginModule.logout());
    }

    @Test
    public void testGuestCredentials() throws LoginException {
        Subject subject = new Subject();
        CallbackHandler cbh = new TestCallbackHandler(new GuestCredentials());
        Map sharedState = new HashMap();
        guestLoginModule.initialize(subject, cbh, sharedState, Collections.<String, Object>emptyMap());

        assertFalse(guestLoginModule.login());
        assertFalse(sharedState.containsKey(AbstractLoginModule.SHARED_KEY_CREDENTIALS));

        assertFalse(guestLoginModule.commit());
        assertTrue(subject.getPrincipals().isEmpty());
        assertTrue(subject.getPublicCredentials().isEmpty());

        assertFalse(guestLoginModule.logout());
    }

    @Test
    public void testSimpleCredentials() throws LoginException {
        Subject subject = new Subject();
        CallbackHandler cbh = new TestCallbackHandler(new SimpleCredentials("test", new char[0]));
        Map sharedState = new HashMap();
        guestLoginModule.initialize(subject, cbh, sharedState, Collections.<String, Object>emptyMap());

        assertFalse(guestLoginModule.login());
        assertFalse(sharedState.containsKey(AbstractLoginModule.SHARED_KEY_CREDENTIALS));

        assertFalse(guestLoginModule.commit());
        assertTrue(subject.getPrincipals().isEmpty());
        assertTrue(subject.getPublicCredentials().isEmpty());

        assertFalse(guestLoginModule.logout());
    }

    @Test
    public void testThrowingCallbackhandler() throws LoginException {
        Subject subject = new Subject();
        CallbackHandler cbh = new ThrowingCallbackHandler(true);
        Map sharedState = new HashMap();
        guestLoginModule.initialize(subject, cbh, sharedState, Collections.<String, Object>emptyMap());

        assertFalse(guestLoginModule.login());
        assertFalse(sharedState.containsKey(AbstractLoginModule.SHARED_KEY_CREDENTIALS));

        assertFalse(guestLoginModule.commit());
        assertTrue(subject.getPublicCredentials(GuestCredentials.class).isEmpty());

        assertFalse(guestLoginModule.logout());
    }

    @Test
    public void testThrowingCallbackhandler2() throws LoginException {
        Subject subject = new Subject();
        CallbackHandler cbh = new ThrowingCallbackHandler(false);
        Map sharedState = new HashMap();
        guestLoginModule.initialize(subject, cbh, sharedState, Collections.<String, Object>emptyMap());

        assertFalse(guestLoginModule.login());
        assertFalse(sharedState.containsKey(AbstractLoginModule.SHARED_KEY_CREDENTIALS));

        assertFalse(guestLoginModule.commit());
        assertTrue(subject.getPublicCredentials(GuestCredentials.class).isEmpty());

        assertFalse(guestLoginModule.logout());
    }

    @Test
    public void testAbort() throws LoginException {
        assertTrue(guestLoginModule.abort());
    }

    @Test
    public void testLogout() throws LoginException {
        assertFalse(guestLoginModule.logout());
    }

    //--------------------------------------------------------------------------

    private class TestCallbackHandler implements CallbackHandler {

        private final Credentials creds;

        private TestCallbackHandler(Credentials creds) {
            this.creds = creds;
        }
        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof CredentialsCallback) {
                    ((CredentialsCallback) callback).setCredentials(creds);
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }

}