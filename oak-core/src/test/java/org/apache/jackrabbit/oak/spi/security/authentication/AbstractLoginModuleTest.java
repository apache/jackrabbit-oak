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

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.PrincipalProviderCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.UserManagerCallback;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.TestPrincipalProvider;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

/**
 * AbstractLoginModuleTest...
 */
public class AbstractLoginModuleTest {

    private static AbstractLoginModule initLoginModule(Class supportedCredentials, Map sharedState) {
        AbstractLoginModule lm = new TestLoginModule(supportedCredentials);
        lm.initialize(new Subject(), null, sharedState, null);
        return lm;
    }


    private static AbstractLoginModule initLoginModule(Class supportedCredentials, CallbackHandler cbh) {
        AbstractLoginModule lm = new TestLoginModule(supportedCredentials);
        lm.initialize(new Subject(), cbh, Collections.<String, Object>emptyMap(), null);
        return lm;
    }

    @Test
    public void testLogout() throws LoginException {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, ImmutableMap.of());

        assertFalse(loginModule.logout());
    }

    @Test
    public void testLogoutSuccess() throws LoginException {
        Subject subject = new Subject(false, ImmutableSet.<Principal>of(new PrincipalImpl("pName")), ImmutableSet.of(new TestCredentials()), ImmutableSet.of());

        AbstractLoginModule loginModule = new TestLoginModule(TestCredentials.class);
        loginModule.initialize(subject, null, ImmutableMap.<String, Object>of(), null);

        assertTrue(loginModule.logout());

        assertTrue(subject.getPublicCredentials().isEmpty());
        assertTrue(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testLogoutSuccess2() throws LoginException {
        Subject subject = new Subject(true, ImmutableSet.<Principal>of(new PrincipalImpl("pName")), ImmutableSet.of(new TestCredentials()), ImmutableSet.of());

        AbstractLoginModule loginModule = new TestLoginModule(TestCredentials.class);
        loginModule.initialize(subject, null, ImmutableMap.<String, Object>of(), null);

        assertTrue(loginModule.logout());

        assertFalse(subject.getPublicCredentials().isEmpty());
        assertFalse(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testAbort() throws LoginException {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, ImmutableMap.of());

        assertTrue(loginModule.abort());

        loginModule.login();
        assertTrue(loginModule.abort());
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

    @Test
    public void testGetCredentialsFromSubject() {
        Subject subject = new Subject();


        subject.getPublicCredentials().add(new TestCredentials());

        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class);
        lm.initialize(subject, null, ImmutableMap.<String, Object>of(), null);

        assertTrue(lm.getCredentials() instanceof TestCredentials);
    }

    @Test
    public void testGetCredentialsFromSubjectWrongClass() {
        Subject subject = new Subject();
        subject.getPublicCredentials().add(new SimpleCredentials("userid", new char[0]));

        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class);
        lm.initialize(subject, null, ImmutableMap.<String, Object>of(), null);

        assertNull(lm.getCredentials());
    }

    @Test
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

    @Test
    public void testGetCredentialsIOException() {
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true));
        assertNull(lm.getCredentials());
    }

    @Test
    public void testGetCredentialsUnsupportedCallbackException() {
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(false));
        assertNull(lm.getCredentials());
    }

    @Test
    public void testGetSharedPreAuthLoginEmptySharedState() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, ImmutableMap.of());
        assertNull(loginModule.getSharedPreAuthLogin());
    }

    @Test
    public void testGetSharedPreAuthLogin() {
        Map<String, PreAuthenticatedLogin> sharedState = new HashMap();
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, sharedState);

        PreAuthenticatedLogin preAuthenticatedLogin = new PreAuthenticatedLogin("userId");
        sharedState.put(AbstractLoginModule.SHARED_KEY_PRE_AUTH_LOGIN, preAuthenticatedLogin);

        assertSame(preAuthenticatedLogin, loginModule.getSharedPreAuthLogin());
    }

    @Test
    public void testGetSharedPreAuthLoginWrongEntry() {
        Map sharedState = new HashMap();
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, sharedState);

        sharedState.put(AbstractLoginModule.SHARED_KEY_PRE_AUTH_LOGIN, "wrongType");

        assertNull(loginModule.getSharedPreAuthLogin());
    }

    @Test
    public void testGetUserManagerFromCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(new TestUserManager()));

        assertNotNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerFromIncompleteCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler());

        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerIOException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true));

        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(false));

        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetPrincipalProviderFromCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(new TestPrincipalProvider()));

        assertNotNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderFromIncompleteCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler());

        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderIOException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true));

        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(false));

        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipals() {
        PrincipalProvider principalProvider = new TestPrincipalProvider();

        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(principalProvider));

        Principal principal = principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP).next();
        String userId = TestPrincipalProvider.getIDFromPrincipal(principal);
        Set<? extends Principal> principals = loginModule.getPrincipals(userId);

        assertFalse(principals.isEmpty());
        assertEquals(principalProvider.getPrincipals(userId), principals);
    }


    @Test
    public void testGetPrincipalsMissingProvider() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler());

        Set<? extends Principal> principals = loginModule.getPrincipals("userId");
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testSetAuthInfo() {
        Subject subject = new Subject();
        AuthInfo authInfo = new AuthInfoImpl("userid", null, null);

        AbstractLoginModule.setAuthInfo(authInfo, subject);
        Set<AuthInfo> fromSubject = subject.getPublicCredentials(AuthInfo.class);
        assertEquals(1, fromSubject.size());
        assertSame(authInfo, fromSubject.iterator().next());
    }

    @Test
    public void testSetAuthInfoPreExisting() {
        Subject subject = new Subject();
        subject.getPublicCredentials().add(new AuthInfoImpl(null, null, null));

        AuthInfo authInfo = new AuthInfoImpl("userid", null, null);

        AbstractLoginModule.setAuthInfo(authInfo, subject);
        Set<AuthInfo> fromSubject = subject.getPublicCredentials(AuthInfo.class);
        assertEquals(1, fromSubject.size());
        assertSame(authInfo, fromSubject.iterator().next());
    }

    //--------------------------------------------------------------------------

    private final class TestCredentials implements Credentials {}

    private static final class TestLoginModule extends AbstractLoginModule {

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
        public boolean login() {
            return true;
        }

        @Override
        public boolean commit() {
            return true;
        }
    }

    private final class TestCallbackHandler implements CallbackHandler {

        private UserManager userManager = null;
        private PrincipalProvider principalProvider = null;

        private TestCallbackHandler() {
        }

        private TestCallbackHandler(@Nonnull UserManager userManager) {
            this.userManager = userManager;
        }

        private TestCallbackHandler(@Nonnull PrincipalProvider principalProvider) {
            this.principalProvider = principalProvider;
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback cb : callbacks) {
                if (cb instanceof PrincipalProviderCallback) {
                    ((PrincipalProviderCallback) cb).setPrincipalProvider(principalProvider);
                } else if (cb instanceof UserManagerCallback) {
                    ((UserManagerCallback) cb).setUserManager(userManager);
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }

        }

    }

    private final class TestUserManager implements UserManager {
        @Override
        public Authorizable getAuthorizable(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Authorizable> T getAuthorizable(String s, Class<T> aClass) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Authorizable getAuthorizable(Principal principal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Authorizable getAuthorizableByPath(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Authorizable> findAuthorizables(String s, String s1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Authorizable> findAuthorizables(String s, String s1, int i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Authorizable> findAuthorizables(Query query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public User createUser(String s, String s1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public User createUser(String s, String s1, Principal principal, String s2) {
            throw new UnsupportedOperationException();
        }

        @Override
        public User createSystemUser(String s, String s1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Group createGroup(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Group createGroup(Principal principal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Group createGroup(Principal principal, String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Group createGroup(String s, Principal principal, String s1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isAutoSave() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void autoSave(boolean b) {
            throw new UnsupportedOperationException();

        }
    }
}