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
package org.apache.jackrabbit.oak.security.authentication;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.PrincipalProviderCallback;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default login module implementation that authenticates JCR {@code Credentials}
 * against the repository. Based on the credentials the {@link Principal}s
 * associated with user are retrieved from a configurable {@link PrincipalProvider}.
 *
 * <h3>Credentials</h3>
 *
 * The {@code Credentials} are collected during {@link #login()} using the
 * following logic:
 *
 * <ul>
 *     <li>{@code Credentials} as specified in {@link javax.jcr.Repository#login(javax.jcr.Credentials)}
 *     in which case they are retrieved from the {@code CallbackHandler}.</li>
 *     <li>A {@link #SHARED_KEY_CREDENTIALS} entry in the shared state. The
 *     expected value is a set of {@code Credentials}. Ffor backwards compatibility
 *     with the Jackrabbit 2.x) the former {@link #SHARED_KEY_JR_CREDENTIALS}
 *     entry in the shared state is also respected. In the latter case
 *     the expected value is a single {@code Credentials} object.</li>
 *     <li>If neither of the above variants provides Credentials this module
 *     tries to obtain them from the subject. See also
 *     {@link Subject#getSubject(java.security.AccessControlContext)}</li>
 * </ul>
 *
 * This implementation of the {@code LoginModule} currently supports the following
 * types of JCR Credentials:
 *
 * <ul>
 *     <li>{@link SimpleCredentials}</li>
 *     <li>{@link GuestCredentials}</li>
 *     <li>{@link TokenCredentials}</li>
 *     <li>{@link ImpersonationCredentials}</li>
 * </ul>
 *
 * The {@link Credentials} obtained during the {@link #login()} are added to
 * the shared state and - upon successful {@link #commit()} to the {@link Subject}.
 *
 * <h3>Principals</h3>
 *
 * TODO
 * - principal lookup -> principal provider
 * - principal resolution based on credentials
 *
 * <h3>Impersonation</h3>
 *
 * TODO
 *
 *
 *
 *
 */
public class LoginModuleImpl implements LoginModule {

    private static final Logger log = LoggerFactory.getLogger(LoginModuleImpl.class);

    /**
     * Backwards compatibility: Key of the sharedState entry referring to a
     * Credentials object being passed between multiple login modules.
     *
     * @deprecated Use {@link #SHARED_KEY_CREDENTIALS} instead.
     */
    private static final String SHARED_KEY_JR_CREDENTIALS = "org.apache.jackrabbit.credentials";

    /**
     * Key of the sharedState entry referring to a Set of Credentials that is
     * shared between multiple login modules.
     */
    public static final String SHARED_KEY_CREDENTIALS = "org.apache.jackrabbit.oak.credentials";

    protected static final Set<Class> SUPPORTED_CREDENTIALS = new HashSet<Class>(2);
    static {
        SUPPORTED_CREDENTIALS.add(SimpleCredentials.class);
        SUPPORTED_CREDENTIALS.add(GuestCredentials.class);
        SUPPORTED_CREDENTIALS.add(ImpersonationCredentials.class);
        SUPPORTED_CREDENTIALS.add(TokenCredentials.class);
    }

    private Subject subject;
    private CallbackHandler callbackHandler;
    private Map sharedState;

    private Set<Credentials> credentials;
    private Set<Principal> principals;
    private String userID;

    //--------------------------------------------------------< LoginModule >---
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        // TODO

        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
    }

    @Override
    public boolean login() throws LoginException {
        // TODO
        credentials = getCredentials();
        principals = getPrincipals();
        userID = getUserID();

        Authentication authentication = new AuthenticationImpl(userID);
        boolean success = authenticate(authentication);
        if (!success) {
            success = impersonate(authentication);
        }
        return success;
    }

    @Override
    public boolean commit() throws LoginException {
        // TODO

        if (!subject.isReadOnly()) {
            subject.getPrincipals().addAll(principals);
            subject.getPublicCredentials().addAll(credentials);
            subject.getPublicCredentials().add(getAuthInfo());
        } else {
            log.debug("Could not add information to read only subject {}", subject);
        }
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        credentials = null;
        principals = null;
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        if (subject.getPrincipals().isEmpty() || subject.getPublicCredentials(Credentials.class).isEmpty()) {
            return false;
        } else {
            // clear subject if not readonly
            if (!subject.isReadOnly()) {
                subject.getPrincipals().clear();
                subject.getPublicCredentials().clear();
            }
            return true;
        }
    }

    //--------------------------------------------------------------------------

    private Set<Credentials> getCredentials() {
        Set<Credentials> credentials = new HashSet<Credentials>();
        credentials.addAll(getSharedCredentials());

        if (callbackHandler != null) {
            log.debug("Login: retrieving Credentials using callback.");
            try {
                CredentialsCallback callback = new CredentialsCallback();
                callbackHandler.handle(new Callback[]{callback});
                Credentials creds = callback.getCredentials();
                if (creds != null) {
                    log.debug("Login: Credentials '{}' obtained from callback", creds);
                    credentials.add(creds);
                }
            } catch (UnsupportedCallbackException e) {
                log.warn(e.getMessage());
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }

        log.debug("Login: adding Credentials to shared state.");
        sharedState.put(SHARED_KEY_CREDENTIALS, credentials);

        if (credentials.isEmpty()) {
            log.debug("Login: No credentials found; looking for supported credentials in subject.");
            for (Class clz : SUPPORTED_CREDENTIALS) {
                credentials.addAll(subject.getPublicCredentials(clz));
            }
        }
        return credentials;
    }

    private Set<Credentials> getSharedCredentials() {
        Set<Credentials> sharedCredentials = new HashSet<Credentials>();
        if (sharedState.containsKey(SHARED_KEY_JR_CREDENTIALS)) {
            Object sc = sharedState.get(SHARED_KEY_JR_CREDENTIALS);
            if (sc instanceof Credentials) {
                sharedCredentials.add((Credentials) sc);
            } else {
                log.debug("Login: Invalid value for share state entry " + SHARED_KEY_JR_CREDENTIALS + ". Credentials expected.");
            }
        }
        if (sharedState.containsKey(SHARED_KEY_CREDENTIALS)) {
            Object scSet = sharedState.get(SHARED_KEY_CREDENTIALS);
            if (scSet instanceof Set) {
                 for (Object sc : (Set) scSet) {
                     if (sc instanceof Credentials) {
                         sharedCredentials.add((Credentials) sc);
                     }
                 }
            } else {
                log.debug("Login: Invalid value for share state entry " + SHARED_KEY_CREDENTIALS + ". Set of Credentials expected.");            }
        }

        return sharedCredentials;
    }

    private <T extends Credentials> java.util.Set<T> getCredentials(java.lang.Class<T> credentialsClass) {
        Set<T> cds = new HashSet<T>();
        for (Credentials c : credentials) {
            if (credentialsClass.isAssignableFrom(c.getClass())) {
                cds.add((T) c);
            }
        }
        return cds;
    }

    private static Principal getPrincipal(Credentials credentials, PrincipalProvider principalProvider) {
        Principal principal = null;
        if (credentials instanceof SimpleCredentials) {
            String userID = ((SimpleCredentials) credentials).getUserID();
            principal = principalProvider.getPrincipal(userID); // FIXME
        } else if (credentials instanceof GuestCredentials) {
            principal = principalProvider.getPrincipal("anonymous"); // FIXME
        }

        return principal;
    }

    private Set<Principal> getPrincipals() {
        Set<Principal> principals = new HashSet<Principal>();
        PrincipalProvider principalProvider = getPrincipalProvider();
        if (principalProvider != null) {
            for (Credentials creds : credentials) {
                Principal p = getPrincipal(creds, principalProvider);
                if (p != null) {
                    principals.add(p);
                    principals.addAll(principalProvider.getGroupMembership(p));
                } else {
                    log.debug("Commit: Cannot retrieve principal for Credentials '{}'.", creds);
                }
            }
        } else {
            log.debug("Commit: Cannot retrieve principals. No principal provider configured.");
        }

        return principals;
    }

    private PrincipalProvider getPrincipalProvider() {
        PrincipalProvider principalProvider = null;
        if (callbackHandler != null) {
            try {
                PrincipalProviderCallback principalCallBack = new PrincipalProviderCallback();
                callbackHandler.handle(new Callback[] {principalCallBack});
                principalProvider = principalCallBack.getPrincipalProvider();
            } catch (IOException e) {
                log.warn(e.getMessage());
            } catch (UnsupportedCallbackException e) {
                log.warn(e.getMessage());
            }
        }
        return principalProvider;
    }

    private String getUserID() {
        // TODO add proper implementation
        String userID = null;
        if (!credentials.isEmpty()) {
            Credentials c = credentials.iterator().next();
            if (c instanceof SimpleCredentials) {
                userID = ((SimpleCredentials) c).getUserID();
            } else if (c instanceof GuestCredentials) {
                userID = "anonymous";
            } else if (c instanceof ImpersonationCredentials) {
                Credentials bc = ((ImpersonationCredentials) c).getBaseCredentials();
                if (bc instanceof SimpleCredentials) {
                    userID = ((SimpleCredentials) bc).getUserID();
                }
            }
        }
        return userID;
    }

    private boolean impersonate(Authentication authentication) {
        for (ImpersonationCredentials ic : getCredentials(ImpersonationCredentials.class)) {
            AuthInfo info = ic.getImpersonatorInfo();
            if (info instanceof AuthInfoImpl) {
                if (authentication.impersonate(((AuthInfoImpl) info).getPrincipals())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean authenticate(Authentication authentication) {
        for (Credentials creds : credentials) {
            if (authentication.authenticate(creds)) {
                return true;
            }
        }
        return false;
    }

    private AuthInfo getAuthInfo() {
        Map<String, Object> attributes = new HashMap<String, Object>();
        for (SimpleCredentials sc : getCredentials(SimpleCredentials.class)) {
            for (String attrName : sc.getAttributeNames()) {
                attributes.put(attrName, sc.getAttribute(attrName));
            }
        }
        return new AuthInfoImpl(userID, attributes, principals);
    }
}