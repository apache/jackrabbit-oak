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

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *     expected value is a validated single {@code Credentials} object.</li>
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
public class LoginModuleImpl extends AbstractLoginModule {

    private static final Logger log = LoggerFactory.getLogger(LoginModuleImpl.class);

    protected static final Set<Class> SUPPORTED_CREDENTIALS = new HashSet<Class>(3);
    static {
        SUPPORTED_CREDENTIALS.add(SimpleCredentials.class);
        SUPPORTED_CREDENTIALS.add(GuestCredentials.class);
        SUPPORTED_CREDENTIALS.add(ImpersonationCredentials.class);
    }

    private Credentials credentials;
    private Set<? extends Principal> principals;
    private String userID;

    //--------------------------------------------------------< LoginModule >---
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        super.initialize(subject, callbackHandler, sharedState, options);

        // TODO
    }

    @Override
    public boolean login() throws LoginException {
        // TODO
        credentials = getCredentials();
        userID = getUserID();

        Authentication authentication = new AuthenticationImpl(userID);
        boolean success = authentication.authenticate(credentials);
        if (!success) {
            success = impersonate(authentication);
        }

        if (success) {
            principals = getPrincipals(userID);

            log.debug("Login: adding Credentials to shared state.");
            sharedState.put(SHARED_KEY_CREDENTIALS, credentials);

            log.debug("Login: adding login name to shared state.");
            sharedState.put(SHARED_KEY_LOGIN_NAME, userID);
        }
        return success;
    }

    @Override
    public boolean commit() throws LoginException {
        if (credentials == null || principals.isEmpty()) {
            return false;
        } else {
            if (!subject.isReadOnly()) {
                subject.getPrincipals().addAll(principals);
                subject.getPublicCredentials().add(credentials);
                subject.getPublicCredentials().add(getAuthInfo());
            } else {
                log.debug("Could not add information to read only subject {}", subject);
            }
            return true;
        }
    }

    @Override
    public boolean abort() throws LoginException {
        credentials = null;
        principals = null;
        return true;
    }

    //------------------------------------------------< AbstractLoginModule >---

    @Override
    protected Set<Class> getSupportedCredentials() {
        return SUPPORTED_CREDENTIALS;
    }

    //--------------------------------------------------------------------------
    @CheckForNull
    private String getUserID() {
        // TODO add proper implementation
        String userID = null;
        if (credentials != null) {
            if (credentials instanceof SimpleCredentials) {
                userID = ((SimpleCredentials) credentials).getUserID();
            } else if (credentials instanceof GuestCredentials) {
                userID = "anonymous";
            } else if (credentials instanceof ImpersonationCredentials) {
                Credentials bc = ((ImpersonationCredentials) credentials).getBaseCredentials();
                if (bc instanceof SimpleCredentials) {
                    userID = ((SimpleCredentials) bc).getUserID();
                }
            } else {
                try {
                    NameCallback callback = new NameCallback("User-ID: ");
                    callbackHandler.handle(new Callback[]{callback});
                    userID = callback.getName();
                } catch (UnsupportedCallbackException e) {
                    log.warn("Credentials- or NameCallback must be supported");
                } catch (IOException e) {
                    log.error("Name-Callback failed: " + e.getMessage());
                }
            }
        }

        if (userID == null) {
            userID = getSharedLoginName();
        }

        return userID;
    }

    private boolean impersonate(Authentication authentication) {
        if (credentials instanceof ImpersonationCredentials) {
            AuthInfo info = ((ImpersonationCredentials) credentials).getImpersonatorInfo();
            if (authentication.impersonate(info.getPrincipals())) {
                return true;
            }
        }
        return false;
    }

    private AuthInfo getAuthInfo() {
        Map<String, Object> attributes = new HashMap<String, Object>();
        if (credentials instanceof SimpleCredentials) {
            SimpleCredentials sc = (SimpleCredentials) credentials;
            for (String attrName : sc.getAttributeNames()) {
                attributes.put(attrName, sc.getAttribute(attrName));
            }
        }
        return new AuthInfoImpl(userID, attributes, principals);
    }
}
