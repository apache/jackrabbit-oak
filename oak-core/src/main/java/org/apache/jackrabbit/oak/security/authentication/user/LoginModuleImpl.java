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
package org.apache.jackrabbit.oak.security.authentication.user;

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
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
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
 * The {@link Credentials} obtained during the {@code #login()} are added to
 * the shared state and - upon successful {@code #commit()} to the {@code Subject}.
 *
 * <h3>Principals</h3>
 * Upon successful login the principals associated with the user are calculated
 * (see also {@link AbstractLoginModule#getPrincipals(String)}. These principals
 * are finally added to the subject during {@code #commit()}.
 *
 * <h3>Impersonation</h3>
 * Impersonation such as defined by {@link javax.jcr.Session#impersonate(javax.jcr.Credentials)}
 * is covered by this login module by the means of {@link ImpersonationCredentials}.
 * Impersonation will succeed if the {@link ImpersonationCredentials#getBaseCredentials() base credentials}
 * refer to a valid user that has not been disabled. If the authenticating
 * subject is not allowed to impersonate the specified user, the login attempt
 * will fail with {@code LoginException}.<p/>
 * Please note, that a user will always be allowed to impersonate him/herself
 * irrespective of the impersonation definitions exposed by
 * {@link org.apache.jackrabbit.api.security.user.User#getImpersonation()}
 */
public final class LoginModuleImpl extends AbstractLoginModule {

    private static final Logger log = LoggerFactory.getLogger(LoginModuleImpl.class);

    protected static final Set<Class> SUPPORTED_CREDENTIALS = new HashSet<Class>(3);
    static {
        SUPPORTED_CREDENTIALS.add(SimpleCredentials.class);
        SUPPORTED_CREDENTIALS.add(GuestCredentials.class);
        SUPPORTED_CREDENTIALS.add(ImpersonationCredentials.class);
    }

    private Credentials credentials;
    private Set<? extends Principal> principals;
    private String userId;

    //--------------------------------------------------------< LoginModule >---

    @Override
    public boolean login() throws LoginException {
        credentials = getCredentials();
        userId = getUserId();

        if (credentials == null || userId == null) {
            log.debug("Could not extract userId/credentials");
            return false;
        }

        Authentication authentication = new UserAuthentication(userId, getUserManager());
        boolean success = authentication.authenticate(credentials);
        if (success) {
            principals = getPrincipals(userId);

            log.debug("Adding Credentials to shared state.");
            sharedState.put(SHARED_KEY_CREDENTIALS, credentials);

            log.debug("Adding login name to shared state.");
            sharedState.put(SHARED_KEY_LOGIN_NAME, userId);
        }
        return success;
    }

    @Override
    public boolean commit() {
        if (credentials == null || principals == null) {
            // login attempt in this login module was not successful
            clearState();
            return false;
        } else {
            if (!subject.isReadOnly()) {
                subject.getPrincipals().addAll(principals);
                subject.getPublicCredentials().add(credentials);
                subject.getPublicCredentials().add(createAuthInfo());
            } else {
                log.debug("Could not add information to read only subject {}", subject);
            }
            return true;
        }
    }

    //------------------------------------------------< AbstractLoginModule >---
    @Override
    protected Set<Class> getSupportedCredentials() {
        return SUPPORTED_CREDENTIALS;
    }

    @Override
    protected void clearState() {
        super.clearState();

        credentials = null;
        principals = null;
        userId = null;
    }

    //--------------------------------------------------------------------------
    @CheckForNull
    private String getUserId() {
        String uid = null;
        if (credentials != null) {
            if (credentials instanceof SimpleCredentials) {
                uid = ((SimpleCredentials) credentials).getUserID();
            } else if (credentials instanceof GuestCredentials) {
                uid = getAnonymousId();
            } else if (credentials instanceof ImpersonationCredentials) {
                Credentials bc = ((ImpersonationCredentials) credentials).getBaseCredentials();
                if (bc instanceof SimpleCredentials) {
                    uid = ((SimpleCredentials) bc).getUserID();
                }
            } else {
                try {
                    NameCallback callback = new NameCallback("User-ID: ");
                    callbackHandler.handle(new Callback[]{callback});
                    uid = callback.getName();
                } catch (UnsupportedCallbackException e) {
                    log.warn("Credentials- or NameCallback must be supported");
                } catch (IOException e) {
                    log.error("Name-Callback failed: " + e.getMessage());
                }
            }
        }

        if (uid == null) {
            uid = getSharedLoginName();
        }
        return uid;
    }

    private String getAnonymousId() {
        SecurityProvider sp = getSecurityProvider();
        if (sp == null) {
            return null;
        } else {
            return UserUtility.getAnonymousId(sp.getUserConfiguration().getConfigurationParameters());
        }
    }

    private AuthInfo createAuthInfo() {
        Map<String, Object> attributes = new HashMap<String, Object>();
        if (credentials instanceof SimpleCredentials) {
            SimpleCredentials sc = (SimpleCredentials) credentials;
            for (String attrName : sc.getAttributeNames()) {
                attributes.put(attrName, sc.getAttribute(attrName));
            }
        }
        return new AuthInfoImpl(userId, attributes, principals);
    }
}
