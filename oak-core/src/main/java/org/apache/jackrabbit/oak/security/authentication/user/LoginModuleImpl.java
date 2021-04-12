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

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Default login module implementation that authenticates JCR {@code Credentials}
 * against the repository. Based on the credentials the {@link Principal}s
 * associated with user are retrieved from a configurable
 * {@link org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider}.
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
 *     {@link javax.security.auth.Subject#getSubject(java.security.AccessControlContext)}</li>
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
 * will fail with {@code LoginException}.<p>
 * Please note, that a user will always be allowed to impersonate him/herself
 * irrespective of the impersonation definitions exposed by
 * {@link org.apache.jackrabbit.api.security.user.User#getImpersonation()}
 */
public final class LoginModuleImpl extends AbstractLoginModule {

    private static final Logger log = LoggerFactory.getLogger(LoginModuleImpl.class);

    protected static final Set<Class> SUPPORTED_CREDENTIALS = new HashSet<>(3);
    static {
        SUPPORTED_CREDENTIALS.add(SimpleCredentials.class);
        SUPPORTED_CREDENTIALS.add(GuestCredentials.class);
        SUPPORTED_CREDENTIALS.add(ImpersonationCredentials.class);
    }

    private Credentials credentials;
    private String userId;
    private Principal principal;
    private boolean success;
    private Set<? extends Principal> principals;
    private AuthInfo authInfo;

    //--------------------------------------------------------< LoginModule >---

    @Override
    public boolean login() throws LoginException {
        credentials = getCredentials();

        // check if we have a pre authenticated login from a previous login module
        PreAuthenticatedLogin preAuthLogin = getSharedPreAuthLogin();
        String loginName = getLoginId(preAuthLogin);
        Authentication authentication = getUserAuthentication(loginName);
        if (authentication != null) {
            success = authenticate(preAuthLogin, authentication);
            if (success) {
                log.debug("Adding Credentials to shared state.");
                //noinspection unchecked
                sharedState.put(SHARED_KEY_CREDENTIALS, credentials);

                log.debug("Adding login name to shared state.");
                //noinspection unchecked
                sharedState.put(SHARED_KEY_LOGIN_NAME, loginName);

                userId = authentication.getUserId();
                if (userId == null) {
                    userId = loginName;
                }
                principal = authentication.getUserPrincipal();
            }
        } else {
            // ensure that we don't commit (OAK-2998, OAK-3032)
            credentials = null;
            userId = null;
        }
        return success;
    }

    @Override
    public boolean commit() {
        if (!success) {
            // login attempt in this login module was not successful
            clearState();
            return false;
        } else {
            principals = Collections.emptySet();
            if (principal != null) {
                principals = getPrincipals(principal);
            } else if (userId != null) {
                principals = getPrincipals(userId);
            }
            authInfo = createAuthInfo(principals);
            if (!subject.isReadOnly()) {
                subject.getPrincipals().addAll(principals);
                if (credentials != null) {
                    subject.getPublicCredentials().add(credentials);
                }
                setAuthInfo(authInfo, subject);
            } else {
                log.debug("Could not add information to read only subject {}", subject);
            }
            closeSystemSession();
            return true;
        }
    }

    @Override
    public boolean logout() throws LoginException {
        Set creds = Stream.of(credentials, authInfo).filter(Objects::nonNull).collect(Collectors.toSet());
        return logout((creds.isEmpty() ? null : creds), principals);
    }

    //------------------------------------------------< AbstractLoginModule >---
    @NotNull
    @Override
    protected Set<Class> getSupportedCredentials() {
        return SUPPORTED_CREDENTIALS;
    }

    @Override
    protected void clearState() {
        super.clearState();

        credentials = null;
        userId = null;
        principal = null;
        principals = null;
        authInfo = null;
    }

    //--------------------------------------------------------------------------
    @Nullable
    private String getLoginId(@Nullable PreAuthenticatedLogin preAuthenticatedLogin) {
        if (preAuthenticatedLogin != null) {
            return preAuthenticatedLogin.getUserId();
        }

        String uid = null;
        if (credentials instanceof SimpleCredentials) {
            uid = ((SimpleCredentials) credentials).getUserID();
        } else if (credentials instanceof GuestCredentials) {
            uid = getAnonymousId();
        } else if (credentials instanceof ImpersonationCredentials) {
            Credentials bc = ((ImpersonationCredentials) credentials).getBaseCredentials();
            if (bc instanceof SimpleCredentials) {
                uid = ((SimpleCredentials) bc).getUserID();
            }
        } // null or other (unsupported) type of credentials (see SUPPORTED_CREDENTIALS)

        if (uid == null) {
            uid = getSharedLoginName();
        }
        return uid;
    }

    @Nullable
    private String getAnonymousId() {
        SecurityProvider sp = getSecurityProvider();
        if (sp == null) {
            return null;
        } else {
            ConfigurationParameters params = sp.getConfiguration(UserConfiguration.class).getParameters();
            return UserUtil.getAnonymousId(params);
        }
    }

    @Nullable
    private Authentication getUserAuthentication(@Nullable String loginName) {
        SecurityProvider securityProvider = getSecurityProvider();
        Root root = getRoot();
        if (securityProvider != null && root != null) {
            UserConfiguration uc = securityProvider.getConfiguration(UserConfiguration.class);
            UserAuthenticationFactory factory = uc.getParameters().getConfigValue(UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, null, UserAuthenticationFactory.class);
            if (factory != null) {
                return factory.getAuthentication(uc, root, loginName);
            } else {
                log.error("No user authentication factory configured in user configuration.");
            }
        }
        return null;
    }

    private boolean authenticate(@Nullable PreAuthenticatedLogin preAuthLogin, @NotNull Authentication authentication) throws LoginException {
        Credentials crds = (preAuthLogin != null) ? PreAuthenticatedLogin.PRE_AUTHENTICATED : credentials;
        try {
            return authentication.authenticate(crds);
        } catch (LoginException e) {
            getLoginModuleMonitor().loginFailed(e, crds);
            throw e;
        }
    }

    private AuthInfo createAuthInfo(@NotNull Set<? extends Principal> principals) {
        Credentials creds;
        if (credentials instanceof ImpersonationCredentials) {
            creds = ((ImpersonationCredentials) credentials).getBaseCredentials();
        } else {
            creds = credentials;
        }
        Map<String, Object> attributes = new HashMap<>();
        Object shared = sharedState.get(SHARED_KEY_ATTRIBUTES);
        if (shared instanceof Map) {
            ((Map<?,?>) shared).forEach((key, value) -> attributes.put(key.toString(), value));
        } else if (creds instanceof SimpleCredentials) {
            SimpleCredentials sc = (SimpleCredentials) creds;
            for (String attrName : sc.getAttributeNames()) {
                attributes.put(attrName, sc.getAttribute(attrName));
            }
        }
        return new AuthInfoImpl(userId, attributes, Iterables.concat(principals, subject.getPrincipals()));
    }
}
