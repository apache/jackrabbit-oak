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
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.PrincipalProviderCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.SecurityProviderCallback;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of the {@link LoginModule} interface that can act
 * as base class for login modules that aim to autenticate subjects against
 * information stored in the content repository.
 *
 * <h2>LoginModule Methods</h2>
 * This base class provides a simple implementation for the following methods
 * of the {@code LoginModule} interface:
 *
 * <h3>{@link LoginModule#initialize(Subject, CallbackHandler, Map, Map) Initialize}</h3>
 * Initialization of this abstract module sets the following protected instance
 * fields:
 * <ul>
 *     <li>subject: The subject to be authenticated,</li>
 *     <li>callbackHandler: The callback handler passed to the login module,</li>
 *     <li>shareState: The map used to share state information with other login
 *     modules,</li>
 *     <li>options: The configuration options of this login module as specified
 *     in the {@link javax.security.auth.login.Configuration}.</li>
 * </ul>
 *
 * <h3>{@link LoginModule#logout() Logout}</h3>
 * If the authenticated subject is not empty this logout implementation attempts
 * to clear both principals and public credentials and returns {@code true}.
 *
 * <h3>{@link LoginModule#abort() Abort}</h3>
 * Clears the state of this login module by setting all private instance
 * variables created in phase 1 or 2 to {@code null}. Subclasses are in charge of
 * releasing their own state information by either overriding {@link #clearState()}.
 *
 * <h2>Utility Methods</h2>
 * The following methods are provided in addition:
 *
 * <h3>{@link #clearState()}</h3>
 * Clears all private state information that has be created during login. This
 * method in called in {@link #abort()} and subclasses are expected to override
 * this method.
 *
 * <h3>{@link #getSupportedCredentials()}</h3>
 * Abstract method used by {@link #getCredentials()} that reveals which credential
 * implementations are supported by the {@code LoginModule}.
 *
 * <h3>{@link #getCredentials()}</h3>
 * Tries to retrieve valid (supported) Credentials in the following order:
 * <ol>
 *     <li>using a {@link CredentialsCallback},</li>
 *     <li>looking for a {@link #SHARED_KEY_CREDENTIALS} entry in the shared
 *     state (see also {@link #getSharedCredentials()} and finally by</li>
 *     <li>searching for valid credentials in the subject.</li>
 * </ol>
 *
 * <h3>{@link #getSharedCredentials()}</h3>
 * This method returns any credentials passed to the login module with the
 * share state. The key to share credentials with a another module extending from
 * this base class is {@link #SHARED_KEY_CREDENTIALS}.
 *
 * <h3>{@link #getSharedLoginName()}</h3>
 * If the shared state contains an entry for {@link #SHARED_KEY_LOGIN_NAME} this
 * method returns the value as login name.
 *
 * <h3>{@link #getPrincipals(String)}</h3>
 * Returns all principals associated with a given user id. This method should
 * be called after a successful authentication in order to be able to populate
 * the subject during {@link #commit()}.
 *
 * <h3>{@link #getPrincipalProvider()}</h3>
 * // TODO
 * <h3>{@link #getUserProvider()}</h3>
 * // TODO
 * <h3>{@link #getSecurityProvider()}</h3>
 * // TODO
 * <h3>{@link #getRoot()}</h3>
 * // TODO
 *
 */
public abstract class AbstractLoginModule implements LoginModule {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AbstractLoginModule.class);

    /**
     * Key of the sharedState entry referring to validated Credentials that is
     * shared between multiple login modules.
     */
    public static final String SHARED_KEY_CREDENTIALS = "org.apache.jackrabbit.credentials";

    /**
     * Key of the sharedState entry referring to a valid login ID that is shared
     * between multiple login modules.
     */
    public static final String SHARED_KEY_LOGIN_NAME = "javax.security.auth.login.name";

    protected Subject subject;
    protected CallbackHandler callbackHandler;
    protected Map sharedState;
    protected ConfigurationParameters options;

    private SecurityProvider securityProvider;
    private Root root;

    //--------------------------------------------------------< LoginModule >---
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
        this.options = new ConfigurationParameters(options);
    }

    @Override
    public boolean logout() throws LoginException {
        boolean success = false;
        if (!subject.getPrincipals().isEmpty() && !subject.getPublicCredentials(Credentials.class).isEmpty()) {
            // clear subject if not readonly
            if (!subject.isReadOnly()) {
                subject.getPrincipals().clear();
                subject.getPublicCredentials().clear();
            }
            success = true;
        }
        // TODO: check if state should be cleared
        return success;
    }

    @Override
    public boolean abort() throws LoginException {
        clearState();
        return true;
    }

    //--------------------------------------------------------------------------
    /**
     * Clear state information that has been created during {@link #login()}.
     */
    protected void clearState() {
        securityProvider = null;
        root = null;
    }

    @Nonnull
    protected abstract Set<Class> getSupportedCredentials();


    @CheckForNull
    protected Credentials getCredentials() {
        Set<Class> supported = getSupportedCredentials();
        if (callbackHandler != null) {
            log.debug("Login: retrieving Credentials using callback.");
            try {
                CredentialsCallback callback = new CredentialsCallback();
                callbackHandler.handle(new Callback[]{callback});
                Credentials creds = callback.getCredentials();
                if (creds != null && supported.contains(creds.getClass())) {
                    log.debug("Login: Credentials '{}' obtained from callback", creds);
                    return creds;
                } else {
                    log.debug("Login: No supported credentials obtained from callback; trying shared state.");
                }
            } catch (UnsupportedCallbackException e) {
                log.warn(e.getMessage());
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }

        Credentials creds = getSharedCredentials();
        if (creds != null && supported.contains(creds.getClass())) {
            log.debug("Login: Credentials obtained from shared state.");
            return creds;
        } else {
            log.debug("Login: No supported credentials found in shared state; looking for credentials in subject.");
            for (Class clz : getSupportedCredentials()) {
                Set<Credentials> cds = subject.getPublicCredentials(clz);
                if (!cds.isEmpty()) {
                    log.debug("Login: Credentials found in subject.");
                    return cds.iterator().next();
                }
            }
        }

        log.debug("No credentials found.");
        return null;
    }

    @CheckForNull
    protected Credentials getSharedCredentials() {
        Credentials shared = null;
        if (sharedState.containsKey(SHARED_KEY_CREDENTIALS)) {
            Object sc = sharedState.get(SHARED_KEY_CREDENTIALS);
            if (sc instanceof Credentials) {
                shared = (Credentials) sc;
            } else {
                log.debug("Login: Invalid value for share state entry " + SHARED_KEY_CREDENTIALS + ". Credentials expected.");
            }
        }

        return shared;
    }

    @CheckForNull
    protected String getSharedLoginName() {
        if (sharedState.containsKey(SHARED_KEY_LOGIN_NAME)) {
            return (String) sharedState.get(SHARED_KEY_LOGIN_NAME);
        } else {
            return null;
        }
    }


    @Nonnull
    protected Set<? extends Principal> getPrincipals(String userID) {
        PrincipalProvider principalProvider = getPrincipalProvider();
        if (principalProvider == null) {
            log.debug("Cannot retrieve principals. No principal provider configured.");
            return Collections.emptySet();
        } else {
            return principalProvider.getPrincipals(userID);
        }
    }

    @CheckForNull
    protected PrincipalProvider getPrincipalProvider() {
        PrincipalProvider principalProvider = null;
        SecurityProvider sp = getSecurityProvider();
        Root root = getRoot();
        if (root != null && sp != null) {
            principalProvider = sp.getPrincipalConfiguration().getPrincipalProvider(root, NamePathMapper.DEFAULT);
        }

        if (principalProvider == null && callbackHandler != null) {
            try {
                PrincipalProviderCallback principalCallBack = new PrincipalProviderCallback();
                callbackHandler.handle(new Callback[] {principalCallBack});
                principalProvider = principalCallBack.getPrincipalProvider();
            } catch (IOException e) {
                log.debug(e.getMessage());
            } catch (UnsupportedCallbackException e) {
                log.debug(e.getMessage());
            }
        }
        return principalProvider;
    }

    @CheckForNull
    protected UserProvider getUserProvider() {
        SecurityProvider sp = getSecurityProvider();
        Root root = getRoot();
        if (root != null && sp != null) {
            return sp.getUserContext().getUserProvider(root);
        } else {
            return null;
        }
    }

    @CheckForNull
    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null && callbackHandler != null) {
            SecurityProviderCallback scb = new SecurityProviderCallback();
            try {
                callbackHandler.handle(new Callback[] {scb});
                securityProvider = scb.getSecurityProvider();
            } catch (UnsupportedCallbackException e) {
                log.debug(e.getMessage());
            } catch (IOException e) {
                log.debug(e.getMessage());
            }
        }
        return securityProvider;
    }

    @CheckForNull
    protected Root getRoot() {
        if (root == null && callbackHandler != null) {
            RepositoryCallback rcb = new RepositoryCallback();
            try {
                callbackHandler.handle(new Callback[] {rcb});
                root = rcb.getRoot();
            } catch (UnsupportedCallbackException e) {
                log.debug(e.getMessage());
            } catch (IOException e) {
                log.debug(e.getMessage());
            }
        }
        return root;
    }
}
