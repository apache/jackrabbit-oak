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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.TokenProviderCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code LoginModule} implementation that is able to handle login request
 * based on {@link TokenCredentials}. In combination with another login module
 * that handles other {@code Credentials} implementation this module will also
 * take care of creating new login tokens and the corresponding credentials
 * upon {@link #commit()}that it will be able to deal with in subsequent
 * login calls.
 *
 * <h2>Login and Commit</h2>
 * <h3>Login</h3>
 * This {@code LoginModule} implementation performs the following tasks upon
 * {@link #login()}.
 *
 * <ol>
 *     <li>Try to retrieve {@link TokenCredentials} credentials (see also
 *     {@link AbstractLoginModule#getCredentials()})</li>
 *     <li>Validates the credentials based on the functionality provided by
 *     {@link TokenAuthentication#authenticate(javax.jcr.Credentials)}</li>
 *     <li>Upon success it retrieves {@code userId} from the {@link org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo}
 *     and calculates the principals associated with that user,</li>
 *     <li>and finally puts the credentials on the shared state.</li>
 * </ol>
 *
 * If no {@code TokenProvider} has been configured {@link #login()} or if
 * no {@code TokenCredentials} can be obtained this module will return {@code false}.
 *
 * <h3>Commit</h3>
 * If login was successfully handled by this module the {@link #commit()} will
 * just populate the subject.<p>
 *
 * If the login was successfully handled by another module in the chain, the
 * {@code TokenLoginModule} will test if the login was associated with a
 * request for login token generation. This mandates that there are credentials
 * present on the shared state that fulfill the requirements defined by
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider#doCreateToken(javax.jcr.Credentials)}.
 *
 * <h3>Example Configurations</h3>
 * The authentication configuration using this {@code LoginModule} could for
 * example look as follows:
 *
 * <h4>TokenLoginModule in combination with another LoginModule</h4>
 * <pre>
 *    jackrabbit.oak {
 *            org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule sufficient;
 *            org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl required;
 *    };
 * </pre>
 * In this case the TokenLoginModule would handle any login issued with
 * {@link TokenCredentials} while the second module would take care any other
 * credentials implementations as long they are supported by the module. In
 * addition the {@link TokenLoginModule} will issue a new token if the login
 * succeeded and the credentials provided by the shared state can be used
 * to issue a new login token (see {@link org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider#doCreateToken(javax.jcr.Credentials)}.
 *
 * <h4>TokenLoginModule as single way to login</h4>
 * <pre>
 *    jackrabbit.oak {
 *            org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule required;
 *    };
 * </pre>
 * If the {@code TokenLoginModule} as single entry in the login configuration
 * the login token must be generated by the application by calling
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider#createToken(Credentials)} or
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider#createToken(String, java.util.Map)}.
 */
public final class TokenLoginModule extends AbstractLoginModule {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(TokenLoginModule.class);

    private TokenProvider tokenProvider;

    private TokenCredentials tokenCredentials;
    private TokenInfo tokenInfo;
    private String userId;
    private Principal principal;

    //--------------------------------------------------------< LoginModule >---
    @Override
    public boolean login() throws LoginException {
        tokenProvider = getTokenProvider();
        if (tokenProvider == null) {
            return false;
        }

        Credentials credentials = getCredentials();
        if (credentials instanceof TokenCredentials) {
            TokenCredentials tc = (TokenCredentials) credentials;
            TokenAuthentication authentication = new TokenAuthentication(tokenProvider);
            if (authentication.authenticate(tc)) {
                tokenCredentials = tc;
                tokenInfo = authentication.getTokenInfo();
                userId = authentication.getUserId();
                principal = authentication.getUserPrincipal();

                log.debug("Login: adding login name to shared state.");
                sharedState.put(SHARED_KEY_LOGIN_NAME, userId);
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean commit() throws LoginException {
        if (tokenCredentials != null && userId != null) {
            Set<? extends Principal> principals = (principal != null) ? getPrincipals(principal) : getPrincipals(userId);
            updateSubject(tokenCredentials, getAuthInfo(tokenInfo, principals), principals);
            return true;
        }
        try{
            if (tokenProvider != null && sharedState.containsKey(SHARED_KEY_CREDENTIALS)) {
                Credentials shared = getSharedCredentials();
                if (shared != null && tokenProvider.doCreateToken(shared)) {
                    Root r = getRoot();
                    if (r != null) {
                        r.refresh(); // refresh root, in case the external login module created users
                    }
                    TokenInfo ti = tokenProvider.createToken(shared);
                    if (ti != null) {
                        TokenCredentials tc = new TokenCredentials(ti.getToken());
                        Map<String, String> attributes = ti.getPrivateAttributes();
                        for (String name : attributes.keySet()) {
                            tc.setAttribute(name, attributes.get(name));
                        }
                        attributes = ti.getPublicAttributes();
                        for (String name : attributes.keySet()) {
                            tc.setAttribute(name, attributes.get(name));
                        }
                        sharedState.put(SHARED_KEY_ATTRIBUTES, attributes);
                        updateSubject(tc, null, null);
                    } else {
                        // failed to create token -> fail commit()
                        Object logId = (userId != null) ? userId : sharedState.get(SHARED_KEY_LOGIN_NAME);
                        log.debug("TokenProvider failed to create a login token for user " + logId);
                        throw new LoginException("Failed to create login token for user " + logId);
                    }
                }
            }
        } finally {
            // the login attempt on this module did not succeed: clear state
            clearState();
        }

        return false;
    }

    //------------------------------------------------< AbstractLoginModule >---
    @Nonnull
    @Override
    protected Set<Class> getSupportedCredentials() {
        return Collections.<Class>singleton(TokenCredentials.class);
    }

    @Override
    protected void clearState() {
        super.clearState();

        tokenCredentials = null;
        tokenInfo = null;
        userId = null;
    }

    //------------------------------------------------------------< private >---

    /**
     * Retrieve the token provider
     * @return the token provider or {@code null}.
     */
    @CheckForNull
    private TokenProvider getTokenProvider() {
        TokenProvider provider = null;
        SecurityProvider securityProvider = getSecurityProvider();
        Root root = getRoot();
        if (root != null && securityProvider != null) {
            TokenConfiguration tokenConfig = securityProvider.getConfiguration(TokenConfiguration.class);
            provider = tokenConfig.getTokenProvider(root);
        }
        if (provider == null && callbackHandler != null) {
            try {
                TokenProviderCallback tcCallback = new TokenProviderCallback();
                callbackHandler.handle(new Callback[] {tcCallback});
                provider = tcCallback.getTokenProvider();
            } catch (IOException e) {
                log.warn(e.getMessage());
            } catch (UnsupportedCallbackException e) {
                log.warn(e.getMessage());
            }
        }
        return provider;
    }

    /**
     * Create the {@code AuthInfo} for the specified {@code tokenInfo} as well as
     * userId and principals, that have been set upon {@link #login}.
     *
     * @param tokenInfo The tokenInfo to retrieve attributes from.
     * @return The {@code AuthInfo} resulting from the successful login.
     */
    @CheckForNull
    private AuthInfo getAuthInfo(@Nullable TokenInfo tokenInfo, @Nonnull Set<? extends Principal> principals) {
        if (tokenInfo != null) {
            Map<String, Object> attributes = new HashMap<String, Object>();
            Map<String, String> publicAttributes = tokenInfo.getPublicAttributes();
            for (String attrName : publicAttributes.keySet()) {
                attributes.put(attrName, publicAttributes.get(attrName));
            }
            return new AuthInfoImpl(tokenInfo.getUserId(), attributes, principals);
        } else {
            return null;
        }
    }

    private void updateSubject(@Nonnull TokenCredentials tc, @Nullable AuthInfo authInfo,
                               @Nullable Set<? extends Principal> principals) {
        if (!subject.isReadOnly()) {
            subject.getPublicCredentials().add(tc);

            if (principals != null) {
                subject.getPrincipals().addAll(principals);
            }

            if (authInfo != null) {
                setAuthInfo(authInfo, subject);
            }
        }
    }
}
