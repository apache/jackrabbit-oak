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
import javax.jcr.Credentials;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.TokenProviderCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code LoginModule} implementation that is able to handle login request
 * based on {@link TokenCredentials}.
 */
public class TokenLoginModule extends AbstractLoginModule {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(TokenLoginModule.class);

    private TokenProvider tokenProvider;

    private TokenCredentials tokenCredentials;
    private TokenInfo tokenInfo;
    private String userID;
    private Set<? extends Principal> principals;

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
                userID = tokenInfo.getUserId();
                principals = getPrincipals(userID);

                log.debug("Login: adding login name to shared state.");
                sharedState.put(SHARED_KEY_LOGIN_NAME, userID);
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean commit() throws LoginException {
        if (tokenCredentials != null) {
            if (!subject.isReadOnly()) {
                subject.getPublicCredentials().add(tokenCredentials);
                subject.getPrincipals().addAll(principals);
                subject.getPublicCredentials().add(getAuthInfo(tokenInfo));
            }
            return true;
        }

        if (tokenProvider != null && sharedState.containsKey(SHARED_KEY_CREDENTIALS)) {
            Credentials shared = getSharedCredentials();
            if (shared != null && tokenProvider.doCreateToken(shared)) {
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
                    subject.getPublicCredentials().add(tc);
                }
            }
        }

        return false;
    }

    @Override
    public boolean abort() throws LoginException {
        tokenCredentials = null;
        principals = null;
        return true;
    }

    //------------------------------------------------< AbstractLoginModule >---
    @Override
    protected Set<Class> getSupportedCredentials() {
        return Collections.<Class>singleton(TokenCredentials.class);
    }

    //--------------------------------------------------------------------------
    private TokenProvider getTokenProvider() {
        TokenProvider provider = null;
        SecurityProvider securityProvider = getSecurityProvider();
        Root root = getRoot();
        if (root != null && securityProvider != null) {
            provider = securityProvider.getTokenProvider(root, options);
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

    private AuthInfo getAuthInfo(TokenInfo tokenInfo) {
        Map<String, Object> attributes = new HashMap<String, Object>();
        if (tokenProvider != null && tokenInfo != null) {
            Map<String, String> publicAttributes = tokenInfo.getPublicAttributes();
            for (String attrName : publicAttributes.keySet()) {
                attributes.put(attrName, publicAttributes.get(attrName));
            }
        }
        return new AuthInfoImpl(userID, attributes, principals);
    }
}
