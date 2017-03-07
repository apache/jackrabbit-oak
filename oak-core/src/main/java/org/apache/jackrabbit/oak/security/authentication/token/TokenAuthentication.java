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

import java.security.Principal;
import java.util.Date;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@code Authentication} interface that deals with
 * token based login. {@link #authenticate(javax.jcr.Credentials) Authentication}
 * will be successful if the specified credentials are valid {@link TokenCredentials}
 * according to the characteristics and constraints enforced by {@link org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider}
 * and the information obtained using {@link org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider#getTokenInfo(String)}
 * respectively.
 */
class TokenAuthentication implements Authentication {

    private static final Logger log = LoggerFactory.getLogger(TokenAuthentication.class);

    private final TokenProvider tokenProvider;
    private TokenInfo tokenInfo;

    TokenAuthentication(TokenProvider tokenProvider) {
        this.tokenProvider = tokenProvider;
    }

    //-----------------------------------------------------< Authentication >---
    @Override
    public boolean authenticate(@Nullable Credentials credentials) throws LoginException {
        if (tokenProvider != null && credentials instanceof TokenCredentials) {
            TokenCredentials tc = (TokenCredentials) credentials;
            if (!validateCredentials(tc)) {
                throw new LoginException("Invalid token credentials.");
            } else {
                return true;
            }
        }
        // no tokenProvider or other credentials implementation -> not handled here.
        return false;
    }

    @CheckForNull
    @Override
    public String getUserId() {
        if (tokenInfo == null) {
            throw new IllegalStateException("UserId can only be retrieved after successful authentication.");
        }
        return tokenInfo.getUserId();
    }

    @CheckForNull
    @Override
    public Principal getUserPrincipal() {
        if (tokenInfo == null) {
            throw new IllegalStateException("Token info can only be retrieved after successful authentication.");
        }
        if (tokenInfo instanceof TokenProviderImpl.TokenInfoImpl) {
            return ((TokenProviderImpl.TokenInfoImpl) tokenInfo).getPrincipal();
        } else {
            return null;
        }
    }

    //-----------------------------------------------------------< internal >---
    @Nonnull
    TokenInfo getTokenInfo() {
        if (tokenInfo == null) {
            throw new IllegalStateException("Token info can only be retrieved after successful authentication.");
        }
        return tokenInfo;
    }

    //------------------------------------------------------------< private >---
    private boolean validateCredentials(TokenCredentials tokenCredentials) {
        // credentials without userID -> check if attributes provide
        // sufficient information for successful authentication.
        String token = tokenCredentials.getToken();

        tokenInfo = tokenProvider.getTokenInfo(token);
        if (tokenInfo == null) {
            log.debug("No valid TokenInfo for token.");
            return false;
        }

        long loginTime = new Date().getTime();
        if (tokenInfo.isExpired(loginTime)) {
            // token is expired
            log.debug("Token is expired");
            tokenInfo.remove();
            return false;
        }

        if (tokenInfo.matches(tokenCredentials)) {
            tokenInfo.resetExpiration(loginTime);
            return true;
        }

        return false;
    }
}
