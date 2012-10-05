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
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TokenAuthentication... TODO
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
    public boolean authenticate(Credentials credentials) {
        boolean success = false;
        if (credentials instanceof TokenCredentials) {
            TokenCredentials tc = (TokenCredentials) credentials;
            success = validateCredentials(tc);
        }
        return success;
    }

    /**
     * Always returns {@code false}
     */
    @Override
    public boolean impersonate(Set<Principal> principals) {
        return false;
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
            log.debug("Invalid token credentials");
            return false;
        }

        long loginTime = new Date().getTime();
        if (tokenInfo.isExpired(loginTime)) {
            // token is expired
            log.debug("Token is expired");
            tokenProvider.removeToken(tokenInfo);
            return false;
        }

        if (tokenInfo.matches(tokenCredentials)) {
            if (!tokenProvider.resetTokenExpiration(tokenInfo, loginTime)) {
                log.debug("Unable to reset token expiration... trying next time");
            }
            return true;
        }


        return false;
    }
}
