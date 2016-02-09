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
package org.apache.jackrabbit.oak.spi.security.authentication.token;

import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;

/**
 * Interface to manage create and manage login tokens.
 */
public interface TokenProvider {

    /**
     * Optional configuration parameter to set the token expiration time in ms.
     * Implementations that do not support this option will ignore any config
     * options with that name.
     */
    String PARAM_TOKEN_EXPIRATION = "tokenExpiration";

    /**
     * Optional configuration parameter to define the length of the key.
     * Implementations that do not support this option will ignore any config
     * options with that name.
     */
    String PARAM_TOKEN_LENGTH = "tokenLength";

    /**
     * Optional configuration parameter to define if a given token should be
     * refreshed or not. Implementations that do not support this option will
     * ignore any config options with that name.
     */
    String PARAM_TOKEN_REFRESH = "tokenRefresh";

    /**
     * Returns {@code true} if the given credentials indicate that a new token
     * needs to be issued.
     *
     * @param credentials The current credentials.
     * @return {@code true} if a new login token needs to be created, {@code false} otherwise.
     */
    boolean doCreateToken(@Nonnull Credentials credentials);

    /**
     * Issues a new login token for the user with the specified credentials
     * and returns the associated {@code TokenInfo}.
     *
     * @param credentials The current credentials.
     * @return The {@code TokenInfo} associated with the new login token or
     * {@code null} if no token has been created.
     */
    @CheckForNull
    TokenInfo createToken(@Nonnull Credentials credentials);

    /**
     * Issues a new login token for the user with the given {@code userId}
     * and the specified attributes.
     *
     * @param userId The identifier of the user for which a new token should
     * be created.
     * @param attributes The attributes associated with the new token.
     * @return The {@code TokenInfo} associated with the new login token or
     * {@code null} if no token has been created.
     */
    @CheckForNull
    TokenInfo createToken(@Nonnull String userId, @Nonnull Map<String,?> attributes);

    /**
     * Retrieves the {@code TokenInfo} associated with the specified login token
     * or {@code null}.
     *
     * @param token A valid login token.
     * @return the {@code TokenInfo} associated with the specified login token
     * or {@code null}.
     */
    @CheckForNull
    TokenInfo getTokenInfo(@Nonnull String token);
}
