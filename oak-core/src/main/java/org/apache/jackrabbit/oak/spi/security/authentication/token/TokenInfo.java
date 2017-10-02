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
import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;

/**
 * The {@code TokenInfo} provides data associated with a login token and
 * basic methods to verify the validity of token credentials at given
 * point in time.
 */
public interface TokenInfo {

    /**
     * Returns the ID of the user associated with this token info object.
     *
     * @return the ID of the user.
     */
    @Nonnull
    String getUserId();

    /**
     * Returns the login token.
     *
     * @return the login token.
     */
    @Nonnull
    String getToken();

    /**
     * Returns {@code true} if the token has already expired; {@code false} otherwise.
     *
     * @param loginTime The login time used to calculate the expiration status.
     * @return {@code true} if the token has already expired; {@code false} otherwise.
     */
    boolean isExpired(long loginTime);

    /**
     * Resets the expiration time of the login token associated with the given
     * {@code TokenInfo}. Whether and when the expiration time of a given login
     * token is being reset is an implementation detail. Implementations that
     * don't allow for resetting the token's expiration time at all will always
     * return {@code false}.
     *
     * @param loginTime The current login time.
     * @return {@code true} if the expiration time has been reset, false otherwise.
     */
    boolean resetExpiration(long loginTime);

    /**
     * Tries to remove the login token and all related information. This method
     * returns {@code true} if the removal was successful.
     *
     * @return {@code true} if the removal was successful, {@code false} otherwise.
     */
    boolean remove();

    /**
     * Returns {@code true} if the specified credentials can be successfully
     * validated against the information stored in this instance.
     *
     * @param tokenCredentials The credentials to validate.
     * @return {@code true} if the specified credentials can be successfully
     * validated against the information stored in this instance; {@code false}
     * otherwise.
     */
    boolean matches(@Nonnull TokenCredentials tokenCredentials);

    /**
     * Returns the private attributes stored with this info object.
     *
     * @return the private attributes stored with this info object.
     */
    @Nonnull
    Map<String, String> getPrivateAttributes();

    /**
     * Returns the public attributes stored with this info object.
     *
     * @return the public attributes stored with this info object.
     */
    @Nonnull
    Map<String, String> getPublicAttributes();
}
