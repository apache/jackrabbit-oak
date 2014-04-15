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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.login.LoginException;

/**
 * {@code ExternalIdentityProvider} defines an interface to an external system that provides users and groups that
 * can be synced with local ones.
 */
public interface ExternalIdentityProvider {

    /**
     * Returns the name of this provider.
     * @return the provider name.
     */
    @Nonnull
    String getName();

    /**
     * Returns the identity for the given reference or {@code null} if it does not exist. The provider should check if
     * the {@link ExternalIdentityRef#getProviderName() provider name} matches his own name or is {@code null} and
     * should not return a foreign identity.
     *
     * @param ref the reference
     * @return an identity or {@code null}
     *
     * @throws ExternalIdentityException if an error occurs.
     */
    @CheckForNull
    ExternalIdentity getIdentity(@Nonnull ExternalIdentityRef ref) throws ExternalIdentityException;

    /**
     * Returns the user for the given (local) id. if the user does not exist {@code null} is returned.
     * @param userId the user id.
     * @return the user or {@code null}
     *
     * @throws ExternalIdentityException if an error occurs.
     */
    @CheckForNull
    ExternalUser getUser(@Nonnull String userId) throws ExternalIdentityException;

    /**
     * Authenticates the user represented by the given credentials and returns it. If the user does not exist in this
     * provider, {@code null} is returned. If the authentication fails, a LoginException is thrown.
     *
     * @param credentials the credentials
     * @return the user or {@code null}
     * @throws ExternalIdentityException if an error occurs
     * @throws javax.security.auth.login.LoginException if the user could not be authenticated
     */
    @CheckForNull
    ExternalUser authenticate(@Nonnull Credentials credentials) throws ExternalIdentityException, LoginException;

    /**
     * Returns the group for the given (local) group name. if the group does not exist {@code null} is returned.
     * @param name the group name
     * @return the group or {@code null}
     *
     * @throws ExternalIdentityException if an error occurs.
     */
    @CheckForNull
    ExternalGroup getGroup(@Nonnull String name) throws ExternalIdentityException;

    /**
     * List all external users.
     * @return an iterator over all external users
     * @throws ExternalIdentityException if an error occurs.
     */
    @Nonnull
    Iterator<ExternalUser> listUsers() throws ExternalIdentityException;

    /**
     * List all external groups.
     * @return an iterator over all external groups
     * @throws ExternalIdentityException if an error occurs.
     */
    @Nonnull
    Iterator<ExternalGroup> listGroups() throws ExternalIdentityException;
}