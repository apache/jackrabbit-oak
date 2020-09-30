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
package org.apache.jackrabbit.api.security.user;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;
import javax.jcr.Credentials;

/**
 * User is a special {@link Authorizable} that can be authenticated and
 * impersonated.
 *
 * @see #getCredentials()
 * @see #getImpersonation()
 */
public interface User extends Authorizable {

    /**
     * @return true if the current user represents the administrator.
     */
    boolean isAdmin();

    /**
     * @return true if the current user represents a system user.
     */
    boolean isSystemUser();

    /**
     * Returns the internal <code>Credentials</code> representation for this
     * user. This method is expected to be used for validation during the
     * login process. However, the return value should neither be usable nor
     * used for {@link javax.jcr.Repository#login}.
     *
     * @return <code>Credentials</code> for this user.
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    @NotNull
    Credentials getCredentials() throws RepositoryException;

    /**
     * @return <code>Impersonation</code> for this <code>User</code>.
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    @NotNull
    Impersonation getImpersonation() throws RepositoryException;

    /**
     * Change the password of this user.
     *
     * @param password The new password.
     * @throws RepositoryException If an error occurs.
     */
    void changePassword(@Nullable String password) throws RepositoryException;

    /**
     * Change the password of this user.
     *
     * @param password The new password.
     * @param oldPassword The old password.
     * @throws RepositoryException If the old password doesn't match or if
     * an error occurs.
     */
    void changePassword(@Nullable String password, @NotNull String oldPassword) throws RepositoryException;

    /**
     * Disable this user thus preventing future login if the <code>reason</code>
     * is a non-null String.<br>
     * Note however, that this user will still be accessible by
     * {@link UserManager#getAuthorizable}.
     *
     * @param reason String describing the reason for disable this user or
     * <code>null</code> if the user account should be enabled again.
     * @throws RepositoryException If an error occurs.
     */
    void disable(@Nullable String reason) throws RepositoryException;

    /**
     * Returns <code>true</code> if this user is disabled, <code>false</code>
     * otherwise.
     *
     * @return <code>true</code> if this user is disabled, <code>false</code>
     * otherwise.
     * @throws RepositoryException If an error occurs.
     */
    boolean isDisabled() throws RepositoryException;

    /**
     * Returns the String specified upon disabling this user or <code>null</code>
     * if {@link #isDisabled()} returns <code>false</code>.
     * 
     * @return The reason specified upon disabling this user or <code>null</code>
     * if this user is not disabled.
     * @throws RepositoryException If an error occurs.
     */
    @Nullable
    String getDisabledReason() throws RepositoryException;
}
