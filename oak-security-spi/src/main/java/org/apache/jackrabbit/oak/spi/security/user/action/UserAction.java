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
package org.apache.jackrabbit.oak.spi.security.user.action;

import java.security.Principal;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The {@code UserAction} interface allows for implementations to be informed
 * about and react to the following changes to a {@link User}:
 *
 * <ul>
 * <li>{@link #onDisable(User, String, Root, NamePathMapper)}</li>
 * </ul>
 *
 * <p>
 * See {@link AuthorizableAction} for details on persisting changes,
 * configuring actions and the API through which actions are invoked.
 * </p>
 *
 * @since OAK 1.10
 */
public interface UserAction extends AuthorizableAction {

    /**
     * Allows to add application specific behavior associated with disabling (or
     * re-enabling) an user.
     *
     * @param user The user to be disabled or re-enabled.
     * @param disableReason The reason passed to {@link User#disable(String)} or {@code null} if the user is to be enabled again.
     * @param root The root associated with the user manager.
     * @param namePathMapper The mapper associated with the user manager.
     * @throws RepositoryException If an error occurs.
     */
    void onDisable(@NotNull User user, @Nullable String disableReason, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException;

    /**
     * Allows to add application specific behavior associated with granting a given
     * principal the ability to impersonate the user.
     *
     * @param user The user associated with the given {@link org.apache.jackrabbit.api.security.user.Impersonation#grantImpersonation(Principal)} call.
     * @param principal The target principal to be granted impersonation.
     * @param root The root associated with the user manager.
     * @param namePathMapper The mapper associated with the user manager.
     * @throws RepositoryException If an error occurs.
     */
    void onGrantImpersonation(@NotNull User user, @NotNull Principal principal, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException;

    /**
     * Allows to add application specific behavior associated with revoking a given
     * principal the ability to impersonate the user.
     *
     * @param user The user associated with the given {@link org.apache.jackrabbit.api.security.user.Impersonation#revokeImpersonation(Principal)} call.
     * @param principal The target principal for which impersonation is revoked.
     * @param root The root associated with the user manager.
     * @param namePathMapper The mapper associated with the user manager.
     * @throws RepositoryException If an error occurs.
     */
    void onRevokeImpersonation(@NotNull User user, @NotNull Principal principal, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException;
}