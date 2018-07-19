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

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract implementation of the {@code AuthorizableAction} interface that
 * doesn't perform any action. This is a convenience implementation allowing
 * subclasses to only implement methods that need extra attention.
 */
public abstract class AbstractAuthorizableAction implements AuthorizableAction {

    //-------------------------------------------------< AuthorizableAction >---
    /**
     * Doesn't perform any action.
     */
    @Override
    public void init(SecurityProvider securityProvider, ConfigurationParameters config) {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     */
    @Override
    public void onCreate(@NotNull Group group, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     */
    @Override
    public void onCreate(@NotNull User user, @Nullable String password, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     */
    @Override
    public void onRemove(@NotNull Authorizable authorizable, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
        // nothing to do
    }

    /**
     * Doesn't perform any action.
     */
    @Override
    public void onPasswordChange(@NotNull User user, @Nullable String newPassword, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
        // nothing to do
    }
}
