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
package org.apache.jackrabbit.oak.jcr.security;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;

/**
 * AccessManager
 */
public class AccessManager {

    private final SessionDelegate delegate;
    private final PermissionProvider permissionProvider;

    public AccessManager(SessionDelegate delegate, PermissionProvider permissionProvider) {
        this.delegate = delegate;
        this.permissionProvider = permissionProvider;
    }

    public boolean hasPermissions(@Nonnull final String oakPath, @Nonnull final String actions) {
        return delegate.safePerform(new SessionOperation<Boolean>("hasPermissions") {
            @Nonnull
            @Override
            public Boolean perform() {
                return permissionProvider.isGranted(oakPath, actions);
            }
        });
    }

    public boolean hasPermissions(@Nonnull final Tree tree, @Nullable final PropertyState property, final long permissions) throws RepositoryException {
        return delegate.safePerform(new SessionOperation<Boolean>("hasPermissions") {
            @Nonnull
            @Override
            public Boolean perform() {
                return permissionProvider.isGranted(tree, property, permissions);
            }
        });
    }

    public void checkPermissions(@Nonnull String oakPath, @Nonnull String actions) throws RepositoryException {
        if (!hasPermissions(oakPath, actions)) {
            throw new AccessDeniedException("Access denied.");
        }
    }

    public void checkPermissions(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) throws RepositoryException {
        if (!hasPermissions(tree, property, permissions)) {
            throw new AccessDeniedException("Access denied.");
        }
    }
}
