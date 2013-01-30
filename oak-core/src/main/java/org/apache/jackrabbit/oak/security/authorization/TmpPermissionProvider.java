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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;

/**
 * TmpPermissionProvider... TODO remove again once permission evaluation works.
 */
class TmpPermissionProvider extends PermissionProviderImpl {

    private final boolean isAdmin;

    public TmpPermissionProvider(@Nonnull Root root, @Nonnull Set<Principal> principals, @Nonnull SecurityProvider securityProvider) {
        super(root, principals, securityProvider);
        isAdmin = principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals);
    }

    public TmpPermissionProvider(@Nonnull Tree rootTree, @Nonnull Set<Principal> principals, @Nonnull SecurityProvider securityProvider) {
        super(rootTree, principals, securityProvider);
        isAdmin = principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals);
    }

    @Override
    public boolean canRead(@Nonnull Tree tree) {
        return true;
    }

    @Override
    public boolean canRead(@Nonnull Tree tree, @Nonnull PropertyState property) {
        return true;
    }

    @Override
    public boolean isGranted(long permissions) {
        if (isAdmin) {
            return true;
        } else {
            return permissions == Permissions.READ;
        }
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, long permissions) {
        if (isAdmin) {
            return true;
        } else {
            return permissions == Permissions.READ_NODE;
        }
    }

    @Override
    public boolean isGranted(@Nonnull Tree parent, @Nonnull PropertyState property, long permissions) {
        if (isAdmin) {
            return true;
        } else {
            return permissions == Permissions.READ_PROPERTY;
        }
    }

    private static boolean isAdmin(Set<Principal> principals) {
        for (Principal principal : principals) {
            if (principal instanceof AdminPrincipal) {
                return true;
            }
        }
        return false;
    }
}