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
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;

/**
 * TmpPermissionProvider... TODO remove again once permission evaluation works.
 */
class TmpPermissionProvider implements PermissionProvider {

    private final boolean isAdmin;

    public TmpPermissionProvider(@Nonnull Root root, @Nonnull Set<Principal> principals, @Nonnull SecurityProvider securityProvider) {
        isAdmin = principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals);
    }

    @Override
    public void refresh() {
        // nothing to do
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        if (isAdmin) {
            return Collections.singleton("jcr:all");
        } else {
            return Collections.singleton("jcr:read");
        }
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, String... privilegeNames) {
        if (isAdmin) {
            return true;
        } else {
            return privilegeNames != null && privilegeNames.length == 1 && "jcr:read".equals(privilegeNames[0]);
        }
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

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        if (isAdmin) {
            return true;
        } else {
            return Session.ACTION_READ.equals(jcrActions);
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