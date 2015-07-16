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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;

/**
 * Extension of the {@link PermissionProvider} interface that allows it to be
 * used in combination with other provider implementations.
 *
 * TODO This is work in progress (OAK-1268)
 */
public interface AggregatedPermissionProvider extends PermissionProvider {

    PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits);

    long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions);

    long supportedPermissions(@Nonnull TreeLocation location, long permissions);

    long supportedPermissions(@Nonnull TreePermission treePermission, long permissions);

    /**
     * Test if the specified permissions are granted for the set of {@code Principal}s
     * associated with this provider instance for the item identified by the
     * given {@code location} and optionally property. This method will only return {@code true}
     * if all permissions are granted.
     *
     * @param location The {@code TreeLocation} to test the permissions for.
     * @param permissions The permissions to be tested.
     * @return {@code true} if the specified permissions are granted for the existing
     * or non-existing item identified by the given location.
     */
    boolean isGranted(@Nonnull TreeLocation location, long permissions);
}