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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;

/**
 * Internal interface to process methods defined by
 * {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider}.
 * Depending on the set of {@link java.security.Principal}s a given implementation
 * be may able to simplify the evaluation. See e.g. {@link org.apache.jackrabbit.oak.security.authorization.permission.AllPermissions}
 * and {@link org.apache.jackrabbit.oak.security.authorization.permission.NoPermissions}
 */
interface CompiledPermissions {

    /**
     * Refresh this instance to reflect the permissions as present with the
     * specified {@code Root}.
     *
     *
     * @param root The root
     * @param workspaceName The workspace name.
     * @see {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider#refresh()}
     */
    void refresh(@Nonnull Root root, @Nonnull String workspaceName);

    /**
     * Returns the {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission}
     * associated with the {@code Root} as specified in {@link #refresh(org.apache.jackrabbit.oak.api.Root, String)}
     *
     * @return an instance of {@code RepositoryPermission}.
     * @see {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider#getRepositoryPermission()}
     */
    @Nonnull
    RepositoryPermission getRepositoryPermission();

    /**
     * Returns the tree permissions for the specified {@code tree}.
     *
     *
     * @param tree The tree for which to obtain the permissions.
     * @param parentPermission The permissions as present with the parent.
     * @return The permissions for the specified tree.
     * @see {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission#getChildPermission(String, org.apache.jackrabbit.oak.spi.state.NodeState)}
     */
    @Nonnull
    TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission);

    /**
     * Returns the tree permissions for the specified {@code tree}.
     *
     *
     * @param tree The tree for which to obtain the permissions.
     * @param parentPermission The permissions as present with the parent.
     * @return The permissions for the specified tree.
     * @see {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission#getChildPermission(String, org.apache.jackrabbit.oak.spi.state.NodeState)}
     */
    @Nonnull
    TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parentPermission);

    /**
     * Returns {@code true} if the given {@code permissions} are granted on the
     * item identified by {@code parent} and optionally {@code property}.
     *
     *
     * @param tree The tree (or parent tree) for which the permissions should be evaluated.
     * @param property An optional property state.
     * @param permissions The permissions to be tested.
     * @return {@code true} if granted.
     * @see {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider#isGranted(org.apache.jackrabbit.oak.api.Tree, org.apache.jackrabbit.oak.api.PropertyState, long)}
     */
    boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions);

    /**
     * Returns {@code true} if the given {@code permissions} are granted on the
     * tree identified by the specified {@code path}.
     *
     * @param path Path of a tree
     * @param permissions The permissions to be tested.
     * @return {@code true} if granted.
     * @see {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider#isGranted(String, String)}
     */
    boolean isGranted(@Nonnull String path, long permissions);

    /**
     * Retrieve the privileges granted at the specified {@code tree}.
     *
     *
     * @param tree The tree for which to retrieve the granted privileges.
     * @return the set of privileges or an empty set if no privileges are granted.
     * @see {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider#getPrivileges(org.apache.jackrabbit.oak.api.Tree)}
     */
    @Nonnull
    Set<String> getPrivileges(@Nullable Tree tree);

    /**
     * Returns {@code true} if all privileges identified by the given {@code privilegeNames}
     * are granted at the given {@code tree}.
     *
     *
     * @param tree The target tree.
     * @param privilegeNames The privilege names to be tested.
     * @return {@code true} if the tree has privileges
     */
    boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames);
}
