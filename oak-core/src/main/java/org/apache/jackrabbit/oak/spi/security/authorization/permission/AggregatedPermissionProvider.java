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
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;

/**
 * Extension of the {@link PermissionProvider} interface that allows it to be
 * used in combination with other provider implementations.
 */
public interface AggregatedPermissionProvider extends PermissionProvider {

    /**
     * Allows to determined the set or subset of privileges evaluated by the
     * implementing permission provider for the specified tree or at the repository
     * level in case the specified {@code tree} is {@code null}.
     *
     * If the given {@code privilegeBits} is {@code null} an implementation returns
     * the complete set that is covered by the provider; otherwise the supported
     * subset of the specified {@code privilegeBits} is returned.
     *
     * Returning {@link PrivilegeBits#EMPTY} indicates that this implementation
     * is not in charge of evaluating the specified privileges and thus will
     * be ignored while computing the composite result of
     * {@link PermissionProvider#getPrivileges(org.apache.jackrabbit.oak.api.Tree)}
     * or {@link PermissionProvider#hasPrivileges(org.apache.jackrabbit.oak.api.Tree, String...)}.
     *
     * @param tree The tree for which the privileges will be evaluated or {@code null}
     * for repository level privileges.
     * @param privilegeBits The privilege(s) to be tested or {@code null}
     * @return The set of privileges or the subset of the given {@code privilegeBits}
     * that are supported and evaluated by the implementation at the given {@code tree}
     * represented as {@code PrivilegeBits}.
     */
    @Nonnull
    PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits);

    /**
     * Allows to determined the set or subset of permissions evaluated by the
     * implementing permission provider for the specified item (identified by
     * {@code tree} and optionally {@code property}) or at the repository level
     * in case the specified {@code tree} is {@code null}.
     *
     * Returning {@link Permissions#NO_PERMISSION} indicates that this implementation
     * is not in charge of evaluating the specified permissions for the
     * specified item and thus will be ignored while computing the composite
     * result of {@link PermissionProvider#isGranted(Tree, PropertyState, long)}.
     *
     * @param tree The tree for which the permissions will be evaluated or {@code null}
     * for repository level privileges.
     * @param property The target property or {@code null}.
     * @param permissions The permisisons to be tested
     * @return The subset of the given {@code permissions} that are supported and
     * evaluated by the implementation for the given item.
     */
    long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions);

    /**
     * Allows to determined the set or subset of permissions evaluated by the
     * implementing permission provider for the specified location.
     *
     * Returning {@link Permissions#NO_PERMISSION} indicates that this implementation
     * is not in charge of evaluating the specified permissions for the
     * specified location and thus will be ignored while computing the composite
     * result of {@link PermissionProvider#isGranted(String, String)} and
     * {@link AggregatedPermissionProvider#isGranted(TreeLocation, long)}.
     *
     * @param location The tree location for which the permissions will be evaluated.
     * @param permissions The permisisons to be tested
     * @return The subset of the given {@code permissions} that are supported and
     * evaluated by the implementation for the given location.
     */
    long supportedPermissions(@Nonnull TreeLocation location, long permissions);

    /**
     * Allows to determined the set or subset of permissions evaluated by the
     * implementing permission provider for the specified tree permission (plus
     * optionally {@code property}).
     *
     * Returning {@link Permissions#NO_PERMISSION} indicates that this implementation
     * is not in charge of evaluating the specified permissions for the
     * specified tree permission and thus will be ignored while computing the composite
     * result of {@link TreePermission#isGranted(long, PropertyState)} and {@link TreePermission#isGranted(long)}.
     *
     * @param treePermission The target tree permission.
     * @param property The target property or {@code null}.
     * @param permissions The permisisons to be tested
     * @return The subset of the given {@code permissions} that are supported and
     * evaluated by the implementation for the given tree permissions.
     */
    long supportedPermissions(@Nonnull TreePermission treePermission, @Nullable PropertyState property, long permissions);

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

    /**
     * Return the {@code TreePermission} for the set of {@code Principal}s associated
     * with this provider at the specified {@code tree} with the given {@code type}.
     *
     * @param tree The tree for which the {@code TreePermission} object should be built.
     * @param type The type of this tree.
     * @param parentPermission The {@code TreePermission} object that has been
     * obtained before for the parent tree.
     * @return The {@code TreePermission} object for the specified {@code tree}.
     */
    @Nonnull
    TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parentPermission);

}