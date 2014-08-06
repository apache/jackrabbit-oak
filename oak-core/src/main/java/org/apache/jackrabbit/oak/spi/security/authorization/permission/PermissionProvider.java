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

import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * Main entry point for permission evaluation in Oak. This provider covers
 * permission validation upon read and write access on the Oak API as well as
 * the various permission related methods defined by the JCR API, namely on
 * {@link javax.jcr.security.AccessControlManager} and {@link javax.jcr.Session}.
 *
 * @see org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration#getPermissionProvider(org.apache.jackrabbit.oak.api.Root, String, java.util.Set)
 */
public interface PermissionProvider {

    /**
     * Refresh this {@code PermissionProvider}. The implementation is expected
     * to subsequently return permission evaluation results that reflect the
     * most recent revision of the repository.
     */
    void refresh();

    /**
     * Returns the set of privilege names which are granted to the set of
     * {@code Principal}s associated with this provider instance for the
     * specified {@code Tree}.
     *
     * @param tree The {@code tree} for which the privileges should be retrieved.
     * @return set of privilege names
     */
    @Nonnull
    Set<String> getPrivileges(@Nullable Tree tree);

    /**
     * Returns whether the principal set associated with this {@code PrivilegeManager}
     * is granted the privileges identified by the specified privilege names
     * for the given {@code tree}. In order to test for privileges being granted
     * on a repository level rather than on a particular tree a {@code null} tree
     * should be passed to this method.
     *
     * <p>
     * Testing a name identifying an aggregate privilege is equivalent to testing
     * each non aggregate privilege name.
     * </p>
     *
     * @param tree The tree to test for privileges being granted.
     * @param privilegeNames The name of the privileges.
     * @return {@code true} if all privileges are granted; {@code false} otherwise.
     */
    boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames);

    /**
     * Return the {@code RepositoryPermission} for the set of {@code Principal}s
     * associated with this provider instance.
     *
     * @return The {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission}
     * for the set of {@code Principal}s this provider instance has been created for.
     */
    @Nonnull
    RepositoryPermission getRepositoryPermission();

    /**
     * Return the {@code TreePermission} for the set of {@code Principal}s associated
     * with this provider at the specified {@code tree}.
     *
     * @param tree The tree for which the {@code TreePermission} object should be built.
     * @param parentPermission The {@code TreePermission} object that has been
     * obtained before for the parent tree.
     * @return The {@code TreePermission} object for the specified {@code tree}.
     */
    @Nonnull
    TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission);

    /**
     * Test if the specified permissions are granted for the set of {@code Principal}s
     * associated with this provider instance for the item identified by the
     * given tree and optionally property. This method will only return {@code true}
     * if all permissions are granted.
     *
     * @param tree The {@code Tree} to test the permissions for.
     * @param property A {@code PropertyState} if the item to test is a property
     * or {@code null} if the item is a {@code Tree}.
     * @param permissions The permissions to be tested.
     * @return {@code true} if the specified permissions are granted for the item identified
     * by the given tree and optionally property state.
     */
    boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions);

    /**
     * Tests if the the specified actions are granted at the given path for
     * the set of {@code Principal}s associated with this provider instance.
     * <p>
     * The {@code jcrActions} parameter is a comma separated list of action
     * strings such as defined by {@link javax.jcr.Session} and passed to
     * {@link javax.jcr.Session#hasPermission(String, String)}. When more than one
     * action is specified in the {@code jcrActions} parameter, this method will
     * only return {@code true} if all of them are granted on the specified path.
     * </p>
     *
     * @param oakPath A valid oak path.
     * @param jcrActions The JCR actions that should be tested separated by ','
     * @return {@code true} if all actions are granted at the specified path;
     * {@code false} otherwise.
     */
    boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions);
}
