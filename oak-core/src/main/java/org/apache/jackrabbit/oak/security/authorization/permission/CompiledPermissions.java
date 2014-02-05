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
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;

/**
 * CompiledPermissions... TODO
 */
public interface CompiledPermissions {

    void refresh(@Nonnull ImmutableRoot root, @Nonnull String workspaceName);

    RepositoryPermission getRepositoryPermission();

    TreePermission getTreePermission(@Nonnull ImmutableTree tree, @Nonnull TreePermission parentPermission);

    /**
     *
     *
     * @param parent
     * @param property
     * @param permissions
     * @return
     */
    boolean isGranted(@Nonnull ImmutableTree parent, @Nullable PropertyState property, long permissions);

    /**
     *
     * @param path Path of an OAK tree
     * @param permissions
     * @return
     */
    boolean isGranted(@Nonnull String path, long permissions);

    /**
     *
     *
     * @param tree
     * @return
     */
    @Nonnull
    Set<String> getPrivileges(@Nullable ImmutableTree tree);

    /**
     *
     *
     * @param tree
     * @param privilegeNames
     * @return
     */
    boolean hasPrivileges(@Nullable ImmutableTree tree, @Nonnull String... privilegeNames);
}
