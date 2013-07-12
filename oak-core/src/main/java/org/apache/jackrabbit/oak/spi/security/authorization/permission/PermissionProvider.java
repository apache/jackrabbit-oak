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
 * PermissionProvider... TODO
 */
public interface PermissionProvider {

    /**
     *
     */
    void refresh();

    /**
     *
     * @param tree
     * @return
     */
    @Nonnull
    Set<String> getPrivileges(@Nullable Tree tree);

    /**
     *
     * @param tree
     * @param privilegeNames
     * @return
     */
    boolean hasPrivileges(@Nullable Tree tree, String... privilegeNames);

    /**
     *
     * @param tree
     * @param property
     * @return
     */
    ReadStatus getReadStatus(@Nonnull Tree tree, @Nullable PropertyState property);

    /**
     * Returns {@code true} if the specified repository level permissions are
     * {@code granted}; false otherwise.
     *
     * @param repositoryPermissions Any valid repository level permission such as
     * for example:
     * <ul>
     *     <li>{@link Permissions#NAMESPACE_MANAGEMENT}</li>
     *     <li>{@link Permissions#NODE_TYPE_DEFINITION_MANAGEMENT}</li>
     *     <li>{@link Permissions#PRIVILEGE_MANAGEMENT}</li>
     *     <li>{@link Permissions#WORKSPACE_MANAGEMENT}</li>
     * </ul>
     * @return {@code true} if the specified repository level permissions are
     * {@code granted}; false otherwise.
     */
    boolean isGranted(long repositoryPermissions);

    /**
     *
     * @param parent
     * @param property
     * @param permissions
     * @return
     */
    boolean isGranted(@Nonnull Tree parent, @Nullable PropertyState property, long permissions);

    /**
     *
     * @param oakPath
     * @param jcrActions
     * @return
     */
    boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions);
}
