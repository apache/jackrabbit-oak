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

import java.util.Collection;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Root;

/**
 * The permission store is used to store and provide access control permissions for principals. It is responsible to
 * load and store the permissions in an optimal form in the repository and must not cache them.
 */
interface PermissionStore {

    long DYNAMIC_ALL_BITS = -1;

    /**
     * Loads the permission entries for the given principal and path. if the given {@code entries} is {@code null}, it
     * will be created automatically if needed. If a {@code entries} is given, it will reuse it and the same object is
     * returned. If no entries can be found for the given principal or path, {@code null} is returned.
     *
     * @param entries the permission entries or {@code null}
     * @param principalName name of the principal
     * @param path access controlled path.
     * @return the given {@code entries}, a new collection or {@code null}
     */
    @CheckForNull
    Collection<PermissionEntry> load(@Nullable Collection<PermissionEntry> entries, @Nonnull String principalName, @Nonnull String path);

    @Nonnull
    PrincipalPermissionEntries load(@Nonnull String principalName);

    long getNumEntries(@Nonnull String principalName, long max);

    void flush(@Nonnull Root root);

}
