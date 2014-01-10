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
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * The permission store is used to store and provide access control permissions for principals. It is responsible to
 * load and store the permissions in an optimal form in the repository and must not cache them.
 */
public interface PermissionStore {

    void load(@Nonnull Collection<PermissionEntry> entries, @Nonnull String principalName, @Nonnull String path);

    void load(@Nonnull Map<String, Collection<PermissionEntry>> entries, @Nonnull String principalName);

    @Nonnull
    PrincipalPermissionEntries load(@Nonnull String principalName);

    boolean hasPermissionEntries(@Nonnull String principalName);

    long getNumEntries(@Nonnull String principalName);

    long getModCount(@Nonnull String principalName);
}
