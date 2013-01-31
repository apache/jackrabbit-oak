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
package org.apache.jackrabbit.oak.spi.security.authorization;

import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * PermissionProvider... TODO
 */
public interface PermissionProvider {

    @Nonnull
    Set<String> getPrivilegeNames(@Nullable Tree tree);

    boolean hasPrivileges(@Nullable Tree tree, String... privilegeNames);

    boolean canRead(@Nonnull Tree tree);

    boolean canRead(@Nonnull Tree tree, @Nonnull PropertyState property);

    boolean isGranted(long permissions);

    boolean isGranted(@Nonnull Tree tree, long permissions);

    boolean isGranted(@Nonnull Tree parent, @Nonnull PropertyState property, long permissions);

    boolean hasPermission(@Nonnull String oakPath, @Nonnull String jcrActions);
}
