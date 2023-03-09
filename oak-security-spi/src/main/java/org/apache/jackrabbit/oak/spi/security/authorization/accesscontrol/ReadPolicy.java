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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.security.NamedAccessControlPolicy;
import java.util.Collection;

public final class ReadPolicy implements NamedAccessControlPolicy {

    public static final NamedAccessControlPolicy INSTANCE = new ReadPolicy();

    private ReadPolicy() {
    }

    @Override
    public String getName() {
        return "Grants read access on configured trees.";
    }
    
    public static boolean hasEffectiveReadPolicy(@NotNull Collection<String> readPaths, @Nullable String oakPath) {
        if (oakPath == null) {
            return false;
        }
        if (readPaths.contains(oakPath)) {
            return true;
        }
        for (String rp : readPaths) {
            if (Text.isDescendant(rp, oakPath)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Evaluates if a {@code ReadPolicy} is accessible for a session object. Note that this method does not verify if 
     * the specified paths point to existing/accessible trees.
     * 
     * @param permissionProvider A permission provider used for evaluating access
     * @param oakPaths The set of configured readable paths.
     * @return {@code true} if the given permission provider has READ_ACCESS_CONTROL granted on any of the specified 
     * readable oak paths; {@code false} otherwise.
     */
    public static boolean canAccessReadPolicy(@NotNull PermissionProvider permissionProvider, @NotNull String... oakPaths) {
        for (String path : oakPaths) {
            if (permissionProvider.isGranted(path, Permissions.PERMISSION_NAMES.get(Permissions.READ_ACCESS_CONTROL))) {
                return true;
            }
        }
        return false;
    }
}