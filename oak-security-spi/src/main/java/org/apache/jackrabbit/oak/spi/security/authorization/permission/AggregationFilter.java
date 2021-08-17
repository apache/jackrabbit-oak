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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.Set;

public interface AggregationFilter {

    /**
     * Determine if permission evaluation for the given set of principals should stop after the give 
     * {@code permissionProvider} completed it's evaluation.
     * 
     * @param permissionProvider An aggregated permission provider instance.
     * @param principals The set of principals for which permissions are being evaluated.
     * @return {@code true} if aggregation of permission providers should be stopped after the given {@code permissionProvider}
     * created for the given set of {@code principals}.
     */
    boolean stop(@NotNull AggregatedPermissionProvider permissionProvider, @NotNull Set<Principal> principals);

    /**
     * Determine if computing effective access control policies for the given set of principals should stop after the 
     * given {@code accessControlManager} completed.
     * 
     * @param accessControlManager An access control manager.
     * @param principals The set of {@link Principal}s for which effective policies are being computed.
     * @return {@code true} if aggregation of effective policies for the specified principals should be stopped after
     * the given {@code accessControlManager}.
     * @see AccessControlManager#getEffectivePolicies(String)
     */
    boolean stop(@NotNull JackrabbitAccessControlManager accessControlManager, @NotNull Set<Principal> principals);

    /**
     * Determine if computing effective access control policies for the given path should stop after the given 
     * {@code accessControlManager} completed.
     * 
     * @param accessControlManager An access control manager.
     * @param absPath An absolute path.
     * @return {@code true} if aggregation of effective policies for the specified effective path should be stopped after
     * the given {@code accessControlManager}.
     * @see JackrabbitAccessControlManager#getEffectivePolicies(Set)
     */
    boolean stop(@NotNull AccessControlManager accessControlManager, @Nullable String absPath);

    /**
     * Default implementation of the {@code AggregationFilter} interface that handles all combinations of permission
     * providers and principals and never aborts the evaluation.
     */
    AggregationFilter DEFAULT = new AggregationFilter() {
        @Override
        public boolean stop(@NotNull AggregatedPermissionProvider permissionProvider, @NotNull Set<Principal> principals) {
            return false;
        }

        @Override
        public boolean stop(@NotNull JackrabbitAccessControlManager accessControlManager, @NotNull Set<Principal> principals) {
            return false;
        }

        @Override
        public boolean stop(@NotNull AccessControlManager accessControlManager, @Nullable String absPath) {
            return false;
        }
    };
}