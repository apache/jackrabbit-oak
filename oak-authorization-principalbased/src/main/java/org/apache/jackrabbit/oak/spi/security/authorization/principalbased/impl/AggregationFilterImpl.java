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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregationFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.Set;

public class AggregationFilterImpl implements AggregationFilter {
    
    @Override
    public boolean stop(@NotNull AggregatedPermissionProvider permissionProvider, @NotNull Set<Principal> principals) {
        // validation of principals already took place before creating 'PrincipalBasedPermissionProvider'
        return permissionProvider instanceof PrincipalBasedPermissionProvider;
    }

    @Override
    public boolean stop(@NotNull JackrabbitAccessControlManager accessControlManager, @NotNull Set<Principal> principals) {
        try {
            return accessControlManager instanceof PrincipalBasedAccessControlManager && ((PrincipalBasedAccessControlManager) accessControlManager).canHandle(principals);
        } catch (AccessControlException e) {
            return false;
        }
    }

    @Override
    public boolean stop(@NotNull AccessControlManager accessControlManager, @Nullable String absPath) {
        return false;
    }
}