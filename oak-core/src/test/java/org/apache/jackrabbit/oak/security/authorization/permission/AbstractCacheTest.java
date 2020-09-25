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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;

import static org.mockito.Mockito.mock;

public abstract class AbstractCacheTest {

    static final String EMPTY_CLASS_NAME = "org.apache.jackrabbit.oak.security.authorization.permission.PermissionCacheBuilder$EmptyCache";
    static final String ENTRYMAP_CLASS_NAME = "org.apache.jackrabbit.oak.security.authorization.permission.PermissionCacheBuilder$PathEntryMapCache";
    static final String DEFAULT_CLASS_NAME = "org.apache.jackrabbit.oak.security.authorization.permission.PermissionCacheBuilder$DefaultPermissionCache";

    PermissionStore store;
    PermissionCacheBuilder permissionCacheBuilder;

    @Before
    public void before() {
        store = mock(PermissionStore.class);
        permissionCacheBuilder = new PermissionCacheBuilder(store);
    }

    @NotNull
    static PrincipalPermissionEntries generatedPermissionEntries(@NotNull String path, boolean isAllow, int index, @NotNull String privilegeName) {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(1);
        ppe.putEntriesByPath(path, ImmutableSet.of(new PermissionEntry(path, isAllow, index, PrivilegeBits.BUILT_IN.get(privilegeName), RestrictionPattern.EMPTY)));
        return ppe;
    }

    @NotNull
    static CacheStrategy createStrategy(long maxSize, long maxPaths, boolean isRefresh) {
        return new CacheStrategyImpl(ConfigurationParameters.of(
                CacheStrategyImpl.EAGER_CACHE_SIZE_PARAM, maxSize,
                CacheStrategyImpl.EAGER_CACHE_MAXPATHS_PARAM, maxPaths), isRefresh);
    }
}