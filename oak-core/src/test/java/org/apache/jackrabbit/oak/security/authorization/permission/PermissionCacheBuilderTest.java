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
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PermissionCacheBuilderTest {

    private static final int MAX_PATH_SIZE = 10;

    private static final String EMPTY_CLASS_NAME = "org.apache.jackrabbit.oak.security.authorization.permission.PermissionCacheBuilder$EmptyCache";
    private static final String SIMPLE_CLASS_NAME = "org.apache.jackrabbit.oak.security.authorization.permission.PermissionCacheBuilder$PathEntryMapCache";
    private static final String DEFAULT_CLASS_NAME = "org.apache.jackrabbit.oak.security.authorization.permission.PermissionCacheBuilder$DefaultPermissionCache";

    private PermissionStore store;
    private PermissionCacheBuilder permissionCacheBuilder;

    @Before
    public void before() {
        store = mock(PermissionStore.class);
        permissionCacheBuilder = new PermissionCacheBuilder(store);
    }

    @NotNull
    private static PrincipalPermissionEntries generatedPermissionEntries(@NotNull String path, boolean isAllow, int index, @NotNull String privilegeName) {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(1);
        ppe.putEntriesByPath(path, ImmutableSet.of(new PermissionEntry(path, isAllow, index, PrivilegeBits.BUILT_IN.get(privilegeName), RestrictionPattern.EMPTY)));
        return ppe;
    }

    @Test(expected = IllegalStateException.class)
    public void testBuildBeforeInitialized() {
        permissionCacheBuilder.build();
    }

    @Test
    public void testBuildForEmptyPrincipals() {
        assertTrue(permissionCacheBuilder.init(ImmutableSet.of(), Long.MAX_VALUE));
        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(EMPTY_CLASS_NAME, cache.getClass().getName());

        verify(store, never()).getNumEntries(anyString(), anyLong());
        verify(store, never()).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testBuildNoExistingEntries() {
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.ZERO);
        when(store.load(anyString())).thenReturn(new PrincipalPermissionEntries(0));

        Set<String> principalNames = Sets.newHashSet("noEntries", "noEntries2", "noEntries3");

        assertTrue(permissionCacheBuilder.init(principalNames, Long.MAX_VALUE));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(EMPTY_CLASS_NAME, cache.getClass().getName());

        assertTrue(cache.getEntries(PathUtils.ROOT_PATH).isEmpty());
        assertTrue(cache.getEntries(mock(Tree.class)).isEmpty());

        verify(store, times(3)).getNumEntries(anyString(), anyLong());
        verify(store, never()).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testBuildFewEntriesSamePath() {
        PrincipalPermissionEntries ppeA = generatedPermissionEntries("/path", false, 0, REP_READ_NODES);
        PrincipalPermissionEntries ppeB = generatedPermissionEntries("/path",false, 1, REP_READ_NODES);

        when(store.load("a")).thenReturn(ppeA);
        when(store.load("b")).thenReturn(ppeB);
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, true));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        assertFalse(permissionCacheBuilder.init(principalNames, Long.MAX_VALUE));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(SIMPLE_CLASS_NAME, cache.getClass().getName());

        verify(store, times(2)).getNumEntries(anyString(), anyLong());
        verify(store, times(2)).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testBuildFewEntriesDifferentPaths() {
        when(store.load("a")).thenReturn(generatedPermissionEntries("/path1", false, 0, REP_READ_NODES));
        when(store.load("b")).thenReturn(generatedPermissionEntries("/path2", false, 0, REP_READ_NODES));
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, true));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        assertFalse(permissionCacheBuilder.init(principalNames, Long.MAX_VALUE));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(SIMPLE_CLASS_NAME, cache.getClass().getName());

        verify(store, times(2)).getNumEntries(anyString(), anyLong());
        verify(store, times(2)).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testBuildPathEntryMapNonExactCnt() {
        when(store.load("a")).thenReturn(generatedPermissionEntries("/path1",false, 0, REP_READ_NODES));
        when(store.load("b")).thenReturn(generatedPermissionEntries("/path2", true, 0, JCR_MODIFY_ACCESS_CONTROL));
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, false));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        assertFalse(permissionCacheBuilder.init(principalNames, Long.MAX_VALUE));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(SIMPLE_CLASS_NAME, cache.getClass().getName());

        verify(store, times(2)).getNumEntries(anyString(), anyLong());
        verify(store, times(2)).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testBuildPathEntryMapResultsInEmptyCache() {
        when(store.load(anyString())).thenReturn(new PrincipalPermissionEntries());
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(MAX_PATH_SIZE+1, false));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        assertFalse(permissionCacheBuilder.init(principalNames, Long.MAX_VALUE));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(EMPTY_CLASS_NAME, cache.getClass().getName());

        verify(store, times(2)).getNumEntries(anyString(), anyLong());
        verify(store, times(2)).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testBuildMaxEntriesReached() throws Exception {
        PrincipalPermissionEntries ppeA = generatedPermissionEntries("/path1",false, 0, REP_READ_NODES);
        PrincipalPermissionEntries ppeB = generatedPermissionEntries("/path2",false, 0, REP_READ_NODES);

        when(store.load("a")).thenReturn(ppeA);
        when(store.load("b")).thenReturn(ppeB);
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, true));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        long maxSize = 1;
        assertFalse(permissionCacheBuilder.init(principalNames, maxSize));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(DEFAULT_CLASS_NAME, cache.getClass().getName());

        verify(store, times(2)).getNumEntries(anyString(), anyLong());
        verify(store, times(2)).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testInitNumEntriesExceedMaxPathExact() {
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(MAX_PATH_SIZE+1, true));

        assertFalse(permissionCacheBuilder.init(ImmutableSet.of("a", "b", "c"), Long.MAX_VALUE));

        verify(store, times(3)).getNumEntries(anyString(), anyLong());
        verify(store, never()).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testInitNumEntriesExceedMaxPathNotExact() {
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(MAX_PATH_SIZE+1, false));

        assertFalse(permissionCacheBuilder.init(ImmutableSet.of("a", "b", "c"), Long.MAX_VALUE));

        verify(store, times(3)).getNumEntries(anyString(), anyLong());
        verify(store, never()).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testInitNumEntriesExceedsMaxLong() {
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(Long.MAX_VALUE, false));

        assertFalse(permissionCacheBuilder.init(ImmutableSet.of("a", "b", "c"), Long.MAX_VALUE));

        verify(store, times(3)).getNumEntries(anyString(), anyLong());
        verify(store, never()).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }
}