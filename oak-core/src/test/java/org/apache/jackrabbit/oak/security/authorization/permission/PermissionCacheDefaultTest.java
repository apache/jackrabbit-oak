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

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PermissionCacheDefaultTest extends AbstractCacheTest {

    private PermissionCache cache;

    @Before
    public void before() {
        super.before();

        when(store.load("a", "/path1")).thenReturn(createPermissionEntryCollection("/path1", false, REP_READ_NODES));
        when(store.load("b", "/path2")).thenReturn(createPermissionEntryCollection("/path2", true, JCR_MODIFY_ACCESS_CONTROL));
        when(store.load("a", "/path2")).thenReturn(null);
        when(store.load("b", "/path1")).thenReturn(null);
        when(store.load("a", "/another/path")).thenReturn(null);
        when(store.load("b", "/another/path")).thenReturn(null);
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, isExact()));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        assertFalse(permissionCacheBuilder.init(principalNames, createStrategy(1, 0, true)));

        cache = permissionCacheBuilder.build();
        assertEquals(DEFAULT_CLASS_NAME, cache.getClass().getName());

        verify(store, never()).load(anyString());
        clearInvocations(store);
    }

    boolean isExact() {
        return true;
    }

    int loadInvocationCnt() {
        // exact number of access controlled paths is known => store can determine when all entries have been already loaded.
        return 3;
    }

    void verifyByPath() {
        verify(store, times(1)).load("a", "/path1");
        verify(store, times(1)).load("b", "/path1");

        verify(store, times(1)).load("b", "/path2");
        verify(store, never()).load("a", "/path2");

        verify(store, never()).load("a", "/any/other/path");
        verify(store, never()).load("b", "/any/other/path");
    }

    private static Collection<PermissionEntry> createPermissionEntryCollection(@NotNull String path, boolean isAllow, @NotNull String privilegeName) {
        return Collections.singleton(new PermissionEntry(path, isAllow, 0, PrivilegeBits.BUILT_IN.get(privilegeName), RestrictionPattern.EMPTY));
    }

    @Test
    public void testGetEntriesByPath() {
        assertEquals(1, cache.getEntries("/path1").size());
        assertEquals(1, cache.getEntries("/path2").size());
        assertTrue(cache.getEntries("/any/other/path").isEmpty());

        verify(store, never()).load(anyString());
        verify(store, times(loadInvocationCnt())).load(anyString(), anyString());
        verifyByPath();

        // second access reads from cache
        assertEquals(1, cache.getEntries("/path1").size());
        assertEquals(1, cache.getEntries("/path2").size());
        assertTrue(cache.getEntries("/any/other/path").isEmpty());

        verify(store, times(loadInvocationCnt())).load(anyString(), anyString());
        verifyByPath();
    }

    @Test
    public void testGetEntriesByTree() {
        Tree t = when(mock(Tree.class).getPath()).thenReturn("/not/access/controlled").getMock();
        when(t.hasChild(AccessControlConstants.REP_POLICY)).thenReturn(false);

        assertTrue(cache.getEntries(t).isEmpty());

        verify(store, never()).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testGetEntriesByAccessControlledTree() {

        Tree t = when(mock(Tree.class).getPath()).thenReturn("/path1", "/path2", "/any/other/path", "/path1", "/path2", "/any/other/path").getMock();
        when(t.hasChild(AccessControlConstants.REP_POLICY)).thenReturn(true);

        assertEquals(1, cache.getEntries(t).size());
        assertEquals(1, cache.getEntries(t).size());
        assertTrue(cache.getEntries(t).isEmpty());

        verify(store, never()).load(anyString());
        verify(store, times(loadInvocationCnt())).load(anyString(), anyString());
        verifyByPath();

        // second access reads from cache
        assertEquals(1, cache.getEntries(t).size());
        assertEquals(1, cache.getEntries(t).size());
        assertTrue(cache.getEntries(t).isEmpty());

        verify(store, times(loadInvocationCnt())).load(anyString(), anyString());
        verifyByPath();
    }
}