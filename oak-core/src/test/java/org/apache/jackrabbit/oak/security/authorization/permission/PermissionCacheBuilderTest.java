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

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class PermissionCacheBuilderTest {

    private static final String EMPTY_CLASS_NAME = "org.apache.jackrabbit.oak.security.authorization.permission.PermissionCacheBuilder$EmptyCache";
    private static final String SIMPLE_CLASS_NAME = "org.apache.jackrabbit.oak.security.authorization.permission.PermissionCacheBuilder$PathEntryMapCache";
    private static final String DEFAULT_CLASS_NAME = "org.apache.jackrabbit.oak.security.authorization.permission.PermissionCacheBuilder$DefaultPermissionCache";

    private PermissionStore store;
    private PermissionCacheBuilder permissionCacheBuilder;

    @Before
    public void before() {
        store = Mockito.mock(PermissionStore.class);
        permissionCacheBuilder = new PermissionCacheBuilder(store);
    }

    @Test(expected = IllegalStateException.class)
    public void testBuildBeforeInitialized() {
        permissionCacheBuilder.build();
    }

    @Test
    public void testBuildForEmptyPrincipals() {
        assertTrue(permissionCacheBuilder.init(ImmutableSet.of(), Long.MAX_VALUE));
        permissionCacheBuilder.init(ImmutableSet.of(), Long.MAX_VALUE);
        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(EMPTY_CLASS_NAME, cache.getClass().getName());
    }

    @Test
    public void testBuildNoExistingEntries() throws Exception {
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.ZERO);
        when(store.load(anyString())).thenReturn(new PrincipalPermissionEntries(0));

        Set<String> principalNames = Sets.newHashSet("noEntries", "noEntries2", "noEntries3");

        assertTrue(permissionCacheBuilder.init(principalNames, Long.MAX_VALUE));
        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(EMPTY_CLASS_NAME, cache.getClass().getName());
    }

    @Test
    public void testBuildFewEntriesSamePath() throws Exception {
        PrincipalPermissionEntries ppeA = new PrincipalPermissionEntries(1);
        ppeA.putEntriesByPath("/path", ImmutableSet.of(new PermissionEntry("/path", false, 0, PrivilegeBits.BUILT_IN.get(PrivilegeBits.REP_READ_NODES), RestrictionPattern.EMPTY)));

        PrincipalPermissionEntries ppeB = new PrincipalPermissionEntries(1);
        ppeB.putEntriesByPath("/path", ImmutableSet.of(new PermissionEntry("/path", false, 1, PrivilegeBits.BUILT_IN.get(PrivilegeBits.REP_READ_NODES), RestrictionPattern.EMPTY)));

        when(store.load("a")).thenReturn(ppeA);
        when(store.load("b")).thenReturn(ppeB);
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, true));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        assertFalse(permissionCacheBuilder.init(principalNames, Long.MAX_VALUE));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(SIMPLE_CLASS_NAME, cache.getClass().getName());
    }

    @Test
    public void testBuildFewEntriesDifferentPaths() throws Exception {
        PrincipalPermissionEntries ppeA = new PrincipalPermissionEntries(1);
        ppeA.putEntriesByPath("/path", ImmutableSet.of(new PermissionEntry("/path", false, 0, PrivilegeBits.BUILT_IN.get(PrivilegeBits.REP_READ_NODES), RestrictionPattern.EMPTY)));

        PrincipalPermissionEntries ppeB = new PrincipalPermissionEntries(1);
        ppeB.putEntriesByPath("/path", ImmutableSet.of(new PermissionEntry("/path", false, 1, PrivilegeBits.BUILT_IN.get(PrivilegeBits.REP_READ_NODES), RestrictionPattern.EMPTY)));

        when(store.load("a")).thenReturn(ppeA);
        when(store.load("b")).thenReturn(ppeB);
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, true));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        assertFalse(permissionCacheBuilder.init(principalNames, Long.MAX_VALUE));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(SIMPLE_CLASS_NAME, cache.getClass().getName());
    }

    @Test
    public void testNoEntriesNonExactCnt() throws Exception {
        when(store.load("a")).thenReturn(new PrincipalPermissionEntries());
        when(store.load("b")).thenReturn(new PrincipalPermissionEntries());
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, false));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        assertFalse(permissionCacheBuilder.init(principalNames, Long.MAX_VALUE));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(EMPTY_CLASS_NAME, cache.getClass().getName());
    }

    @Test
    public void testBuildMaxEntriesReached() throws Exception {
        PrincipalPermissionEntries ppeA = new PrincipalPermissionEntries(1);
        ppeA.putEntriesByPath("/path1", ImmutableSet.of(new PermissionEntry("/path1", false, 0, PrivilegeBits.BUILT_IN.get(PrivilegeBits.REP_READ_NODES), RestrictionPattern.EMPTY)));

        PrincipalPermissionEntries ppeB = new PrincipalPermissionEntries(1);
        ppeA.putEntriesByPath("/path2", ImmutableSet.of(new PermissionEntry("/path2", false, 0, PrivilegeBits.BUILT_IN.get(PrivilegeBits.REP_READ_NODES), RestrictionPattern.EMPTY)));

        when(store.load("a")).thenReturn(ppeA);
        when(store.load("b")).thenReturn(ppeB);
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, true));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        long maxSize = 1;
        assertFalse(permissionCacheBuilder.init(principalNames, maxSize));

        PermissionCache cache = permissionCacheBuilder.build();
        assertEquals(DEFAULT_CLASS_NAME, cache.getClass().getName());
    }
}