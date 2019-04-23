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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class PermissionEntryCacheTest {

    private PermissionEntryCache cache = new PermissionEntryCache();

    private PermissionEntry permissionEntry;
    private PrincipalPermissionEntries ppe;
    private PermissionStore store;

    @Before
    public void before() {
        permissionEntry = new PermissionEntry("/path", true, 0, PrivilegeBits.BUILT_IN.get(PrivilegeBits.JCR_READ), RestrictionPattern.EMPTY);
        ppe = new PrincipalPermissionEntries();
        ppe.putEntriesByPath("/path", Sets.newHashSet(permissionEntry));

        store = Mockito.mock(PermissionStore.class);

    }

    private PrincipalPermissionEntries getPrincipalPermissionEntries(boolean fullyLoaded) {
        ppe.setFullyLoaded(fullyLoaded);
        return ppe;
    }

    @Test
    public void testMissingInit() throws Exception {
        Map<String, PrincipalPermissionEntries> entries = inspectEntries(cache);
        assertNotNull(entries);
        assertTrue(entries.isEmpty());
    }

    @Test
    public void testInit() throws Exception {
        cache.init("a", 5);

        PrincipalPermissionEntries entries = inspectEntries(cache, "a");
        assertNotNull(entries);
        assertFalse(entries.isFullyLoaded());
        assertEquals(0, entries.getSize());
    }

    @Test
    public void testInitTwice() throws Exception {
        cache.init("a", 5);
        cache.init("a", 25);

        PrincipalPermissionEntries entries = inspectEntries(cache, "a");
        assertNotNull(entries);

        Field f = PrincipalPermissionEntries.class.getDeclaredField("expectedSize");
        f.setAccessible(true);

        long expectedSize = (long) f.get(entries);
        assertEquals(5, expectedSize);
        assertFalse(entries.isFullyLoaded());
    }

    @Test
    public void testInitDifferentPrincipal() throws Exception {
        cache.init("a", 5);

        PrincipalPermissionEntries entries = inspectEntries(cache, "notInitialized");
        assertNull(entries);
    }

    @Test
    public void testLoadMissingInit() throws Exception {
        PrincipalPermissionEntries ppeA = getPrincipalPermissionEntries(true);

        when(store.load("a")).thenReturn(ppeA);

        Collection<PermissionEntry> result = new TreeSet();
        cache.load(store, result, "a", "/path");

        assertTrue(result.isEmpty());
    }

    @Test
    public void testLoadNotComplete() throws Exception {
        cache.init("a", Long.MAX_VALUE);

        Collection<PermissionEntry> entries = Sets.newHashSet(permissionEntry);
        when(store.load("a", "/path")).thenReturn(entries);

        Collection<PermissionEntry> result = Sets.newHashSet();
        cache.load(store, result, "a", "/path");

        assertEquals(entries, result);

        PrincipalPermissionEntries inspectedEntries = inspectEntries(cache, "a");
        assertFalse(inspectedEntries.isFullyLoaded());

        // requesting the entries again must NOT hit the store
        when(store.load("a", "/path")).thenThrow(IllegalStateException.class);

        result.clear();
        cache.load(store, result, "a", "/path");

        assertEquals(entries, result);
    }

    @Test
    public void testLoadCompleted() throws Exception {
        cache.init("a", 1);

        Collection<PermissionEntry> entries = Sets.newHashSet(permissionEntry);
        when(store.load("a", "/path")).thenReturn(entries);

        Collection<PermissionEntry> result = Sets.newHashSet();
        cache.load(store, result, "a", "/path");

        assertEquals(entries, result);

        PrincipalPermissionEntries inspectedEntries = inspectEntries(cache, "a");
        assertTrue(inspectedEntries.isFullyLoaded());

        // requesting the entries again must NOT hit the store
        when(store.load("a", "/path")).thenThrow(IllegalStateException.class);

        result.clear();
        cache.load(store, result, "a", "/path");

        assertEquals(entries, result);
    }

    @Test
    public void testLoadNonExistingNotComplete() throws Exception {
        cache.init("a", Long.MAX_VALUE);

        when(store.load("a", "/path")).thenReturn(null);

        Collection<PermissionEntry> result = Sets.newHashSet();
        cache.load(store, result, "a", "/path");

        assertTrue(result.isEmpty());

        PrincipalPermissionEntries inspectedEntries = inspectEntries(cache, "a");
        assertFalse(inspectedEntries.isFullyLoaded());

        // requesting the entries again must NOT hit the store
        when(store.load("a", "/path")).thenThrow(IllegalStateException.class);

        result.clear();
        cache.load(store, result, "a", "/path");

        assertTrue(result.isEmpty());
    }

    @Test
    public void testLoadNonExistingNotCompleted2() throws Exception {
        cache.init("a", 1);
        when(store.load("a", "/path")).thenReturn(null);

        Collection<PermissionEntry> result = Sets.newHashSet();
        cache.load(store, result, "a", "/path");

        assertTrue(result.isEmpty());

        PrincipalPermissionEntries inspectedEntries = inspectEntries(cache, "a");
        assertFalse(inspectedEntries.isFullyLoaded());

        // requesting the entries again must NOT hit the store
        when(store.load("a", "/path")).thenThrow(IllegalStateException.class);

        result.clear();
        cache.load(store, result, "a", "/path");

        assertTrue(result.isEmpty());
    }

    @Test
    public void testLoadNonExistingCompleted() throws Exception {
        cache.init("a", 0);
        when(store.load("a", "/path")).thenReturn(null);

        Collection<PermissionEntry> result = Sets.newHashSet();
        cache.load(store, result, "a", "/path");

        assertTrue(result.isEmpty());

        PrincipalPermissionEntries inspectedEntries = inspectEntries(cache, "a");
        assertTrue(inspectedEntries.isFullyLoaded());

        // requesting the entries again must NOT hit the store
        when(store.load("a", "/path")).thenThrow(IllegalStateException.class);

        result.clear();
        cache.load(store, result, "a", "/path");

        assertTrue(result.isEmpty());
    }


    @Test
    public void testGetFullyLoadedEntries() throws Exception {
        PrincipalPermissionEntries ppeA = getPrincipalPermissionEntries(true);

        when(store.load("a")).thenReturn(ppeA);

        PrincipalPermissionEntries entries = cache.getFullyLoadedEntries(store, "a");
        assertSame(ppeA, entries);

        PrincipalPermissionEntries inspectedEntries = inspectEntries(cache, "a");
        assertSame(ppeA, inspectedEntries);

        // requesting the entries again must NOT hit the store
        when(store.load("a")).thenThrow(IllegalStateException.class);
        entries = cache.getFullyLoadedEntries(store, "a");
        assertSame(ppeA, entries);
    }

    private static PrincipalPermissionEntries inspectEntries(@NotNull PermissionEntryCache cache, @NotNull String principalName) throws Exception {
        Map<String, PrincipalPermissionEntries> entries = inspectEntries(cache);
        return entries.get(principalName);
    }

    private static Map<String, PrincipalPermissionEntries> inspectEntries(@NotNull PermissionEntryCache cache) throws Exception {
        Field f = PermissionEntryCache.class.getDeclaredField("entries");
        f.setAccessible(true);

        return (Map<String, PrincipalPermissionEntries>) f.get(cache);
    }
}
