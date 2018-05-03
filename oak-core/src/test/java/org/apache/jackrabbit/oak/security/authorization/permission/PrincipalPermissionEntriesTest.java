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
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PrincipalPermissionEntriesTest {

    private final PermissionEntry permissionEntry = new PermissionEntry("/path", true, 0, PrivilegeBits.BUILT_IN.get(PrivilegeBits.JCR_READ), RestrictionPattern.EMPTY);

    @Test
    public void testExpectedSize() throws Exception {
        assertEquals(Long.MAX_VALUE, inspectExpectedSize(new PrincipalPermissionEntries()));
        assertEquals(1, inspectExpectedSize(new PrincipalPermissionEntries(1)));
    }

    @Test
    public void testGetEntriesUponCreation() {
        assertTrue(new PrincipalPermissionEntries(1).getEntries().isEmpty());
        assertTrue(new PrincipalPermissionEntries().getEntries().isEmpty());
    }

    @Test
    public void testGetEntriesByPathUponCreation() {
        assertNull(new PrincipalPermissionEntries(1).getEntriesByPath("/path"));
        assertNull(new PrincipalPermissionEntries().getEntriesByPath("/path"));
    }

    @Test
    public void testIsFullyLoadedUponCreation() {
        assertFalse(new PrincipalPermissionEntries(0).isFullyLoaded());
        assertFalse(new PrincipalPermissionEntries(1).isFullyLoaded());
        assertFalse(new PrincipalPermissionEntries().isFullyLoaded());
    }

    @Test
    public void testSetFullyLoaded() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(1);
        ppe.setFullyLoaded(true);

        assertTrue(ppe.isFullyLoaded());
    }

    @Test
    public void testSetFullyLoadedNoExpectedSize() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries();
        ppe.setFullyLoaded(true);

        assertTrue(ppe.isFullyLoaded());
    }

    @Test
    public void testPutAllEntriesSetsFullyLoadedIgnoresExpectedSize() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(1);
        ppe.putAllEntries(ImmutableMap.of());

        assertTrue(ppe.isFullyLoaded());
    }

    @Test
    public void testPutAllEntriesSetsFullyLoaded() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(1);
        ppe.putAllEntries(ImmutableMap.of("/path", ImmutableSet.of(permissionEntry)));
        assertTrue(ppe.isFullyLoaded());
    }

    @Test
    public void testPutAllEntriesWithoutExpectedSizeSetsFullyLoaded() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries();
        ppe.putAllEntries(ImmutableMap.of("/path", ImmutableSet.of(permissionEntry)));
        assertTrue(ppe.isFullyLoaded());
    }

    @Test
    public void testPutAllEntries() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries();

        Map<String, Collection<PermissionEntry>> allEntries = ImmutableMap.of("/path", ImmutableSet.of(permissionEntry));
        ppe.putAllEntries(allEntries);

        assertEquals(allEntries, ppe.getEntries());
    }

    @Test
    public void testPutEntriesByPathSetsFullyLoaded() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(1);
        ppe.putEntriesByPath("/path", ImmutableSet.of(permissionEntry));

        assertTrue(ppe.isFullyLoaded());
    }

    @Test
    public void testPutEntriesByPathExceedingExpectedSizeSetsFullyLoaded() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(1);
        Collection<PermissionEntry> collection = ImmutableSet.of(permissionEntry);
        ppe.putEntriesByPath("/path", collection);
        ppe.putEntriesByPath("/path2", collection);

        assertTrue(ppe.isFullyLoaded());
    }

    @Test
    public void testPutEntriesByPathNotReachingExpectedSize() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(2);
        ppe.putEntriesByPath("/path", ImmutableSet.of(permissionEntry));

        assertFalse(ppe.isFullyLoaded());
    }

    @Test
    public void testPutEntriesByPath() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(2);
        ppe.putEntriesByPath("/path", ImmutableSet.of(permissionEntry));

        assertEquals(1, ppe.getEntries().size());
    }

    @Test
    public void testGetEntriesByPath() {
        PrincipalPermissionEntries ppe = new PrincipalPermissionEntries(2);
        Collection<PermissionEntry> collection = ImmutableSet.of(permissionEntry);
        Map<String, Collection<PermissionEntry>> allEntries =
                        ImmutableMap.of("/path", collection, "/path2", collection);

        ppe.putAllEntries(allEntries);

        assertEquals(collection, ppe.getEntriesByPath("/path"));
        assertEquals(collection, ppe.getEntriesByPath("/path2"));
        assertNull(ppe.getEntriesByPath("/nonExisting"));
    }

    private static final long inspectExpectedSize(@Nonnull PrincipalPermissionEntries ppe) throws Exception {
        Field f = PrincipalPermissionEntries.class.getDeclaredField("expectedSize");
        f.setAccessible(true);

        return (long) f.get(ppe);
    }
}
