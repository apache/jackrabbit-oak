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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PermissionEntryTest {

    private String path = "/path";
    private int index = 15;
    private PermissionEntry entry = new PermissionEntry(path, true, index, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_NODES), RestrictionPattern.EMPTY);

    private RestrictionPattern pattern = mock(RestrictionPattern.class);
    private PermissionEntry entryWithNonEmptyPattern = new PermissionEntry(path, false, index, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_ADD_PROPERTIES), pattern);

    @Test
    public void testMatchesEmptyPattern() {
        assertTrue(entry.matches());
    }

    @Test
    public void testMatches() {
        when(pattern.matches()).thenReturn(true);
        assertTrue(entryWithNonEmptyPattern.matches());

        when(pattern.matches()).thenReturn(false);
        assertFalse(entryWithNonEmptyPattern.matches());
    }

    @Test
    public void testMatchesTreeEmptyPattern() {
        // restriction pattern is empty => matches
        assertTrue(entry.matches(mock(Tree.class), mock(PropertyState.class)));
        assertTrue(entry.matches(mock(Tree.class), null));
    }

    @Test
    public void testMatchesTree() {
        Tree t = mock(Tree.class);
        PropertyState ps = mock(PropertyState.class);

        when(pattern.matches(any(Tree.class), nullable(PropertyState.class))).thenReturn(true);
        assertTrue(entryWithNonEmptyPattern.matches(t, ps));
        assertTrue(entryWithNonEmptyPattern.matches(t, null));

        when(pattern.matches(any(Tree.class), nullable(PropertyState.class))).thenReturn(false);
        assertFalse(entryWithNonEmptyPattern.matches(t, ps));
        assertFalse(entryWithNonEmptyPattern.matches(t, null));

        verify(pattern, times(2)).matches(t, ps);
        verify(pattern, times(2)).matches(t, null);
    }

    @Test
    public void testMatchesPathEmptyPattern() {
        // restriction patterrn is empty => matches
        assertTrue(entry.matches(path));
        assertTrue(entry.matches(PathUtils.getParentPath(path)));
        assertTrue(entry.matches(PathUtils.concat(path, "some", "child")));
        assertTrue(entry.matches(""));
        assertTrue(entry.matches("/some/other/path"));
    }

    @Test
    public void testMatchesPath() {
        String[] paths = new String[] {path, PathUtils.getParentPath(path), PathUtils.concat(path, "some", "child"), "/some/other/path", ""};
        when(pattern.matches(anyString())).thenReturn(true);
        for (String p : paths) {
            assertTrue(entryWithNonEmptyPattern.matches(p));
        }

        when(pattern.matches(anyString())).thenReturn(false);
        for (String p : paths) {
            assertFalse(entryWithNonEmptyPattern.matches(p));
        }

        for (String p : paths) {
            verify(pattern, times(2)).matches(p);
        }
    }

    @Test
    public void testMatchesParentEmptyPattern() {
        // the entry matchesParent if the parent of the path to be evaluated is equal or a descendant of the entry-path
        assertTrue(entry.matchesParent(path));
        assertTrue(entry.matchesParent(PathUtils.concat(path, "parent", "of", "target")));
        assertFalse(entry.matchesParent(PathUtils.getParentPath(path)));
        assertFalse(entry.matchesParent("/another/path"));
    }

    @Test
    public void testMatchesParent() {
        // the entry matchesParent if the parent of the path to be evaluated is equal or a descendant of the entry-path
        // and the pattern evaluates to true (which is always the case here)
        when(pattern.matches(anyString())).thenReturn(true);
        assertTrue(entryWithNonEmptyPattern.matchesParent(path));
        assertTrue(entryWithNonEmptyPattern.matchesParent(PathUtils.concat(path, "parent", "of", "target")));
        assertFalse(entryWithNonEmptyPattern.matchesParent(PathUtils.getParentPath(path)));
        assertFalse(entryWithNonEmptyPattern.matchesParent("/another/path"));

        // pattern doesn't match => always false
        when(pattern.matches(anyString())).thenReturn(false);
        assertFalse(entryWithNonEmptyPattern.matchesParent(path));
        assertFalse(entryWithNonEmptyPattern.matchesParent(PathUtils.concat(path, "parent", "of", "target")));
        assertFalse(entryWithNonEmptyPattern.matchesParent(PathUtils.getParentPath(path)));
        assertFalse(entryWithNonEmptyPattern.matchesParent("/another/path"));
    }

    @Test
    public void testCompareToEqualPath() {
        assertEquals(0, entry.compareTo(entry));
        assertEquals(0, entry.compareTo(new PermissionEntry(path, entry.isAllow, index, entry.privilegeBits, entry.restriction)));

        PermissionEntry higherIndexEntry = new PermissionEntry(path, entry.isAllow, index + 1, entry.privilegeBits, entry.restriction);
        assertEquals(1, entry.compareTo(higherIndexEntry));
        assertEquals(-1, higherIndexEntry.compareTo(entry));
    }

    @Test
    public void testCompareToDifferentPathSameDepth() {
        String sameDepthPath = "/another";
        PermissionEntry anotherEntry = new PermissionEntry(sameDepthPath, entry.isAllow, index, entry.privilegeBits, entry.restriction);
        PermissionEntry anotherEntry2 = new PermissionEntry(sameDepthPath, !entry.isAllow, 3, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_LOCK_MANAGEMENT), mock(RestrictionPattern.class));

        assertEquals(path.compareTo(sameDepthPath), entry.compareTo(anotherEntry));
        assertEquals(path.compareTo(sameDepthPath), entry.compareTo(anotherEntry2));

        assertNotEquals(sameDepthPath.compareTo(path), entry.compareTo(anotherEntry));
        assertNotEquals(sameDepthPath.compareTo(path), entry.compareTo(anotherEntry2));
    }

    @Test
    public void testCompareToDifferentPathHigherDepth() {
        String higherDepthPath = PathUtils.concat(path, "to", "higher", "depth");
        PermissionEntry higherDepthEntry = new PermissionEntry(higherDepthPath, entry.isAllow, index, entry.privilegeBits, entry.restriction);

        assertEquals(1, entry.compareTo(higherDepthEntry));
    }

    @Test
    public void testCompareToDifferentPathLowerDepth() {
        String lowerDepthPath = PathUtils.getParentPath(path);
        PermissionEntry lowerDepthEntry = new PermissionEntry(lowerDepthPath, entry.isAllow, index, entry.privilegeBits, entry.restriction);

        assertEquals(-1, entry.compareTo(lowerDepthEntry));
    }

    @Test
    public void testEquals() {
        assertTrue(entry.equals(entry));
        assertTrue(entry.equals(new PermissionEntry(path, entry.isAllow, index, entry.privilegeBits, entry.restriction)));
        assertTrue(entry.equals(new PermissionEntry(path, entry.isAllow, index, PrivilegeBits.getInstance(entry.privilegeBits).unmodifiable(), entry.restriction)));
    }

    @Test
    public void testNotEqual() {
        // path different
        assertNotEquals(entry, new PermissionEntry("/", entry.isAllow, index, entry.privilegeBits, entry.restriction));
        assertNotEquals(entry, new PermissionEntry("/path2", entry.isAllow, index, entry.privilegeBits, entry.restriction));

        // isAllow different
        assertNotEquals(entry, new PermissionEntry(path, !entry.isAllow, index, entry.privilegeBits, entry.restriction));

        // index different
        assertNotEquals(entry, new PermissionEntry(path, entry.isAllow, 2, entry.privilegeBits, entry.restriction));

        // privbits different
        assertNotEquals(entry, new PermissionEntry(path, entry.isAllow, index, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), entry.restriction));

        // restrictions different
        assertNotEquals(entry, new PermissionEntry(path, entry.isAllow, index, entry.privilegeBits, mock(RestrictionPattern.class)));

        assertFalse(entry.equals(null));
    }
}