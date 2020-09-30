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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EntryPredicateTest {

    private String path = "/some/path";
    private String parentPath = PathUtils.getParentPath(path);

    private RestrictionPattern pattern = mock(RestrictionPattern.class);
    private PermissionEntry entry = new PermissionEntry(path, true, 1, PrivilegeBits.EMPTY, pattern);

    private Tree mockTree(@NotNull String path, @Nullable Tree parent) {
        Tree t = mock(Tree.class);
        if (parent != null) {
            when(t.getParent()).thenReturn(parent);
        }
        when(t.getPath()).thenReturn(path);
        return t;
    }

    @Test
    public void testPredicateRepositoryLevel() {
        EntryPredicate pred = EntryPredicate.create();
        assertNull(pred.getPath());

        when(pattern.matches()).thenReturn(true);

        assertFalse(pred.apply(null));

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        verify(pattern, times(3)).matches();
    }

    @Test
    public void testPredicatePathRespectParent() {
        EntryPredicate pred = EntryPredicate.create(path, true);
        assertEquals(path, pred.getPath());

        // pattern neither matches path nor parent path
        when(pattern.matches(path)).thenReturn(false);
        when(pattern.matches(parentPath)).thenReturn(false);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        // pattern matches path and parent path
        when(pattern.matches(path)).thenReturn(true);
        when(pattern.matches(parentPath)).thenReturn(true);

        assertFalse(pred.apply(null));

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        // pattern only matches path
        when(pattern.matches(path)).thenReturn(true);
        when(pattern.matches(parentPath)).thenReturn(false);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        // pattern only matches parent path
        when(pattern.matches(path)).thenReturn(false);
        when(pattern.matches(parentPath)).thenReturn(true);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        verify(pattern, times(12)).matches(path);
        verify(pattern, times(4)).matches(parentPath);
    }

    @Test
    public void testPredicatePathDontRespectParent() {
        EntryPredicate pred = EntryPredicate.create(path, false);
        assertEquals(path, pred.getPath());

        // pattern neither matches path nor parent path
        when(pattern.matches(path)).thenReturn(false);
        when(pattern.matches(parentPath)).thenReturn(false);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        // pattern matches path and parent path
        when(pattern.matches(path)).thenReturn(true);
        when(pattern.matches(parentPath)).thenReturn(true);

        assertFalse(pred.apply(null));

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        // pattern only matches path
        when(pattern.matches(path)).thenReturn(true);
        when(pattern.matches(parentPath)).thenReturn(false);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        // pattern only matches parent path
        when(pattern.matches(path)).thenReturn(false);
        when(pattern.matches(parentPath)).thenReturn(true);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        verify(pattern, times(12)).matches(path);
        verify(pattern, never()).matches(parentPath);
    }

    @Test
    public void testPredicateTreeRespectParent() {
        Tree parent = mockTree(parentPath, null);
        Tree tree = mockTree(path, parent);
        PropertyState ps = mock(PropertyState.class);
        when(ps.getName()).thenReturn("property");

        EntryPredicate pred = EntryPredicate.create(tree, ps, true);
        assertEquals(path, pred.getPath());

        // pattern neither matches path nor parent path
        when(pattern.matches(tree, ps)).thenReturn(false);
        when(pattern.matches(parent, ps)).thenReturn(false);
        when(pattern.matches(parent, null)).thenReturn(false);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        // pattern matches path and parent path
        when(pattern.matches(tree, ps)).thenReturn(true);
        when(pattern.matches(parent, ps)).thenReturn(true);
        when(pattern.matches(parent, null)).thenReturn(true);

        assertFalse(pred.apply(null));

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        // pattern only matches path
        when(pattern.matches(tree, ps)).thenReturn(true);
        when(pattern.matches(parent, ps)).thenReturn(false);
        when(pattern.matches(parent, null)).thenReturn(false);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        // pattern only matches parent path
        when(pattern.matches(tree, ps)).thenReturn(false);
        when(pattern.matches(parent, ps)).thenReturn(true);
        when(pattern.matches(parent, null)).thenReturn(true);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        verify(pattern, times(12)).matches(tree, ps);
        verify(pattern, times(4)).matches(parent, null);
        verify(pattern, never()).matches(parent, ps);
    }

    @Test
    public void testPredicateTreeDontRespectParent() {
        Tree parent = mockTree(parentPath, null);
        Tree tree = mockTree(path, parent);
        PropertyState ps = mock(PropertyState.class);
        when(ps.getName()).thenReturn("property");

        EntryPredicate pred = EntryPredicate.create(tree, ps,false);
        assertEquals(path, pred.getPath());

        // pattern neither matches path nor parent path
        when(pattern.matches(tree, ps)).thenReturn(false);
        when(pattern.matches(parent, ps)).thenReturn(false);
        when(pattern.matches(parent, null)).thenReturn(false);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        // pattern matches path and parent path
        when(pattern.matches(tree, ps)).thenReturn(true);
        when(pattern.matches(parent, ps)).thenReturn(true);
        when(pattern.matches(parent, null)).thenReturn(true);

        assertFalse(pred.apply(null));

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        // pattern only matches path
        when(pattern.matches(tree, ps)).thenReturn(true);
        when(pattern.matches(parent, ps)).thenReturn(false);
        when(pattern.matches(parent, null)).thenReturn(false);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        // pattern only matches parent path
        when(pattern.matches(tree, ps)).thenReturn(false);
        when(pattern.matches(parent, ps)).thenReturn(true);
        when(pattern.matches(parent, null)).thenReturn(true);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        verify(pattern, times(12)).matches(tree, ps);
        verify(pattern, never()).matches(parent, ps);
        verify(pattern, never()).matches(parent, null);
    }

    @Test
    public void testPredicateRootPath() {
        EntryPredicate pred = EntryPredicate.create(PathUtils.ROOT_PATH, true);
        assertEquals(PathUtils.ROOT_PATH, pred.getPath());

        // pattern doesn't match path
        when(pattern.matches(PathUtils.ROOT_PATH)).thenReturn(false);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        // pattern matches path
        when(pattern.matches(PathUtils.ROOT_PATH)).thenReturn(true);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        verify(pattern, times(6)).matches(PathUtils.ROOT_PATH);
    }

    @Test
    public void testPredicateRootPathDontRespectParent() {
        EntryPredicate pred = EntryPredicate.create(PathUtils.ROOT_PATH, false);
        assertEquals(PathUtils.ROOT_PATH, pred.getPath());

        // pattern doesn't match path
        when(pattern.matches(PathUtils.ROOT_PATH)).thenReturn(false);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        // pattern matches path
        when(pattern.matches(PathUtils.ROOT_PATH)).thenReturn(true);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        verify(pattern, times(6)).matches(PathUtils.ROOT_PATH);
    }

    @Test
    public void testPredicateRootTree() {
        Tree tree = mockTree(PathUtils.ROOT_PATH, null);
        when(tree.isRoot()).thenReturn(true);

        EntryPredicate pred = EntryPredicate.create(tree, null,true);
        assertEquals(PathUtils.ROOT_PATH, pred.getPath());

        // pattern doesn't match path
        when(pattern.matches(tree, null)).thenReturn(false);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        // pattern matches path
        when(pattern.matches(tree, null)).thenReturn(true);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        verify(tree, never()).getParent();
        verify(pattern, times(6)).matches(tree, null);
    }

    @Test
    public void testPredicateRootTreeDontRespectParent() {
        Tree tree = mockTree(PathUtils.ROOT_PATH, null);
        when(tree.isRoot()).thenReturn(true);

        EntryPredicate pred = EntryPredicate.create(tree, null,false);
        assertEquals(PathUtils.ROOT_PATH, pred.getPath());

        // pattern doesn't match path
        when(pattern.matches(tree, null)).thenReturn(false);

        assertFalse(pred.apply(entry));
        assertFalse(pred.apply(entry, true));
        assertFalse(pred.apply(entry, false));

        // pattern matches path
        when(pattern.matches(tree, null)).thenReturn(true);

        assertTrue(pred.apply(entry));
        assertTrue(pred.apply(entry, true));
        assertTrue(pred.apply(entry, false));

        verify(tree, never()).getParent();
        verify(pattern, times(6)).matches(tree, null);
    }
}