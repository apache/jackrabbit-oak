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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SubtreePatternTest {
    
    private static final String PATH = "/var/foo";
    private static final List<String> SUBTREES = Arrays.asList("/a/path", "/only/the/subtree/", ".ext", "rel/path/");
    private final SubtreePattern pattern = new SubtreePattern(PATH, SUBTREES);
    
    @Test
    public void testMatches() {
        assertFalse(pattern.matches());
    }
    
    @Test
    public void testMatchesTree() {
        Tree tree = when(mock(Tree.class).getPath()).thenReturn("/var").getMock();
        assertFalse(pattern.matches(tree, null));
        verify(tree).getPath();
        verifyNoMoreInteractions(tree);

        tree = when(mock(Tree.class).getPath()).thenReturn("/var/foo/a").getMock();
        PropertyState prop = when(mock(PropertyState.class).getName()).thenReturn("path").getMock();
        assertTrue(pattern.matches(tree, prop));
        verify(tree).getPath();
        verify(prop).getName();
        verifyNoMoreInteractions(tree, prop);
    }
    
    @Test
    public void testTargetMatch() {
        assertTrue(pattern.matches("/var/foo/a/path"));
        assertTrue(pattern.matches("/var/foo/any/a/path"));
        assertTrue(pattern.matches("/var/foo.ext"));
        assertTrue(pattern.matches("/var/foo/any.ext"));
        assertTrue(pattern.matches("/var/foo/any/something.ext"));
        assertTrue(pattern.matches("/var/foo/any.ext/something"));
        assertTrue(pattern.matches("/var/foo/.ext"));
        assertTrue(pattern.matches("/var/foo/.ext/something"));
        assertTrue(pattern.matches("/var/foo/rel/path/something"));
        assertTrue(pattern.matches("/var/foorel/path/something"));
        assertTrue(pattern.matches("/var/foo/any/rel/path/something"));
        assertTrue(pattern.matches("/var/foo/anyrel/path/something"));
        
        assertFalse(pattern.matches("/vara/path"));
        assertFalse(pattern.matches("/var/foo/only/the/subtree"));
        assertFalse(pattern.matches("/var/foorel/path"));
        assertFalse(pattern.matches("/var/foo/rel/path"));
        assertFalse(pattern.matches("/var/foo/anyrel/path"));
        assertFalse(pattern.matches("/var/foo/any/rel/path"));


        assertFalse(pattern.matches("/"));
        assertFalse(pattern.matches("/etc/a/path"));
        assertFalse(pattern.matches("/var"));
        assertFalse(pattern.matches("/content.ext", false));
    }
    
    @Test
    public void testSubtreeMatch() {
        assertTrue(pattern.matches("/var/foo/a/path/child"));
        assertTrue(pattern.matches("/var/foo/any.ext/child"));
        assertTrue(pattern.matches("/var/foo/rel/path/child"));
        assertTrue(pattern.matches("/var/foorel/path/child"));
        assertTrue(pattern.matches("/var/foo/only/the/subtree/child"));

        assertFalse(pattern.matches("/var/a/path/child"));
        assertFalse(pattern.matches("/vara/path/child"));
    }
    
    @Test
    public void testEmptyValues() {
        SubtreePattern sp = new SubtreePattern(PATH, Collections.emptyList());
        assertNoMatch(sp);
    }

    @Test
    public void testEmptyString() {
        SubtreePattern sp = new SubtreePattern(PATH, Collections.singletonList(""));
        assertNoMatch(sp);
    }

    @Test
    public void testNullString() {
        SubtreePattern sp = new SubtreePattern(PATH, Collections.singletonList(null));
        assertNoMatch(sp);
    }
    
    private static void assertNoMatch(@NotNull SubtreePattern sp) {
        assertFalse(sp.matches("/"));
        assertFalse(sp.matches("/etc"));
        assertFalse(sp.matches("/var"));
        assertFalse(sp.matches("/var/foo"));
        assertFalse(sp.matches("/var/foo/"));
        assertFalse(sp.matches("/var/foo/some/path"));
    }

    @Test
    public void testHashCode() {
        assertEquals(pattern.hashCode(), new SubtreePattern(PATH, SUBTREES).hashCode());
        assertNotEquals(pattern.hashCode(), new SubtreePattern("/var", SUBTREES).hashCode());
        assertNotEquals(pattern.hashCode(), new SubtreePattern(PATH, SUBTREES.subList(0, 2)).hashCode());
        assertNotEquals(pattern.hashCode(), new SubtreePattern("/var", Collections.emptyList()).hashCode());
    }

    @Test
    public void testEquals() {
        assertEquals(pattern, pattern);
        assertEquals(pattern, new SubtreePattern(PATH, SUBTREES));
    }

    @Test
    public void testNotEquals() {
        // different tree path
        assertNotEquals(pattern, new SubtreePattern("/another/path", SUBTREES));
        // different set of subtrees
        assertNotEquals(pattern, new SubtreePattern(PATH, Collections.emptyList()));
        assertNotEquals(pattern, new SubtreePattern(PATH, Collections.singleton("/some/other/subtree")));
        assertNotEquals(pattern, new SubtreePattern(PATH, SUBTREES.subList(0, 2)));
        assertNotEquals(pattern, new SubtreePattern(PATH, Lists.asList("/subtree/", SUBTREES.toArray(new String[0]))));
        // different restrictions
        assertNotEquals(pattern, new ItemNamePattern(ImmutableSet.of("a", "b")));
        assertNotEquals(pattern, new PrefixPattern(ImmutableSet.of("a", "b", "c")));
    }
}