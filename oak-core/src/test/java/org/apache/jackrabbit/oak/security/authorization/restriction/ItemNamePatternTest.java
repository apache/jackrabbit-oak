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

import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.AccessDeniedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ItemNamePatternTest extends AbstractSecurityTest {

    private final Set<String> names = Set.of("a", "b", "c");
    private final ItemNamePattern pattern = new ItemNamePattern(names);

    private static Tree addTree(@NotNull Tree parent, @NotNull String relPath) throws AccessDeniedException {
        Tree t = parent;
        for (String elem : PathUtils.elements(relPath)) {
            t = TreeUtil.addChild(t, elem, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        }
        return t;
    }

    @Test
    public void testMatchesItem() throws Exception {

        Tree rootTree = root.getTree("/");
        List<String> matching = ImmutableList.of("a", "b", "c", "d/e/a", "a/b/c/d/b", "test/c");
        for (String relPath : matching) {
            Tree testTree = addTree(rootTree, relPath);

            assertTrue(pattern.matches(testTree, null));
            assertTrue(pattern.matches(testTree, PropertyStates.createProperty("a", Boolean.FALSE)));
            assertFalse(pattern.matches(testTree, PropertyStates.createProperty("f", "anyval")));

            testTree.remove();
        }

        List<String> notMatching = ImmutableList.of("d", "b/d", "d/e/f", "c/b/abc");
        for (String relPath : notMatching) {
            Tree testTree = addTree(rootTree, relPath);

            assertFalse(pattern.matches(testTree, null));
            assertTrue(pattern.matches(testTree, PropertyStates.createProperty("a", Boolean.FALSE)));
            assertFalse(pattern.matches(testTree, PropertyStates.createProperty("f", "anyval")));

            testTree.remove();
        }
    }

    @Test
    public void testMatchesPath() {
        List<String> matching = ImmutableList.of("/a", "/b", "/c", "/d/e/a", "/a/b/c/d/b", "/test/c");
        for (String p : matching) {
            assertTrue(pattern.matches(p));
        }

        List<String> notMatching = ImmutableList.of("/", "/d", "/b/d", "/d/e/f", "/c/b/abc");
        for (String p : notMatching) {
            assertFalse(pattern.matches(p));
        }
    }

    @Test
    public void testMatchesNull() {
        assertFalse(pattern.matches());
    }

    @Test
    public void testToString() {
        assertEquals(names.toString(), pattern.toString());
    }

    @Test
    public void testHashCode() {
        assertEquals(names.hashCode(), pattern.hashCode());
    }

    @Test
    public void testEquals() {
        assertEquals(pattern, pattern);
        assertEquals(pattern, new ItemNamePattern(names));
    }

    @Test
    public void testNotEquals() {
        assertNotEquals(pattern, new ItemNamePattern(Set.of("a", "b")));
        assertNotEquals(pattern, new PrefixPattern(Set.of("a", "b", "c")));
    }
}