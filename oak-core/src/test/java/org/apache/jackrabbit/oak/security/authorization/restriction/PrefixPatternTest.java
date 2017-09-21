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
import javax.jcr.NamespaceRegistry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class PrefixPatternTest extends AbstractSecurityTest {

    private final Set<String> prefixes = ImmutableSet.of(NamespaceRegistry.PREFIX_JCR);

    private final PrefixPattern pattern = new PrefixPattern(prefixes);

    @Test
    public void testMatchesItem() throws Exception {

        NodeUtil rootTree = new NodeUtil(root.getTree("/"));
        for (String prefix : prefixes) {
            Tree testTree = rootTree.addChild(prefix + ":name", NodeTypeConstants.NT_OAK_UNSTRUCTURED).getTree();

            assertTrue(pattern.matches(testTree, null));
            assertTrue(pattern.matches(testTree, PropertyStates.createProperty(prefix + ":f", "anyval")));

            assertFalse(pattern.matches(testTree, PropertyStates.createProperty("a", Boolean.FALSE)));

            testTree.remove();
        }

        List<String> notMatching = ImmutableList.of(NamespaceRegistry.PREFIX_EMPTY, NamespaceRegistry.PREFIX_MIX, "any");
        for (String prefix : notMatching) {
            String name = (prefix.isEmpty()) ? "name" : prefix + ":name";
            Tree testTree = rootTree.addChild(name, NodeTypeConstants.NT_OAK_UNSTRUCTURED).getTree();

            assertFalse(pattern.matches(testTree, null));
            assertFalse(pattern.matches(testTree, PropertyStates.createProperty("f", "anyval")));

            assertTrue(pattern.matches(testTree, PropertyStates.createProperty("jcr:a", Boolean.FALSE)));

            testTree.remove();
        }
    }

    @Test
    public void testMatchesPath() {
        List<String> notMatching = ImmutableList.of("/", "/a", "/jcr:b", "/d/jcr:e/a", "/a/b/c/d/jcr:b");
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
        assertEquals(prefixes.toString(), pattern.toString());
    }

    @Test
    public void testHashCode() {
        assertEquals(prefixes.hashCode(), pattern.hashCode());
    }

    @Test
    public void testEquals() {
        assertEquals(pattern, pattern);
        assertEquals(pattern, new PrefixPattern(prefixes));
    }

    @Test
    public void testNotEquals() {
        assertNotEquals(pattern, new PrefixPattern(ImmutableSet.of(NamespaceRegistry.PREFIX_EMPTY)));
        assertNotEquals(pattern, new PrefixPattern(ImmutableSet.of(NamespaceRegistry.PREFIX_EMPTY, NamespaceRegistry.PREFIX_JCR)));
        assertNotEquals(pattern, new PrefixPattern(ImmutableSet.of("oak")));
        assertNotEquals(pattern, new ItemNamePattern(prefixes));
    }

}