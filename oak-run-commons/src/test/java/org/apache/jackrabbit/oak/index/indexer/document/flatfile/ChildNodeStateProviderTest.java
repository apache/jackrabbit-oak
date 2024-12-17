/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.guava.common.collect.ImmutableList.copyOf;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.TestUtils.createList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ChildNodeStateProviderTest {

    @Test
    public void emptyCase() {
        Set<String> preferred = Set.of("u", "v", "x", "y", "z");
        ChildNodeStateProvider p = new ChildNodeStateProvider(emptyList(), "/a", preferred);
        assertEquals(0, p.getChildNodeCount(1));
        assertEquals(0, Iterables.size(p.getChildNodeNames()));
        assertEquals(0, Iterables.size(p.getChildNodeEntries()));
        assertFalse(p.hasChildNode("foo"));
        assertFalse(p.getChildNode("foo").exists());
    }

    @Test
    public void children() {
        Set<String> preferred = Set.of("jcr:content", "x");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f", "/g", "/h"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred);

        assertEquals(asList("jcr:content", "c", "d"), copyOf(childNames(p.children())));
        assertEquals(5, citr.getCount());

        citr.reset();
        p = new ChildNodeStateProvider(citr, "/e", preferred);
        assertEquals(singletonList("f"), copyOf(childNames(p.children())));
        assertEquals(7, citr.getCount());


        p = new ChildNodeStateProvider(citr, "/g", preferred);
        assertEquals(emptyList(), copyOf(childNames(p.children())));
    }

    @Test
    public void children2() {
        Set<String> preferred = Set.of("b");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/b", "/a/b/c", "/a/b/c/d", "/e", "/e/f", "/g", "/h"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred);

        assertEquals(singletonList("b"), copyOf(childNames(p.children())));
        assertEquals(5, citr.getCount());

        citr.reset();
        p = new ChildNodeStateProvider(citr, "/a/b", preferred);
        assertEquals(singletonList("c"), copyOf(childNames(p.children())));
        assertEquals(5, citr.getCount());

        p = new ChildNodeStateProvider(citr, "/a/b/c", preferred);
        assertEquals(singletonList("d"), copyOf(childNames(p.children())));

        p = new ChildNodeStateProvider(citr, "/a/b/c/d", preferred);
        assertEquals(emptyList(), copyOf(childNames(p.children())));

        p = new ChildNodeStateProvider(citr, "/h", preferred);
        assertEquals(emptyList(), copyOf(childNames(p.children())));
    }

    @Test
    public void hasChildNode_InLimit() {
        Set<String> preferred = Set.of("jcr:content", "x");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred);

        citr.reset();
        assertTrue(p.hasChildNode("jcr:content"));
        assertEquals("Unexpected number of reads to get jcr:content", 2, citr.getCount());

        citr.reset();
        assertFalse(p.hasChildNode("x"));
        assertEquals("Unexpected number reads to conclude that preferred child 'x' is missing",
                3, citr.getCount());

        citr.reset();
        assertTrue(p.hasChildNode("c"));
        assertEquals("Unexpected number reads to get 'c'", 3, citr.getCount());

        citr.reset();
        assertTrue(p.hasChildNode("d"));
        assertEquals("Unexpected number reads to get 'd'", 4, citr.getCount());

        citr.reset();
        assertFalse(p.hasChildNode("y"));
        assertEquals("Unexpected number reads to conclude that non-preferred child 'x' is missing",
                5, citr.getCount());
    }

    @Test
    public void allPreferredReadable() {
        Set<String> preferred = Set.of("x", "y");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/x", "/a/x/1", "/a/x/2", "/a/x/3",
                "/a/y"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred);

        assertTrue(p.hasChildNode("x"));
        assertTrue(p.hasChildNode("y"));
    }

    @Test
    public void childCount() {
        Set<String> preferred = Set.of("jcr:content", "x");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred);
        assertEquals(1, p.getChildNodeCount(1));
        assertEquals(3, p.getChildNodeCount(2));
    }

    @Test
    public void childNames() {
        Set<String> preferred = Set.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred);

        assertEquals(asList("jcr:content", "c", "d"), copyOf(childNames(p.children())));
        assertEquals(5, citr.getCount());
    }

    @Test
    public void childNames2() {
        Set<String> preferred = Set.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/jcr:content/metadata",
                "/a/c", "/a/c/status","/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred);

        assertEquals(asList("jcr:content", "c", "d"), copyOf(childNames(p.children())));
        assertEquals(7, citr.getCount());
    }

    @Test
    public void childEntries() {
        Set<String> preferred = Set.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred);

        Map<String, NodeState> children = new HashMap<>();
        p.getChildNodeEntries().forEach(e -> children.put(e.getName(), e.getNodeState()));
        assertThat(children.keySet(), containsInAnyOrder("jcr:content", "c", "d"));

        assertEquals("/a/jcr:content", children.get("jcr:content").getString("path"));
        assertEquals("/a/d", children.get("d").getString("path"));
        assertEquals("/a/c", children.get("c").getString("path"));
    }

    private Iterator<String> childNames(Iterator<NodeStateEntry> children) {
        return Iterators.transform(children, c -> PathUtils.getName(c.getPath()));
    }

}