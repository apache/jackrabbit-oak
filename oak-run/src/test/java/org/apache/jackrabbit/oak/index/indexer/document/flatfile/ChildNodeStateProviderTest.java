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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.copyOf;
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
        ChildNodeStateProvider p = new ChildNodeStateProvider(emptyList(), "/a", 5);
        assertEquals(0, p.getChildNodeCount(1));
        assertEquals(0, Iterables.size(p.getChildNodeNames()));
        assertEquals(0, Iterables.size(p.getChildNodeEntries()));
        assertFalse(p.hasChildNode("foo"));
        assertFalse(p.getChildNode("foo").exists());
    }

    @Test
    public void children() {
        Set<String> preferred = ImmutableSet.of("jcr:content", "x");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f", "/g", "/h"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", 100);

        assertEquals(asList("jcr:content", "c", "d"), copyOf(childNames(p.children())));
        assertEquals(5, citr.getCount());

        citr.reset();
        p = new ChildNodeStateProvider(citr, "/e", 100);
        assertEquals(singletonList("f"), copyOf(childNames(p.children())));
        assertEquals(7, citr.getCount());


        p = new ChildNodeStateProvider(citr, "/g", 100);
        assertEquals(emptyList(), copyOf(childNames(p.children())));
    }

    @Test
    public void children2() {
        Set<String> preferred = ImmutableSet.of("b");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/b", "/a/b/c", "/a/b/c/d", "/e", "/e/f", "/g", "/h"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", 100);

        assertEquals(singletonList("b"), copyOf(childNames(p.children())));
        assertEquals(5, citr.getCount());

        citr.reset();
        p = new ChildNodeStateProvider(citr, "/a/b", 100);
        assertEquals(singletonList("c"), copyOf(childNames(p.children())));
        assertEquals(5, citr.getCount());

        p = new ChildNodeStateProvider(citr, "/a/b/c", 100);
        assertEquals(singletonList("d"), copyOf(childNames(p.children())));

        p = new ChildNodeStateProvider(citr, "/a/b/c/d", 100);
        assertEquals(emptyList(), copyOf(childNames(p.children())));

        p = new ChildNodeStateProvider(citr, "/h", 100);
        assertEquals(emptyList(), copyOf(childNames(p.children())));
    }

    @Test
    public void hasChildNode_InLimit() {
        Set<String> preferred = ImmutableSet.of("jcr:content", "x");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred.size());

        assertTrue(p.hasChildNode("jcr:content"));
        assertTrue(p.hasChildNode("c"));
        assertFalse(p.hasChildNode("d"));
    }

    @Test
    public void childCount() {
        Set<String> preferred = ImmutableSet.of("jcr:content", "x");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", preferred.size());
        assertEquals(1, p.getChildNodeCount(1));
        assertEquals(3, p.getChildNodeCount(2));
    }

    @Test
    public void childNames() {
        Set<String> preferred = ImmutableSet.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", 100);

        assertEquals(asList("jcr:content", "c", "d"), copyOf(childNames(p.children())));
        assertEquals(5, citr.getCount());
    }

    @Test
    public void childNames2() {
        Set<String> preferred = ImmutableSet.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/jcr:content/metadata",
                "/a/c", "/a/c/status","/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", 100);

        assertEquals(asList("jcr:content", "c", "d"), copyOf(childNames(p.children())));
        assertEquals(7, citr.getCount());
    }

    @Test
    public void childEntries() {
        Set<String> preferred = ImmutableSet.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/c", "/a/d", "/e", "/e/f"));
        ChildNodeStateProvider p = new ChildNodeStateProvider(citr, "/a", 100);

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