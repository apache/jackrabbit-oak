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
package org.apache.jackrabbit.oak.index.indexer.document.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.OakInitializer;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class PathIteratorFilterTest {

    @Test
    public void convert() {
        List<PathFilter> list = new ArrayList<>();
        // no includes
        assertEquals("[]", PathIteratorFilter.getAllIncludedPaths(list).toString());

        // root
        list.add(new PathFilter(List.of("/"), Collections.emptyList()));
        assertEquals("[/]", PathIteratorFilter.getAllIncludedPaths(list).toString());

        // root is higher than /content, so we only need to retain root
        list.add(new PathFilter(List.of("/content"), Collections.emptyList()));
        assertEquals("[/]", PathIteratorFilter.getAllIncludedPaths(list).toString());

        list.clear();
        list.add(new PathFilter(List.of("/content"), Collections.emptyList()));
        assertEquals("[/content]", PathIteratorFilter.getAllIncludedPaths(list).toString());

        // /content is higher than /content/abc, so we only keep /content
        list.add(new PathFilter(List.of("/content/abc"), Collections.emptyList()));
        assertEquals("[/content]", PathIteratorFilter.getAllIncludedPaths(list).toString());

        // /libs is new
        list.add(new PathFilter(List.of("/lib"), Collections.emptyList()));
        assertEquals("[/content, /lib]", PathIteratorFilter.getAllIncludedPaths(list).toString());

        // root overrides everything
        list.add(new PathFilter(List.of("/"), Collections.emptyList()));
        assertEquals("[/]", PathIteratorFilter.getAllIncludedPaths(list).toString());
    }

    @Test
    public void emptySet() {
        List<PathFilter> list = new ArrayList<>();
        PathIteratorFilter filter = new PathIteratorFilter(PathIteratorFilter.getAllIncludedPaths(list));
        assertFalse(filter.includes("/"));
        assertNull(filter.nextIncludedPath("/"));
    }

    @Test
    public void all() {
        List<PathFilter> list = new ArrayList<>();
        list.add(new PathFilter(List.of("/"), Collections.emptyList()));
        PathIteratorFilter filter = new PathIteratorFilter(PathIteratorFilter.getAllIncludedPaths(list));
        assertTrue(filter.includes("/"));
        assertTrue(filter.includes("/content"));
        assertTrue(filter.includes("/var"));
        assertNull(filter.nextIncludedPath("/"));
    }

    @Test
    public void content() {
        List<PathFilter> list = new ArrayList<>();
        list.add(new PathFilter(List.of("/content"), Collections.emptyList()));
        PathIteratorFilter filter = new PathIteratorFilter(PathIteratorFilter.getAllIncludedPaths(list));
        assertFalse(filter.includes("/"));
        assertEquals("/content", filter.nextIncludedPath("/"));
        assertTrue(filter.includes("/content"));
        assertFalse(filter.includes("/content-"));
        assertEquals("/content/", filter.nextIncludedPath("/content-"));
        assertTrue(filter.includes("/content/abc"));
        assertTrue(filter.includes("/content/def"));
        assertFalse(filter.includes("/contentNext"));
        assertFalse(filter.includes("/var"));
        assertNull(filter.nextIncludedPath("/var"));
    }

    @Test
    public void contentAndEtc() {
        List<PathFilter> list = new ArrayList<>();
        list.add(new PathFilter(List.of("/content"), Collections.emptyList()));
        list.add(new PathFilter(List.of("/etc"), Collections.emptyList()));
        PathIteratorFilter filter = new PathIteratorFilter(PathIteratorFilter.getAllIncludedPaths(list));
        assertFalse(filter.includes("/"));
        assertEquals("/content", filter.nextIncludedPath("/"));
        assertTrue(filter.includes("/content"));
        assertTrue(filter.includes("/content/abc"));
        assertTrue(filter.includes("/content/def"));
        assertFalse(filter.includes("/content1"));
        assertEquals("/etc", filter.nextIncludedPath("/content1"));
        assertTrue(filter.includes("/etc"));
        assertTrue(filter.includes("/etc/test"));
        assertFalse(filter.includes("/tmp"));
        assertNull(filter.nextIncludedPath("/tmp"));

        assertEquals("/content", filter.nextIncludedPath("/"));
        assertEquals("/etc", filter.nextIncludedPath("/content1"));
        assertEquals("/etc/", filter.nextIncludedPath("/etc"));
        assertNull(filter.nextIncludedPath("/etc/"));
    }

    @Test
    public void whiteboxTest() {
        List<PathFilter> list = new ArrayList<>();
        list.add(new PathFilter(List.of("/content"), Collections.emptyList()));
        list.add(new PathFilter(List.of("/etc"), Collections.emptyList()));
        PathIteratorFilter filter = new PathIteratorFilter(PathIteratorFilter.getAllIncludedPaths(list));
        // we verify that the internal set contains the children as well
        assertEquals("[/content, /content/, /etc, /etc/]", filter.toString());

        PathIteratorFilter f = new PathIteratorFilter(
                new TreeSet<>(List.of("/", "/etc")));
        assertEquals("[/etc, /etc/]", f.toString());

        PathIteratorFilter all = new PathIteratorFilter();
        assertEquals("[/]", all.toString());
    }

    @Test
    public void extractPathFiltersTest() {
        NodeStore store = new MemoryNodeStore();
        EditorHook hook = new EditorHook(
                new CompositeEditorProvider(new NamespaceEditorProvider(), new TypeEditorProvider()));
        OakInitializer.initialize(store, new InitialContent(), hook);
        Set<IndexDefinition> defs = new HashSet<>();
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("test-1");
        builder.includedPaths("/content", "/etc");
        IndexDefinition test1 = IndexDefinition.newBuilder(store.getRoot(), builder.build(), "/").build();
        defs.add(test1);
        builder = new IndexDefinitionBuilder();
        builder.indexRule("test-2");
        builder.includedPaths("/content/abc", "/var");
        IndexDefinition test2 = IndexDefinition.newBuilder(store.getRoot(), builder.build(), "/").build();
        defs.add(test2);
        List<PathFilter> list = PathIteratorFilter.extractPathFilters(defs);
        assertEquals("[/content, /etc, /var]", PathIteratorFilter.getAllIncludedPaths(list).toString());
    }

}
