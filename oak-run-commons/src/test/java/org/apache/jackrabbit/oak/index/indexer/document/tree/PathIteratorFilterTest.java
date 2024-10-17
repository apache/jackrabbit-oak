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
import java.util.List;

import org.junit.Test;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;

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
        assertTrue(filter.includes("/content/abc"));
        assertTrue(filter.includes("/content/def"));
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
        assertNull(filter.nextIncludedPath("/etc"));
    }

}
