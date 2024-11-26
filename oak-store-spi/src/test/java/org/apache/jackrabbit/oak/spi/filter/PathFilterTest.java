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

package org.apache.jackrabbit.oak.spi.filter;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_INCLUDED_PATHS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PathFilterTest {

    @Test
    public void exclude() {
        PathFilter p = new PathFilter(Set.of("/"), Set.of("/etc"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/a"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc/workflow"));
    }

    @Test
    public void include() {
        PathFilter p = new PathFilter(Set.of("/content", "/etc"), Set.of("/etc/workflow/instance"));
        assertEquals(PathFilter.Result.TRAVERSE, p.filter("/"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/var"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/content"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/content/example"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/etc"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/etc/workflow"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc/workflow/instance"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc/workflow/instance/1"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/x"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/e"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etcx"));
    }

    @Test
    public void emptyConfig() {
        NodeBuilder root = EMPTY_NODE.builder();
        PathFilter p = PathFilter.from(root);
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/a"));
    }

    @Test
    public void config() {
        NodeBuilder root = EMPTY_NODE.builder();
        root.setProperty(createProperty(PROP_INCLUDED_PATHS, Set.of("/etc"), Type.STRINGS));
        root.setProperty(createProperty(PROP_EXCLUDED_PATHS, Set.of("/etc/workflow"), Type.STRINGS));
        PathFilter p = PathFilter.from(root);
        assertEquals(PathFilter.Result.TRAVERSE, p.filter("/"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/etc"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/etc/a"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc/workflow"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc/workflow/1"));
    }

    @Test
    public void configWithStringProperties() {
        NodeBuilder root = EMPTY_NODE.builder();
        root.setProperty(createProperty(PROP_INCLUDED_PATHS, "/etc", Type.STRING));
        root.setProperty(createProperty(PROP_EXCLUDED_PATHS, "/etc/workflow", Type.STRING));
        PathFilter p = PathFilter.from(root);
        assertEquals(PathFilter.Result.TRAVERSE, p.filter("/"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/etc"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/etc/a"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc/workflow"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc/workflow/1"));
    }

    @Test
    public void configOnlyExclude() {
        NodeBuilder root = EMPTY_NODE.builder();
        root.setProperty(createProperty(PROP_EXCLUDED_PATHS, Set.of("/etc/workflow"), Type.STRINGS));
        PathFilter p = PathFilter.from(root);
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/etc"));
        assertEquals(PathFilter.Result.INCLUDE, p.filter("/etc/a"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc/workflow"));
        assertEquals(PathFilter.Result.EXCLUDE, p.filter("/etc/workflow/1"));
    }

    @Test
    public void invalid() {
        try {
            new PathFilter(Set.of(), Set.of("/etc"));
            fail();
        } catch (IllegalStateException ignore) {
            // expected
        }

        try {
            new PathFilter(Set.of("/etc/workflow"), Set.of("/etc"));
            fail();
        } catch (IllegalStateException ignore) {
            // expected
        }

        try {
            new PathFilter(Set.of(), Set.of());
            fail();
        } catch (IllegalStateException ignore) {
            // expected
        }
    }

    @Test
    public void checkPathsAreAbsoluteOk() {
        new PathFilter(List.of("/a", "/b", "/c"), List.of());
    }

    @Test(expected = IllegalStateException.class)
    public void invalidIncludedStartsWithEmpty() {
        new PathFilter(List.of( "/a", " /b"), List.of());
    }

    @Test(expected = IllegalStateException.class)
    public void invalidExcludedStartsWithEmpty() {
        new PathFilter(List.of(), List.of( "/a", " /b"));
    }

    @Test(expected = IllegalStateException.class)
    public void invalidIncludedDoesNotStartWithSlash() {
        new PathFilter(List.of("a"), List.of());
    }

    @Test(expected = IllegalStateException.class)
    public void invalidExcludedDoesNotStartWithSlash() {
        new PathFilter(List.of(), List.of("a"));
    }

    @Test
    public void getStringsLenientNodeBuilder_MultipleValues() {
        NodeBuilder root = EMPTY_NODE.builder();
        @NotNull NodeBuilder b1 = root.setProperty(createProperty("propMultiple", Set.of("/p1", "/p2"), Type.STRINGS));
        assertEquals(Set.of("/p1", "/p2"), toSet(PathFilter.getStrings(b1.getProperty("propMultiple"), Set.of("default"))));
    }

    @Test
    public void getStringsLenientNodeBuilder_SingleValueAsSet() {
        NodeBuilder root = EMPTY_NODE.builder();
        @NotNull NodeBuilder b1 = root.setProperty(createProperty("propMultiple", Set.of("/p1"), Type.STRINGS));
        assertEquals(Set.of("/p1"), toSet(PathFilter.getStrings(b1.getProperty("propMultiple"), Set.of("default"))));
    }

    @Test
    public void getStringsLenientNodeBuilder_SingleValueAsString() {
        NodeBuilder root = EMPTY_NODE.builder();
        @NotNull NodeBuilder b1 = root.setProperty(createProperty("propMultiple", "/p1", Type.STRING));
        assertEquals(Set.of("/p1"), toSet(PathFilter.getStrings(b1.getProperty("propMultiple"), Set.of("default"))));
    }

    @Test
    public void getStringsLenientNodeBuilder_NoValue() {
        NodeBuilder root = EMPTY_NODE.builder();
        assertEquals(Set.of("default"), toSet(PathFilter.getStrings(root.getProperty("propMultiple"), Set.of("default"))));
    }

    @Test
    public void getStringsLenientNodeBuilder_WrongType() {
        NodeBuilder root = EMPTY_NODE.builder();
        @NotNull NodeBuilder b1 = root.setProperty(createProperty("propMultiple", 1L, Type.LONG));
        assertEquals(Set.of("default"), toSet(PathFilter.getStrings(root.getProperty("propMultiple"), Set.of("default"))));
    }

    static private <T> Set<T> toSet(Iterable<T> iterable) {
        HashSet<T> set = new HashSet<>();
        for (T t : iterable) {
            set.add(t);
        }
        return set;
    }


}
