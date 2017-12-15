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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public class TestUtils {

    static List<String> sortPaths(List<String> paths) {
        return sortPaths(paths, emptySet());
    }

    static List<String> sortPaths(List<String> paths, Set<String> preferredElements) {
        return sortPaths(paths, new PathElementComparator(preferredElements));
    }

    static List<String> sortPaths(List<String> paths, Comparator<Iterable<String>> comparator) {
        List<Iterable<String>> copy = paths.stream()
                .map(p -> copyOf(elements(p)))
                .sorted(comparator)
                .collect(toList());
        Joiner j = Joiner.on('/');
        return copy.stream().map(e -> "/" + j.join(e)).collect(toList());
    }

    static CountingIterable<NodeStateEntry> createList(Set<String> preferred, List<String> paths) {
        return new CountingIterable<>(createEntries(sortPaths(paths, preferred)));
    }

    static Iterable<NodeStateEntry> createEntries(List<String> paths) {
        return Iterables.transform(paths, p -> new NodeStateEntry(createNodeState(p), p));
    }

    private static NodeState createNodeState(String p) {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("path", p);
        return builder.getNodeState();
    }
}
