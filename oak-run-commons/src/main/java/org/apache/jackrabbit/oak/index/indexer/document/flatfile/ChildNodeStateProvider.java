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

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.PeekingIterator;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.guava.common.collect.Iterators.size;
import static org.apache.jackrabbit.guava.common.collect.Iterators.transform;
import static java.util.Collections.emptyIterator;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

class ChildNodeStateProvider {
    private final Iterable<NodeStateEntry> entries;
    private final String path;
    private final Set<String> preferredPathElements;

    public ChildNodeStateProvider(Iterable<NodeStateEntry> entries, String path, Set<String> preferredPathElements) {
        this.entries = entries;
        this.path = path;
        this.preferredPathElements = preferredPathElements;
    }

    public boolean hasChildNode(@NotNull String name) {
        return getChildNode(name).exists();
    }

    @NotNull
    public NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        boolean isPreferred = preferredPathElements.contains(name);
        Iterable<NodeStateEntry> it = () -> children(isPreferred);
        Optional<NodeStateEntry> o = StreamSupport.stream(it.spliterator(), false).filter(p -> name.equals(name(p))).findFirst();
        return o.isPresent() ? o.get().getNodeState() : MISSING_NODE;
    }

    public long getChildNodeCount(long max) {
        Iterator<NodeStateEntry> childrenIter = children();
        if (max == 1 && childrenIter.hasNext()) {
            return 1;
        }
        return size(childrenIter);
    }

    public Iterable<String> getChildNodeNames() {
        return () -> transform(children(), ChildNodeStateProvider::name);
    }

    @NotNull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return () -> transform(children(), p -> new MemoryChildNodeEntry(name(p), p.getNodeState()));
    }

    Iterator<NodeStateEntry> children() {
        return children(false);
    }

    Iterator<NodeStateEntry> children(boolean preferred) {
        PeekingIterator<NodeStateEntry> pitr = Iterators.peekingIterator(entries.iterator());
        if (!pitr.hasNext()) {
            return emptyIterator();
        }

        //Skip till current entry
        while (pitr.hasNext() && !pitr.peek().getPath().equals(path)) {
            pitr.next();
        }

        //Skip past the current find
        Validate.checkState(pitr.hasNext() && path.equals(pitr.next().getPath()),
                "Did not found path [%s] in leftover iterator. Possibly node state accessed " +
                        "after main iterator has moved past it", path);

        //Prepare an iterator to fetch all child node paths i.e. immediate and their children
        return new AbstractIterator<>() {
            @Override
            protected NodeStateEntry computeNext() {
                while (pitr.hasNext() && isAncestor(path, pitr.peek().getPath())) {
                    NodeStateEntry nextEntry = pitr.next();
                    String nextEntryPath = nextEntry.getPath();
                    if (PathUtils.isDirectAncestor(path, nextEntryPath)) {
                        String nextEntryName = PathUtils.getName(nextEntryPath);
                        if (preferred && !preferredPathElements.contains(nextEntryName)) {
                            return endOfData();
                        }
                        return nextEntry;
                    }
                }
                return endOfData();
            }
        };
    }

    private static String name(NodeStateEntry p) {
        return getName(p.getPath());
    }
}
