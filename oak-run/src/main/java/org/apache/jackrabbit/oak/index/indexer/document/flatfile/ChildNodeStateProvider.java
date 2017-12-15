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

import javax.annotation.Nonnull;

import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.limit;
import static com.google.common.collect.Iterators.size;
import static com.google.common.collect.Iterators.transform;
import static java.util.Collections.emptyIterator;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

class ChildNodeStateProvider {
    private final Iterable<NodeStateEntry> entries;
    private final String path;
    private final int checkChildLimit;

    public ChildNodeStateProvider(Iterable<NodeStateEntry> entries, String path, int checkChildLimit) {
        this.entries = entries;
        this.path = path;
        this.checkChildLimit = checkChildLimit;
    }

    public boolean hasChildNode(@Nonnull String name) {
        return getChildNode(name).exists();
    }

    @Nonnull
    public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
        Optional<NodeStateEntry> o = Iterators.tryFind(limit(children(), checkChildLimit), p -> name.equals(name(p)));
        return o.isPresent() ? o.get().getNodeState() : MISSING_NODE;
    }

    public long getChildNodeCount(long max) {
        if (max == 1 && children().hasNext()) {
            return 1;
        }
        return size(children());
    }

    public Iterable<String> getChildNodeNames() {
        return () -> transform(children(), p -> name(p));
    }

    @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return () -> transform(children(), p -> new MemoryChildNodeEntry(name(p), p.getNodeState()));
    }

    Iterator<NodeStateEntry> children() {
        PeekingIterator<NodeStateEntry> pitr = Iterators.peekingIterator(entries.iterator());
        if (!pitr.hasNext()) {
            return emptyIterator();
        }

        //Skip till current entry
        while (pitr.hasNext() && !pitr.peek().getPath().equals(path)) {
            pitr.next();
        }

        //Skip past the current find
        checkState(pitr.hasNext() && path.equals(pitr.next().getPath()),
                "Did not found path [%s] in leftover iterator. Possibly node state accessed " +
                        "after main iterator has moved past it", path);

        return new AbstractIterator<NodeStateEntry>() {
            @Override
            protected NodeStateEntry computeNext() {
                if (pitr.hasNext() && isImmediateChild(pitr.peek().getPath())) {
                    return pitr.next();
                }
                return endOfData();
            }
        };
    }

    private static String name(NodeStateEntry p) {
        return getName(p.getPath());
    }

    private boolean isImmediateChild(String childPath){
        return getParentPath(childPath).equals(path);
    }
}
