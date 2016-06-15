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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FIXME OAK-4277: Finalise de-duplication caches
// implement configuration, monitoring and management
// add unit tests
// document, nullability
public abstract class NodeCache {

    public abstract void put(String key, RecordId value, int depth);

    @CheckForNull
    public abstract RecordId get(String key);

    @Nonnull
    public static NodeCache newNodeCache(int capacity, int maxDepth) {
        if (capacity <= 0) {
            return new NodeCache.Empty();
        } else {
            return new NodeCache.Default(capacity, maxDepth);
        }
    }

    @Nonnull
    public static Supplier<NodeCache> factory(int capacity, int maxDepth) {
        if (capacity <= 0) {
            return NodeCache.Empty.supplier();
        } else {
            return NodeCache.Default.supplier(capacity, maxDepth);
        }
    }

    private static class Empty extends NodeCache {
        static final Supplier<NodeCache> supplier() {
            return  new Supplier<NodeCache>() {
                @Override
                public NodeCache get() {
                    return new NodeCache.Empty();
                }
            };
        }

        @Override
        public synchronized void put(String key, RecordId value, int depth) { }

        @Override
        public synchronized RecordId get(String key) { return null; }
    }

    private static class Default extends NodeCache {
        private static final Logger LOG = LoggerFactory.getLogger(Default.class);

        private final int capacity;
        private final List<Map<String, RecordId>> nodes;

        private int size;

        private final Set<Integer> muteDepths = newHashSet();

        static final Supplier<NodeCache> supplier(final int capacity, final int size) {
            return new Supplier<NodeCache>() {
                @Override
                public NodeCache get() {
                    return new NodeCache.Default(capacity, size);
                }
            };
        }

        Default(int capacity, int maxDepth) {
            checkArgument(capacity > 0);
            checkArgument(maxDepth > 0);
            this.capacity = capacity;
            this.nodes = newArrayList();
            for (int k = 0; k < maxDepth; k++) {
                nodes.add(new HashMap<String, RecordId>());
            }
        }

        @Override
        public synchronized void put(String key, RecordId value, int depth) {
            // FIXME OAK-4277: Finalise de-duplication caches
            // Validate and optimise the eviction strategy.
            // Nodes with many children should probably get a boost to
            // protecting them from preemptive eviction. Also it might be
            // necessary to implement pinning (e.g. for checkpoints).
            while (size >= capacity) {
                int d = nodes.size() - 1;
                int removed = nodes.remove(d).size();
                size -= removed;
                if (removed > 0) {
                    LOG.info("Evicted cache at depth {} as size {} reached capacity {}. " +
                            "New size is {}", d, size + removed, capacity, size);
                }
            }

            if (depth < nodes.size()) {
                if (nodes.get(depth).put(key, value) == null) {
                    size++;
                }
            } else {
                if (muteDepths.add(depth)) {
                    LOG.info("Not caching {} -> {} as depth {} reaches or exceeds the maximum of {}",
                            key, value, depth, nodes.size());
                }
            }
        }

        @Override
        public synchronized RecordId get(String key) {
            for (Map<String, RecordId> map : nodes) {
                if (!map.isEmpty()) {
                    RecordId recordId = map.get(key);
                    if (recordId != null) {
                        return recordId;
                    }
                }
            }
            return null;
        }

    }

}
