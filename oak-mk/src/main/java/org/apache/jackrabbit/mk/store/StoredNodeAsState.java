/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.store;

import org.apache.jackrabbit.mk.model.AbstractChildNodeEntry;
import org.apache.jackrabbit.mk.model.AbstractNodeState;
import org.apache.jackrabbit.mk.model.AbstractPropertyState;
import org.apache.jackrabbit.mk.model.ChildNode;
import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.PropertyState;
import org.apache.jackrabbit.mk.model.StoredNode;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

class StoredNodeAsState extends AbstractNodeState {

    private final StoredNode node;

    private final RevisionProvider provider;

    public StoredNodeAsState(StoredNode node, RevisionProvider provider) {
        this.node = node;
        this.provider = provider;
    }

    Id getId() {
        return node.getId();
    }

    private static class SimplePropertyState extends AbstractPropertyState {
        private final String name;
        private final String value;

        // todo make name and value not nullable
        public SimplePropertyState(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getEncodedValue() {
            return value;
        }

    }

    @Override
    public PropertyState getProperty(String name) {
        String value = node.getProperties().get(name);
        if (value != null) {
            return new SimplePropertyState(name, value);
        } else {
            return null;
        }
    }

    @Override
    public long getPropertyCount() {
        return node.getProperties().size();
    }

    @Override
    public Iterable<PropertyState> getProperties() {
        return new Iterable<PropertyState>() {
            @Override
            public Iterator<PropertyState> iterator() {
                final Iterator<Map.Entry<String, String>> iterator =
                        node.getProperties().entrySet().iterator();
                return new Iterator<PropertyState>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }
                    @Override
                    public PropertyState next() {
                        Map.Entry<String, String> entry = iterator.next();
                        return new SimplePropertyState(
                                entry.getKey(), entry.getValue());
                    }
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public NodeState getChildNode(String name) {
        ChildNode entry = node.getChildNodeEntry(name);
        if (entry != null) {
            return getChildNodeEntry(entry).getNode();
        } else {
            return null;
        }
    }

    @Override
    public long getChildNodeCount() {
        return node.getChildNodeCount();
    }

    @Override
    public Iterable<ChildNodeEntry> getChildNodeEntries(
            final long offset, final int count) {
        if (count < -1) {
            throw new IllegalArgumentException("Illegal count: " + count);
        } else if (offset > Integer.MAX_VALUE) {
            return Collections.emptyList();
        } else {
            return new Iterable<ChildNodeEntry>() {
                @Override
                public Iterator<ChildNodeEntry> iterator() {
                    final Iterator<ChildNode> iterator =
                            node.getChildNodeEntries((int) offset, count);
                    return new Iterator<ChildNodeEntry>() {
                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }
                        @Override
                        public ChildNodeEntry next() {
                            return getChildNodeEntry(iterator.next());
                        }
                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }
    }

    private ChildNodeEntry getChildNodeEntry(
            final org.apache.jackrabbit.mk.model.ChildNode entry) {
        return new AbstractChildNodeEntry() {
            @Override
            public String getName() {
                return entry.getName();
            }
            @Override
            public NodeState getNode() {
                try {
                    StoredNode child = provider.getNode(entry.getId());
                    return new StoredNodeAsState(child, provider);
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected error", e);
                }
            }
        };
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof StoredNodeAsState) {
            StoredNodeAsState other = (StoredNodeAsState) that;
            if (provider == other.provider
                    && node.getId().equals(other.node.getId())) {
                return true;
            }
        }
        return super.equals(that);
    }

}
