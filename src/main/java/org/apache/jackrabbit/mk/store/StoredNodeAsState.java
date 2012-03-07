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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.jackrabbit.mk.model.StoredNode;
import org.apache.jackrabbit.oak.tree.ChildNodeEntry;
import org.apache.jackrabbit.oak.tree.NodeState;
import org.apache.jackrabbit.oak.tree.PropertyState;

class StoredNodeAsState implements NodeState {

    private final StoredNode node;

    private final RevisionProvider provider;

    public StoredNodeAsState(StoredNode node, RevisionProvider provider) {
        this.node = node;
        this.provider = provider;
    }

    private static class SimplePropertyState implements PropertyState {

        private final String name;

        private final String value;

        public SimplePropertyState(String name, String value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public String getEncodedValue() {
            return value;
        }

    }

    public PropertyState getProperty(String name) {
        String value = node.getProperties().get(name);
        if (value != null) {
            return new SimplePropertyState(name, value);
        } else {
            return null;
        }
    }

    public long getPropertyCount() {
        return node.getProperties().size();
    }

    public Iterable<PropertyState> getProperties() {
        return new Iterable<PropertyState>() {
            public Iterator<PropertyState> iterator() {
                final Iterator<Map.Entry<String, String>> iterator =
                        node.getProperties().entrySet().iterator();
                return new Iterator<PropertyState>() {
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }
                    public PropertyState next() {
                        Map.Entry<String, String> entry = iterator.next();
                        return new SimplePropertyState(
                                entry.getKey(), entry.getValue());
                    }
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public NodeState getChildNode(String name) {
        org.apache.jackrabbit.mk.model.ChildNodeEntry entry =
                node.getChildNodeEntry(name);
        if (entry != null) {
            return getChildNodeEntry(entry).getNode();
        } else {
            return null;
        }
    }

    public long getChildNodeCount() {
        return node.getChildNodeCount();
    }

    public Iterable<ChildNodeEntry> getChildNodeEntries(
            final long offset, final long length) {
        if (length < -1) {
            throw new IllegalArgumentException("Illegal length: " + length);
        } else if (offset > Integer.MAX_VALUE) {
            return Collections.emptyList();
        } else {
            return new Iterable<ChildNodeEntry>() {
                public Iterator<ChildNodeEntry> iterator() {
                    int count = -1;
                    if (length < Integer.MAX_VALUE) {
                        count = (int) length;
                    }
                    final Iterator<org.apache.jackrabbit.mk.model.ChildNodeEntry> iterator =
                            node.getChildNodeEntries((int) offset, count);
                    return new Iterator<ChildNodeEntry>() {
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }
                        public ChildNodeEntry next() {
                            return getChildNodeEntry(iterator.next());
                        }
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }
    }

    private ChildNodeEntry getChildNodeEntry(
            final org.apache.jackrabbit.mk.model.ChildNodeEntry entry) {
        return new ChildNodeEntry() {
            public String getName() {
                return entry.getName();
            }
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

}
