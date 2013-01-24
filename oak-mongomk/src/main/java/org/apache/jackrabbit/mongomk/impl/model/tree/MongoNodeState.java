/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mongomk.impl.model.tree;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.jackrabbit.mk.model.tree.AbstractChildNode;
import org.apache.jackrabbit.mk.model.tree.AbstractNodeState;
import org.apache.jackrabbit.mk.model.tree.AbstractPropertyState;
import org.apache.jackrabbit.mk.model.tree.ChildNode;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.model.tree.PropertyState;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * This dummy NodeStore implementation is needed in order to be able to reuse
 * Oak's DiffBuilder in MongoMK.
 */
public class MongoNodeState extends AbstractNodeState {

    private final Node node;

    /**
     * Create a node state with the supplied node.
     *
     * @param node Node.
     */
    public MongoNodeState(Node node) {
        this.node = node;
    }

    /**
     * Returns the underlying node.
     *
     * @return The underlying node.
     */
    public Node unwrap() {
        return node;
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
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
                        return new SimplePropertyState(entry.getKey(), entry.getValue());
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
    public Iterable<? extends ChildNode> getChildNodeEntries(final long offset,
            final int count) {
        if (count < -1) {
            throw new IllegalArgumentException("Illegal count: " + count);
        }

        if (offset > Integer.MAX_VALUE) {
            return Collections.emptyList();
        }

        return new Iterable<ChildNode>() {
            @Override
            public Iterator<ChildNode> iterator() {
                final Iterator<Node> iterator =
                        node.getChildNodeEntries((int) offset, count);
                return new Iterator<ChildNode>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }
                    @Override
                    public ChildNode next() {
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

    private ChildNode getChildNodeEntry(final Node entry) {

        return new AbstractChildNode() {
            @Override
            public String getName() {
                return PathUtils.getName(entry.getPath());
            }
            @Override
            public NodeState getNode() {
                try {
                    return new MongoNodeState(entry);
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected error", e);
                }
            }
        };
    }

    private static class SimplePropertyState extends AbstractPropertyState {
        private final String name;
        private final String value;

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
}
