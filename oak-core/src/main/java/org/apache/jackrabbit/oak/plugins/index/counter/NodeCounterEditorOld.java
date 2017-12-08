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
package org.apache.jackrabbit.oak.plugins.index.counter;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An approximate descendant node counter mechanism.
 */
@Deprecated
public class NodeCounterEditorOld implements Editor {

    public static final String DATA_NODE_NAME = ":index";

    // the property that is used with the "old" (pseudo-random number generator based) method
    public static final String COUNT_PROPERTY_NAME = ":count";

    // the property that is used with the "new" (hash of the path based) method
    public static final String COUNT_HASH_PROPERTY_NAME = ":cnt";

    public static final int DEFAULT_RESOLUTION = 1000;

    private final NodeCounterRoot root;
    private final NodeCounterEditorOld parent;
    private final String name;
    private long countOffset;
    private SipHash hash;

    public NodeCounterEditorOld(NodeCounterRoot root, NodeCounterEditorOld parent, String name, SipHash hash) {
        this.parent = parent;
        this.root = root;
        this.name = name;
        this.hash = hash;
    }

    private SipHash getHash() {
        if (hash != null) {
            return hash;
        }
        SipHash h;
        if (parent == null) {
            h = new SipHash(root.seed);
        } else {
            h = new SipHash(parent.getHash(), name.hashCode());
        }
        this.hash = h;
        return h;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        // nothing to do
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (NodeCounter.COUNT_HASH) {
            leaveNew(before, after);
            return;
        }
        leaveOld(before, after);
    }

    private void leaveOld(NodeState before, NodeState after)
            throws CommitFailedException {
        long offset = ApproximateCounter.calculateOffset(
                countOffset, root.resolution);
        if (offset == 0) {
            return;
        }
        // only read the value of the property if really needed
        NodeBuilder builder = getBuilder();
        PropertyState p = builder.getProperty(COUNT_PROPERTY_NAME);
        long count = p == null ? 0 : p.getValue(Type.LONG);
        offset = ApproximateCounter.adjustOffset(count,
                offset, root.resolution);
        if (offset == 0) {
            return;
        }
        count += offset;
        root.callback.indexUpdate();
        if (count == 0) {
            if (builder.getChildNodeCount(1) >= 0) {
                builder.removeProperty(COUNT_PROPERTY_NAME);
            } else {
                builder.remove();
            }
        } else {
            builder.setProperty(COUNT_PROPERTY_NAME, count);
        }
    }

    public void leaveNew(NodeState before, NodeState after)
            throws CommitFailedException {
        if (countOffset == 0) {
            return;
        }
        NodeBuilder builder = getBuilder();
        PropertyState p = builder.getProperty(COUNT_HASH_PROPERTY_NAME);
        long count = p == null ? 0 : p.getValue(Type.LONG);
        count += countOffset;
        root.callback.indexUpdate();
        if (count <= 0) {
            if (builder.getChildNodeCount(1) >= 0) {
                builder.removeProperty(COUNT_HASH_PROPERTY_NAME);
            } else {
                builder.remove();
            }
        } else {
            builder.setProperty(COUNT_HASH_PROPERTY_NAME, count);
        }
    }

    private NodeBuilder getBuilder() {
        if (parent == null) {
            return root.definition.child(DATA_NODE_NAME);
        }
        return parent.getBuilder().child(name);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        // nothing to do
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        // nothing to do
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        // nothing to do
    }

    @Override
    @CheckForNull
    public Editor childNodeChanged(String name, NodeState before, NodeState after)
            throws CommitFailedException {
        return getChildIndexEditor(name, null);
    }

    @Override
    @CheckForNull
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        if (NodeCounter.COUNT_HASH) {
            SipHash h = new SipHash(getHash(), name.hashCode());
            // with bitMask=1024: with a probability of 1:1024,
            if ((h.hashCode() & root.bitMask) == 0) {
                // add 1024
                count(root.bitMask + 1);
            }
            return getChildIndexEditor(name, h);
        }
        count(1);
        return getChildIndexEditor(name, null);
    }

    @Override
    @CheckForNull
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        if (NodeCounter.COUNT_HASH) {
            SipHash h = new SipHash(getHash(), name.hashCode());
            // with bitMask=1024: with a probability of 1:1024,
            if ((h.hashCode() & root.bitMask) == 0) {
                // subtract 1024                
                count(-(root.bitMask + 1));
            }
            return getChildIndexEditor(name, h);
        }
        count(-1);
        return getChildIndexEditor(name, null);
    }

    private void count(int offset) {
        countOffset += offset;
        if (parent != null) {
            parent.count(offset);
        }
    }

    private Editor getChildIndexEditor(String name, SipHash hash) {
        return new NodeCounterEditorOld(root, this, name, hash);
    }

    public static class NodeCounterRoot {
        final int resolution;
        final long seed;
        final int bitMask;
        final NodeBuilder definition;
        final NodeState root;
        final IndexUpdateCallback callback;

        NodeCounterRoot(int resolution, long seed, NodeBuilder definition, NodeState root, IndexUpdateCallback callback) {
            this.resolution = resolution;
            this.seed = seed;
            // if resolution is 1000, then the bitMask is 1023 (bits 0..9 set)
            this.bitMask = (Integer.highestOneBit(resolution) * 2) - 1;
            this.definition = definition;
            this.root = root;
            this.callback = callback;
        }
    }

}