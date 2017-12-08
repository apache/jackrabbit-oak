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
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.HashMap;
import java.util.Map;

/**
 * An approximate descendant node counter mechanism.
 */
public class NodeCounterEditor implements Editor {

    public static final String DATA_NODE_NAME = ":index";
    
    // the property that is used with the "old" (pseudo-random number generator based) method
    public static final String COUNT_PROPERTY_NAME = ":count";
    
    // the property that is used with the "new" (hash of the path based) method
    public static final String COUNT_HASH_PROPERTY_NAME = ":cnt";
    
    public static final int DEFAULT_RESOLUTION = 1000;
    
    private final NodeCounterRoot root;
    private final NodeCounterEditor parent;
    private final String name;
    private final MountInfoProvider mountInfoProvider;
    private final Map<Mount, Integer> countOffsets;
    private final Mount currentMount;
    private final boolean mountCanChange;
    private SipHash hash;


    NodeCounterEditor(NodeCounterRoot root, MountInfoProvider mountInfoProvider) {
        this.root = root;
        this.name = "/";
        this.parent = null;
        this.mountInfoProvider = mountInfoProvider;
        this.currentMount = mountInfoProvider.getDefaultMount();
        this.mountCanChange = true;
        this.countOffsets = new HashMap<>();
    }

    private NodeCounterEditor(NodeCounterRoot root, NodeCounterEditor parent, String name, SipHash hash, MountInfoProvider mountInfoProvider) {
        this.parent = parent;
        this.root = root;
        this.name = name;
        this.hash = hash;
        this.mountInfoProvider = mountInfoProvider;
        this.countOffsets = new HashMap<>();
        if (parent.mountCanChange) {
            String path = getPath();
            this.currentMount = mountInfoProvider.getMountByPath(path);
            this.mountCanChange = currentMount.isDefault() && supportMounts(path);
        } else {
            this.currentMount = this.parent.currentMount;
            this.mountCanChange = false;
        }
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
        if (countOffsets.isEmpty()) {
            return;
        }
        boolean updated = false;
        for (Map.Entry<Mount, Integer> e : countOffsets.entrySet()) {
            long offset = ApproximateCounter.calculateOffset(e.getValue(), root.resolution);
            if (offset == 0) {
                continue;
            }
            // only read the value of the property if really needed
            NodeBuilder builder = getBuilder(e.getKey());
            PropertyState p = builder.getProperty(COUNT_PROPERTY_NAME);
            long count = p == null ? 0 : p.getValue(Type.LONG);
            offset = ApproximateCounter.adjustOffset(count, offset, root.resolution);
            if (offset == 0) {
                continue;
            }
            updated = true;
            count += offset;
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
        if (updated) {
            root.callback.indexUpdate();
        }
    }

    public void leaveNew(NodeState before, NodeState after) throws CommitFailedException {
        if (countOffsets.isEmpty()) {
            return;
        }
        root.callback.indexUpdate();
        for (Map.Entry<Mount, Integer> e : countOffsets.entrySet()) {
            NodeBuilder builder = getBuilder(e.getKey());
            int countOffset = e.getValue();

            PropertyState p = builder.getProperty(COUNT_HASH_PROPERTY_NAME);
            long count = p == null ? 0 : p.getValue(Type.LONG);
            count += countOffset;
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
    }

    private NodeBuilder getBuilder(Mount mount) {
        if (parent == null) {
            return root.definition.child(Multiplexers.getNodeForMount(mount, DATA_NODE_NAME));
        } else {
            return parent.getBuilder(mount).child(name);
        }
    }

    private String getPath() {
        if (parent == null) {
            return name;
        } else {
            return PathUtils.concat(parent.getPath(), name);
        }
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
                count(root.bitMask + 1, currentMount);
            }
            return getChildIndexEditor(name, h);
        }
        count(1, currentMount);
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
                count(-(root.bitMask + 1), currentMount);
            }
            return getChildIndexEditor(name, h);
        }
        count(-1, currentMount);
        return getChildIndexEditor(name, null);
    }
    
    private void count(int offset, Mount mount) {
        countOffsets.compute(mount, (m, v) -> v == null ? offset : v + offset);
        if (parent != null) {
            parent.count(offset, mount);
        }
    }
    
    private Editor getChildIndexEditor(String name, SipHash hash) {
        return new NodeCounterEditor(root, this, name, hash, mountInfoProvider);
    }

    private boolean supportMounts(String path) {
        return mountInfoProvider
                .getNonDefaultMounts()
                .stream()
                .anyMatch(m -> m.isSupportFragmentUnder(path) || m.isUnder(path));
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
