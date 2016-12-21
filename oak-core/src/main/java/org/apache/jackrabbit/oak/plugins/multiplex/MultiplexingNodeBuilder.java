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
package org.apache.jackrabbit.oak.plugins.multiplex;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.MoveDetector;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import static java.lang.Long.MAX_VALUE;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeState.STOP_COUNTING_CHILDREN;
import static org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeState.accumulateChildSizes;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.checkValidName;

class MultiplexingNodeBuilder implements NodeBuilder {

    private final String path;

    private final MultiplexingContext ctx;

    private final Map<MountedNodeStore, NodeBuilder> nodeBuilders;

    private final MountedNodeStore owningStore;

    private final MultiplexingNodeBuilder parent;

    private final MultiplexingNodeBuilder rootBuilder;

    MultiplexingNodeBuilder(String path, Map<MountedNodeStore, NodeBuilder> nodeBuilders, MultiplexingContext ctx) {
        this(path, nodeBuilders, ctx, null);
    }

    private MultiplexingNodeBuilder(String path, Map<MountedNodeStore, NodeBuilder> nodeBuilders, MultiplexingContext ctx, MultiplexingNodeBuilder parent) {
        checkArgument(nodeBuilders.size() == ctx.getStoresCount(), "Got %s builders but the context manages %s stores", nodeBuilders.size(), ctx.getStoresCount());
        this.path = path;
        this.ctx = ctx;
        this.nodeBuilders = newHashMap(nodeBuilders);
        this.owningStore = ctx.getOwningStore(path);
        this.parent = parent;
        if (parent == null) {
            this.rootBuilder = this;
        } else {
            this.rootBuilder = parent.rootBuilder;
        }
    }

    Map<MountedNodeStore, NodeBuilder> getBuilders() {
        return nodeBuilders;
    }

    @Override
    public NodeState getNodeState() {
        return new MultiplexingNodeState(path, buildersToNodeStates(nodeBuilders), ctx);
    }

    @Override
    public NodeState getBaseState() {
        return new MultiplexingNodeState(path, buildersToBaseStates(nodeBuilders), ctx);
    }

    private static Map<MountedNodeStore, NodeState> buildersToNodeStates(Map<MountedNodeStore, NodeBuilder> builders) {
        return copyOf(transformValues(builders, new Function<NodeBuilder, NodeState>() {
            @Override
            public NodeState apply(NodeBuilder input) {
                if (input.exists()) {
                    return input.getNodeState();
                } else {
                    return MISSING_NODE;
                }
            }
        }));
    }

    private static Map<MountedNodeStore, NodeState> buildersToBaseStates(Map<MountedNodeStore, NodeBuilder> builders) {
        return copyOf(transformValues(builders, new Function<NodeBuilder, NodeState>() {
            @Override
            public NodeState apply(NodeBuilder input) {
                return input.getBaseState();
            }
        }));
    }

    // node or property-related methods ; directly delegate to wrapped builder
    @Override
    public boolean exists() {
        return getWrappedNodeBuilder().exists();
    }

    @Override
    public boolean isNew() {
        return getWrappedNodeBuilder().isNew();
    }

    @Override
    public boolean isNew(String name) {
        return getWrappedNodeBuilder().isNew(name);
    }

    @Override
    public boolean isModified() {
        return getWrappedNodeBuilder().isModified();
    }

    @Override
    public boolean isReplaced() {
        return getWrappedNodeBuilder().isReplaced();
    }

    @Override
    public boolean isReplaced(String name) {
        return getWrappedNodeBuilder().isReplaced(name);
    }

    @Override
    public long getPropertyCount() {
        return getWrappedNodeBuilder().getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return getWrappedNodeBuilder().getProperties();
    }

    @Override
    public boolean hasProperty(String name) {
        return getWrappedNodeBuilder().hasProperty(name);
    }

    @Override
    public PropertyState getProperty(String name) {
        return getWrappedNodeBuilder().getProperty(name);
    }

    @Override
    public boolean getBoolean(String name) {
        return getWrappedNodeBuilder().getBoolean(name);
    }

    @Override
    public String getString(String name) {
        return getWrappedNodeBuilder().getString(name);
    }

    @Override
    public String getName(String name) {
        return getWrappedNodeBuilder().getName(name);
    }

    @Override
    public Iterable<String> getNames(String name) {
        return getWrappedNodeBuilder().getNames(name);
    }

    @Override
    public NodeBuilder setProperty(PropertyState property) throws IllegalArgumentException {
        getWrappedNodeBuilder().setProperty(property);
        return this;
    }

    @Override
    public <T> NodeBuilder setProperty(String name, T value) throws IllegalArgumentException {
        getWrappedNodeBuilder().setProperty(name, value);
        return this;
    }

    @Override
    public <T> NodeBuilder setProperty(String name, T value, Type<T> type) throws IllegalArgumentException {
        getWrappedNodeBuilder().setProperty(name, value, type);
        return this;
    }

    @Override
    public NodeBuilder removeProperty(String name) {
        getWrappedNodeBuilder().removeProperty(name);
        return this;
    }

    // child-related methods, require multiplexing
    @Override
    public long getChildNodeCount(final long max) {
        List<MountedNodeStore> contributingStores = ctx.getContributingStoresForBuilders(path, nodeBuilders);
        if (contributingStores.isEmpty()) {
            return 0; // this shouldn't happen
        } else if (contributingStores.size() == 1) {
            return getWrappedNodeBuilder().getChildNodeCount(max);
        } else {
            // Count the children in each contributing store.
            return accumulateChildSizes(concat(transform(contributingStores, new Function<MountedNodeStore, Iterable<String>>() {
                @Override
                public Iterable<String> apply(MountedNodeStore input) {
                    NodeBuilder contributing = nodeBuilders.get(input);
                    if (contributing.getChildNodeCount(max) == MAX_VALUE) {
                        return singleton(STOP_COUNTING_CHILDREN);
                    } else {
                        return filter(contributing.getChildNodeNames(), ctx.belongsToStore(input, path));
                    }
                }
            })), max);
        }
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return concat(transform(ctx.getContributingStoresForBuilders(path, nodeBuilders), new Function<MountedNodeStore, Iterable<String>>() {
            @Override
            public Iterable<String> apply(final MountedNodeStore mountedNodeStore) {
                return filter(nodeBuilders.get(mountedNodeStore).getChildNodeNames(), ctx.belongsToStore(mountedNodeStore, path));
            }
        }));
    }

    @Override
    public boolean hasChildNode(String name) {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        return nodeBuilders.get(mountedStore).hasChildNode(name);
    }

    @Override
    public NodeBuilder child(String name) {
        if (hasChildNode(name)) {
            return getChildNode(name);
        } else {
            return setChildNode(name);
        }
    }

    private void createAncestors(MountedNodeStore mountedNodeStore) {
        NodeBuilder builder = rootBuilder.nodeBuilders.get(mountedNodeStore);
        for (String element : PathUtils.elements(path)) {
            builder = builder.child(element);
        }
        nodeBuilders.put(mountedNodeStore, builder);
    }

    @Override
    public NodeBuilder getChildNode(String name) {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        if (!nodeBuilders.get(mountedStore).hasChildNode(name)) {
            return MISSING_NODE.builder();
        }
        Map<MountedNodeStore, NodeBuilder> newNodeBuilders = newHashMap();
        for (MountedNodeStore mns : ctx.getAllMountedNodeStores()) {
            newNodeBuilders.put(mns, nodeBuilders.get(mns).getChildNode(name));
        }
        return new MultiplexingNodeBuilder(childPath, newNodeBuilders, ctx, this);
    }

    @Override
    public NodeBuilder setChildNode(String name) throws IllegalArgumentException {
        return setChildNode(name, EmptyNodeState.EMPTY_NODE);
    }

    @Override
    public NodeBuilder setChildNode(String name, NodeState nodeState) {
        checkState(exists(), "This builder does not exist: " + PathUtils.getName(path));

        String childPath = PathUtils.concat(path, name);
        MountedNodeStore childStore = ctx.getOwningStore(childPath);
        if (childStore != owningStore && !nodeBuilders.get(childStore).exists()) {
            createAncestors(childStore);
        }
        NodeBuilder childBuilder = nodeBuilders.get(childStore).setChildNode(name, nodeState);

        Map<MountedNodeStore, NodeBuilder> newNodeBuilders = newHashMap();
        newNodeBuilders.put(childStore, childBuilder);
        for (MountedNodeStore mns : ctx.getAllMountedNodeStores()) {
            if (!newNodeBuilders.containsKey(mns)) {
                newNodeBuilders.put(mns, nodeBuilders.get(mns).getChildNode(name));
            }
        }
        return new MultiplexingNodeBuilder(childPath, newNodeBuilders, ctx, this);
    }

    @Override
    public boolean remove() {
        return getWrappedNodeBuilder().remove();
    }

    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) {
        checkNotNull(newParent);
        checkValidName(newName);
        if ("/".equals(path) || !exists() || newParent.hasChildNode(newName)) {
            return false;
        } else {
            if (newParent.exists()) {
                annotateSourcePath();
                NodeState nodeState = getNodeState();
                newParent.setChildNode(newName, nodeState);
                remove();
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return ctx.createBlob(stream);
    }

    private NodeBuilder getWrappedNodeBuilder() {
        return nodeBuilders.get(owningStore);
    }

    private void annotateSourcePath() {
        String sourcePath = getSourcePath();
        if (!isTransientlyAdded(sourcePath)) {
            setProperty(MoveDetector.SOURCE_PATH, sourcePath);
        }
    }

    private final String getSourcePath() {
        // Traverse up the hierarchy until we encounter the first builder
        // having a source path annotation or until we hit the root
        MultiplexingNodeBuilder builder = this;
        String sourcePath = getSourcePathAnnotation(builder);
        while (sourcePath == null && builder.parent != null) {
            builder = builder.parent;
            sourcePath = getSourcePathAnnotation(builder);
        }

        if (sourcePath == null) {
            // Neither self nor any parent has a source path annotation. The source
            // path is just the path of this builder
            return getPath();
        } else {
            // The source path is the source path of the first parent having a source
            // path annotation with the relative path from this builder up to that
            // parent appended.
            return PathUtils.concat(sourcePath,
                    PathUtils.relativize(builder.getPath(), getPath()));
        }
    }

    private static String getSourcePathAnnotation(MultiplexingNodeBuilder builder) {
        PropertyState base = builder.getBaseState().getProperty(MoveDetector.SOURCE_PATH);
        PropertyState head = builder.getNodeState().getProperty(MoveDetector.SOURCE_PATH);
        if (Objects.equal(base, head)) {
            // Both null: no source path annotation
            // Both non null but equals: source path annotation is from a previous commit
            return null;
        } else {
            return head.getValue(Type.STRING);
        }
    }

    private boolean isTransientlyAdded(String path) {
        NodeState node = rootBuilder.getBaseState();
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
        }
        return !node.exists();
    }

    private String getPath() {
        return path;
    }
}