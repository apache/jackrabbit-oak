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
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeState.accumulateChildSizes;

class MultiplexingNodeBuilder implements NodeBuilder {

    private final String path;

    private final MultiplexingContext ctx;

    private final Map<MountedNodeStore, NodeBuilder> rootBuilders;

    private final List<String> checkpoints;

    private NodeBuilder wrappedNodeBuilder;

    MultiplexingNodeBuilder(String path, MultiplexingContext ctx, List<String> checkpoints, Map<MountedNodeStore, NodeBuilder> rootBuilders) {
        this.path = path;
        this.ctx = ctx;
        this.rootBuilders = rootBuilders;
        this.checkpoints = checkpoints;

        checkArgument(rootBuilders.size() == ctx.getStoresCount(), "Got %s builders but the context manages %s stores", rootBuilders.size(), ctx.getStoresCount());
    }

    Map<MountedNodeStore, NodeBuilder> getRootBuilders() {
        return rootBuilders;
    }

    String getPath() {
        return path;
    }

    @Override
    public NodeState getNodeState() {
        return new MultiplexingNodeState(path, ctx, checkpoints, copyOf(transformValues(rootBuilders, new Function<NodeBuilder, NodeState>() {
            @Override
            public NodeState apply(NodeBuilder input) {
                return input.getNodeState();
            }
        })));
    }

    @Override
    public NodeState getBaseState() {
        return new MultiplexingNodeState(path, ctx, checkpoints, copyOf(transformValues(rootBuilders, new Function<NodeBuilder, NodeState>() {
            @Override
            public NodeState apply(NodeBuilder input) {
                return input.getBaseState();
            }
        })));
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
        List<MountedNodeStore> contributingStores = ctx.getContributingStores(path, checkpoints);
        if (contributingStores.isEmpty()) {
            return 0; // this shouldn't happen
        } else if (contributingStores.size() == 1) {
            return getWrappedNodeBuilder().getChildNodeCount(max);
        } else {
            // Count the children in each contributing store.
            return accumulateChildSizes(transform(contributingStores, new Function<MountedNodeStore, Long>() {
                @Override
                public Long apply(MountedNodeStore input) {
                    NodeBuilder contributing = getBuilderByPath(rootBuilders.get(input), path);
                    if (contributing.getChildNodeCount(max) == Long.MAX_VALUE) {
                        return Long.MAX_VALUE;
                    } else {
                        return (long) size(filter(contributing.getChildNodeNames(), ctx.belongsToStore(input, path)));
                    }
                }
            }), max);
        }
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return concat(transform(ctx.getContributingStores(path, Collections.<String>emptyList()), new Function<MountedNodeStore, Iterable<String>>() {
            @Override
            public Iterable<String> apply(final MountedNodeStore mountedNodeStore) {
                return filter(getBuilderByPath(rootBuilders.get(mountedNodeStore), path).getChildNodeNames(), ctx.belongsToStore(mountedNodeStore, path));
            }
        }));
    }

    @Override
    public boolean hasChildNode(String name) {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        return getNodeBuilder(mountedStore, path).hasChildNode(name);
    }

    @Override
    public NodeBuilder child(String name) throws IllegalArgumentException {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore owningStore = ctx.getOwningStore(childPath);
        NodeBuilder builder = rootBuilders.get(owningStore);
        for (String element : PathUtils.elements(childPath)) {
            builder = builder.child(element);
        }
        return wrapChild(builder, name);
    }

    @Override
    public MultiplexingNodeBuilder getChildNode(String name) throws IllegalArgumentException {
        return new MultiplexingNodeBuilder(PathUtils.concat(path, name), ctx, checkpoints, rootBuilders);
    }

    @Override
    public NodeBuilder setChildNode(String name) throws IllegalArgumentException {
        return setChildNode(name, EmptyNodeState.EMPTY_NODE);
    }

    @Override
    public NodeBuilder setChildNode(String name, NodeState nodeState) throws IllegalArgumentException {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore owningStore = ctx.getOwningStore(childPath);
        NodeBuilder builder = getNodeBuilder(owningStore, path).setChildNode(name, nodeState);
        return wrapChild(builder, name);
    }

    private NodeBuilder wrapChild(NodeBuilder builder, String name) {
        MultiplexingNodeBuilder child = getChildNode(name);
        child.wrappedNodeBuilder = builder;
        return child;
    }

    @Override
    public boolean remove() {
        return getWrappedNodeBuilder().remove();
    }

    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) throws IllegalArgumentException {
        return getWrappedNodeBuilder().moveTo(newParent, newName);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return ctx.createBlob(stream);
    }

    private NodeBuilder getNodeBuilder(MountedNodeStore nodeStore, String path) {
        NodeBuilder rootBuilder = rootBuilders.get(nodeStore);
        return getBuilderByPath(rootBuilder, path);
    }

    private static NodeBuilder getBuilderByPath(NodeBuilder root, String path) {
        NodeBuilder child = root;
        for (String element : PathUtils.elements(path)) {
            if (child.hasChildNode(element)) {
                child = child.getChildNode(element);
            } else {
                return MISSING_NODE.builder();
            }
        }
        return child;
    }

    private NodeBuilder getWrappedNodeBuilder() {
        if (wrappedNodeBuilder == null || !wrappedNodeBuilder.exists()) {
            wrappedNodeBuilder = getNodeBuilder(ctx.getOwningStore(path), path);
        }
        return wrappedNodeBuilder;
    }
}
