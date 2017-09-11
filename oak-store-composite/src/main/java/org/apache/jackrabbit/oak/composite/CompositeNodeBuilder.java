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
package org.apache.jackrabbit.oak.composite;

import com.google.common.collect.FluentIterable;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.MAX_VALUE;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.composite.CompositeNodeState.STOP_COUNTING_CHILDREN;
import static org.apache.jackrabbit.oak.composite.CompositeNodeState.accumulateChildSizes;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.checkValidName;

class CompositeNodeBuilder implements NodeBuilder {

    private final CompositionContext ctx;

    private final NodeMap<NodeBuilder> nodeBuilders;

    CompositeNodeBuilder(NodeMap<NodeBuilder> nodeBuilders, CompositionContext ctx) {
        this.ctx = ctx;
        this.nodeBuilders = nodeBuilders;
    }

    NodeBuilder getNodeBuilder(MountedNodeStore mns) {
        return nodeBuilders.get(mns);
    }

    @Override
    public CompositeNodeState getNodeState() {
        return new CompositeNodeState(getPath(), nodeBuilders.getAndApply((mns, b) -> b.exists() ? b.getNodeState() : MISSING_NODE), ctx);
    }

    @Override
    public CompositeNodeState getBaseState() {
        return new CompositeNodeState(getPath(), nodeBuilders.getAndApply((mns, b) -> b.getBaseState()), ctx);
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

    // child-related methods, require composition
    @Override
    public long getChildNodeCount(final long max) {
        List<MountedNodeStore> contributingStores = ctx.getContributingStoresForBuilders(getPath(), nodeBuilders);
        if (contributingStores.isEmpty()) {
            return 0; // this shouldn't happen
        } else if (contributingStores.size() == 1) {
            return getWrappedNodeBuilder().getChildNodeCount(max);
        } else {
            // Count the children in each contributing store.
            return accumulateChildSizes(FluentIterable.from(contributingStores)
                    .transformAndConcat(mns -> {
                        NodeBuilder node = nodeBuilders.get(mns);
                        if (node.getChildNodeCount(max) == MAX_VALUE) {
                            return singleton(STOP_COUNTING_CHILDREN);
                        } else {
                            return FluentIterable.from(node.getChildNodeNames()).filter(e -> belongsToStore(mns, e));
                        }
                    }), max);
        }
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return FluentIterable.from(ctx.getContributingStoresForBuilders(getPath(), nodeBuilders))
                .transformAndConcat(mns -> FluentIterable
                        .from(nodeBuilders.get(mns).getChildNodeNames())
                        .filter(e -> belongsToStore(mns, e)));
    }

    @Override
    public boolean hasChildNode(String name) {
        String childPath = simpleConcat(getPath(), name);
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

    @Override
    public NodeBuilder getChildNode(final String name) {
        String childPath = simpleConcat(getPath(), name);
        if (!ctx.shouldBeComposite(childPath)) {
            return nodeBuilders.get(ctx.getOwningStore(childPath)).getChildNode(name);
        }
        return new CompositeNodeBuilder(nodeBuilders.lazyApply((mns, b) -> b.getChildNode(name)), ctx);
    }

    @Override
    public NodeBuilder setChildNode(String name) throws IllegalArgumentException {
        return setChildNode(name, EmptyNodeState.EMPTY_NODE);
    }

    @Override
    public NodeBuilder setChildNode(final String name, NodeState nodeState) {
        checkState(exists(), "This builder does not exist: " + PathUtils.getName(getPath()));
        String childPath = simpleConcat(getPath(), name);
        final MountedNodeStore childStore = ctx.getOwningStore(childPath);
        if (childStore != ctx.getGlobalStore() && !nodeBuilders.get(childStore).exists()) {
            throw new IllegalStateException("The mount root doesn't exist: " + getPath() + " for " + childStore);
        }
        final NodeBuilder childBuilder = nodeBuilders.get(childStore).setChildNode(name, nodeState);
        if (!ctx.shouldBeComposite(childPath)) {
            return childBuilder;
        }
        return new CompositeNodeBuilder(nodeBuilders.lazyApply((mns, b) -> b.getChildNode(name)).replaceNode(childStore, childBuilder), ctx);
    }

    @Override
    public boolean remove() {
        return getWrappedNodeBuilder().remove();
    }

    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) {
        return getWrappedNodeBuilder().moveTo(newParent, newName);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return ctx.createBlob(stream);
    }

    private NodeBuilder getWrappedNodeBuilder() {
        return nodeBuilders.get(ctx.getGlobalStore());
    }

    String getPath() {
        return ((MemoryNodeBuilder) getWrappedNodeBuilder()).getPath();
    }

    private boolean belongsToStore(MountedNodeStore mns, String childName) {
        return ctx.belongsToStore(mns, getPath(), childName);
    }

    /**
     * This simplified version of {@link PathUtils#concat(String, String)} method
     * assumes that the parentPath is valid and not null, while the second argument
     * is just a name (not a subpath).
     *
     * @param parentPath the parent path
     * @param name       name to concatenate
     * @return the parentPath concatenated with name
     */
    static String simpleConcat(String parentPath, String name) {
        checkValidName(name);
        if (PathUtils.denotesRoot(parentPath)) {
            return parentPath + name;
        } else {
            return new StringBuilder(parentPath.length() + name.length() + 1)
                    .append(parentPath)
                    .append('/')
                    .append(name)
                    .toString();
        }
    }
}