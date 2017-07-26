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

import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.composite.util.Memoizer;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.MoveDetector;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.Long.MAX_VALUE;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.composite.CompositeNodeState.STOP_COUNTING_CHILDREN;
import static org.apache.jackrabbit.oak.composite.CompositeNodeState.accumulateChildSizes;
import static org.apache.jackrabbit.oak.composite.CompositeNodeState.wrapWithNullCheck;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.checkValidName;

class CompositeNodeBuilder implements NodeBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeNodeBuilder.class);

    private final String path;

    private final CompositionContext ctx;

    private Function<MountedNodeStore, NodeBuilder> nodeBuilders;

    private final MountedNodeStore owningStore;

    private final CompositeNodeBuilder parent;

    private final CompositeNodeBuilder rootBuilder;

    CompositeNodeBuilder(String path, Function<MountedNodeStore, NodeBuilder> nodeBuilders, CompositionContext ctx) {
        this(path, nodeBuilders, ctx, null);
    }

    private CompositeNodeBuilder(String path, Function<MountedNodeStore, NodeBuilder> nodeBuilders, CompositionContext ctx, CompositeNodeBuilder parent) {
        this.path = path;
        this.ctx = ctx;
        this.nodeBuilders = wrapWithNullCheck(Memoizer.memoize(nodeBuilders), LOG, path);
        this.owningStore = ctx.getOwningStore(path);
        this.parent = parent;
        if (parent == null) {
            this.rootBuilder = this;
        } else {
            this.rootBuilder = parent.rootBuilder;
        }
    }

    NodeBuilder getNodeBuilder(MountedNodeStore mns) {
        return nodeBuilders.apply(mns);
    }

    @Override
    public CompositeNodeState getNodeState() {
        Map<MountedNodeStore, NodeState> states = ctx.getAllMountedNodeStores().stream().collect(Collectors.toMap(Function.identity(),
                nodeBuilders
                        .andThen(n -> n.exists() ? n.getNodeState() : MISSING_NODE)));
        return new CompositeNodeState(path, states, ctx);
    }

    @Override
    public CompositeNodeState getBaseState() {
        Map<MountedNodeStore, NodeState> states = ctx.getAllMountedNodeStores().stream().collect(Collectors.toMap(Function.identity(),
                nodeBuilders
                        .andThen(NodeBuilder::getBaseState)));
        return new CompositeNodeState(path, states, ctx);
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
        List<MountedNodeStore> contributingStores = ctx.getContributingStoresForBuilders(path, nodeBuilders);
        if (contributingStores.isEmpty()) {
            return 0; // this shouldn't happen
        } else if (contributingStores.size() == 1) {
            return getWrappedNodeBuilder().getChildNodeCount(max);
        } else {
            // Count the children in each contributing store.
            return accumulateChildSizes(FluentIterable.from(contributingStores)
                    .transformAndConcat(mns -> {
                        NodeBuilder node = nodeBuilders.apply(mns);
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
        return FluentIterable.from(ctx.getContributingStoresForBuilders(path, nodeBuilders))
                .transformAndConcat(mns -> FluentIterable
                        .from(nodeBuilders.apply(mns).getChildNodeNames())
                        .filter(e -> belongsToStore(mns, e)));
    }

    @Override
    public boolean hasChildNode(String name) {
        String childPath = simpleConcat(path, name);
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        return nodeBuilders.apply(mountedStore).hasChildNode(name);
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
        NodeBuilder builder = rootBuilder.nodeBuilders.apply(mountedNodeStore);
        for (String element : PathUtils.elements(path)) {
            builder = builder.child(element);
        }

        // the nodeBuilders function should be updated, to return the new node builder
        Map<MountedNodeStore, NodeBuilder> map = newHashMap(ctx.getAllMountedNodeStores().stream().collect(Collectors.toMap(Function.identity(), nodeBuilders)));
        map.put(mountedNodeStore, builder);
        nodeBuilders = wrapWithNullCheck(m -> map.get(m), LOG, path);
    }

    @Override
    public NodeBuilder getChildNode(final String name) {
        String childPath = simpleConcat(path, name);
        if (!ctx.shouldBeComposite(childPath)) {
            return nodeBuilders.apply(ctx.getOwningStore(childPath)).getChildNode(name);
        }
        return new CompositeNodeBuilder(childPath, nodeBuilders.andThen(b -> b.getChildNode(name)), ctx, this);
    }

    @Override
    public NodeBuilder setChildNode(String name) throws IllegalArgumentException {
        return setChildNode(name, EmptyNodeState.EMPTY_NODE);
    }

    @Override
    public NodeBuilder setChildNode(final String name, NodeState nodeState) {
        checkState(exists(), "This builder does not exist: " + PathUtils.getName(path));
        String childPath = simpleConcat(path, name);
        final MountedNodeStore childStore = ctx.getOwningStore(childPath);
        if (childStore != owningStore && !nodeBuilders.apply(childStore).exists()) {
            createAncestors(childStore);
        }
        final NodeBuilder childBuilder = nodeBuilders.apply(childStore).setChildNode(name, nodeState);
        if (!ctx.shouldBeComposite(childPath)) {
            return childBuilder;
        }

        return new CompositeNodeBuilder(childPath, m -> m == childStore ? childBuilder : nodeBuilders.apply(m).getChildNode(name), ctx, this);
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
        return nodeBuilders.apply (owningStore);
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
        CompositeNodeBuilder builder = this;
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

    private static String getSourcePathAnnotation(CompositeNodeBuilder builder) {
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

    String getPath() {
        return path;
    }

    private boolean belongsToStore(MountedNodeStore mns, String childName) {
        return ctx.belongsToStore(mns, path, childName);
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