package org.apache.jackrabbit.oak.plugins.multiplex;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.HasNativeNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.Nullable;

public class MultiplexingNodeBuilder implements NodeBuilder, HasNativeNodeBuilder {

    private final String path;

    private final MultiplexingContext ctx;

    private final Map<MountedNodeStore, NodeBuilder> rootBuilders;

    private NodeBuilder wrappedNodeBuilder;

    public MultiplexingNodeBuilder(String path, MultiplexingContext ctx, Map<MountedNodeStore, NodeBuilder> rootBuilders) {
        this.path = path;
        this.ctx = ctx;
        this.rootBuilders = newHashMap(rootBuilders);
        
        checkArgument(rootBuilders.size() == ctx.getStoresCount(), "Got %s builders but the context manages %s stores", rootBuilders.size(), ctx.getStoresCount());
    }
    
    public Map<MountedNodeStore, NodeBuilder> getRootBuilders() {
        return rootBuilders;
    }
    
    public String getPath() {
        return path;
    }

    @Override
    public NodeState getNodeState() {
        return new MultiplexingNodeState(path, ctx, Collections.<String>emptyList(), transformValues(rootBuilders, new Function<NodeBuilder, NodeState>() {
            @Override
            public NodeState apply(@Nullable NodeBuilder input) {
                return input.getNodeState();
            }
        }));
    }

    @Override
    public NodeState getBaseState() {
        return new MultiplexingNodeState(path, ctx, Collections.<String>emptyList(), transformValues(rootBuilders, new Function<NodeBuilder, NodeState>() {
            @Override
            public NodeState apply(@Nullable NodeBuilder input) {
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
    public long getChildNodeCount(long max) {
        long count = 0;

        for (NodeBuilder parent : getBuildersForPath(path)) {
            long mountCount = parent.getChildNodeCount(max);
            if (mountCount == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            count += mountCount;
        }

        return count;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return concat(transform(getBuildersForPath(path), new Function<NodeBuilder, Iterable<? extends String>>(){
            @Override
            public Iterable<? extends String> apply(NodeBuilder input) {
                return input.getChildNodeNames();
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
        return new MultiplexingNodeBuilder(childPath, ctx, rootBuilders);
    }

    @Override
    public NodeBuilder getChildNode(String name) throws IllegalArgumentException {
        return new MultiplexingNodeBuilder(PathUtils.concat(path, name), ctx, rootBuilders);
    }

    @Override
    public NodeBuilder setChildNode(String name) throws IllegalArgumentException {
        return setChildNode(name, EmptyNodeState.EMPTY_NODE);
    }

    @Override
    public NodeBuilder setChildNode(String name, NodeState nodeState) throws IllegalArgumentException {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore owningStore = ctx.getOwningStore(childPath);
        getNodeBuilder(owningStore, path).setChildNode(name, nodeState);
        return getChildNode(name);
    }
    
    // operations potentially affecting other mounts
    @Override
    public boolean remove() {
        return getWrappedNodeBuilder().remove();
    }

    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) throws IllegalArgumentException {
        return getWrappedNodeBuilder().moveTo(newParent, newName);
    }
    
    // blobs
    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return ctx.getMultiplexingNodeStore().createBlob(stream);
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

    @Override
    public NodeBuilder getNativeRootBuilder() {
        return rootBuilders.get(ctx.getOwningStore("/"));
    }

    private Iterable<NodeBuilder> getBuildersForPath(final String path) {
        return transform(ctx.getContributingStores(path, Collections.<String>emptyList()), new Function<MountedNodeStore, NodeBuilder>() {
            @Override
            public NodeBuilder apply(MountedNodeStore mountedNodeStore) {
                return getNodeBuilder(mountedNodeStore, path);
            }
        });
    }

    private NodeBuilder getWrappedNodeBuilder() {
        if (wrappedNodeBuilder == null || !wrappedNodeBuilder.exists()) {
            wrappedNodeBuilder = getNodeBuilder(ctx.getOwningStore(path), path);
        }
        return wrappedNodeBuilder;
    }
}
