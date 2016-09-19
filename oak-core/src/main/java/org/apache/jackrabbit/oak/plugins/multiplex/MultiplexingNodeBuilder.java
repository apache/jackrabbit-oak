package org.apache.jackrabbit.oak.plugins.multiplex;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.HasNativeNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class MultiplexingNodeBuilder implements NodeBuilder, HasNativeNodeBuilder {

    private final String path;
    private final NodeBuilder wrappedBuilder;
    private final MultiplexingContext ctx;
    private final Map<MountedNodeStore, NodeBuilder> affectedBuilders;

    public MultiplexingNodeBuilder(String path, NodeBuilder builder, MultiplexingContext ctx, Map<MountedNodeStore, NodeBuilder> affectedBuilders) {
        
        this.path = path;
        this.wrappedBuilder = builder;
        this.ctx = ctx;
        this.affectedBuilders = affectedBuilders;
        
        Preconditions.checkArgument(affectedBuilders.size() == ctx.getStoresCount(), "Got %s builders but the context manages %s stores",
                affectedBuilders.size(), ctx.getStoresCount());
    }
    
    // multiplexing-specific APIs

    public @Nullable NodeBuilder builderFor(MountedNodeStore mountedNodeStore) {
        
        return affectedBuilders.get(mountedNodeStore);
    }
    
    public Map<MountedNodeStore, NodeBuilder> getAffectedBuilders() {
        
        return affectedBuilders;
    }
    
    public String getPath() {
        return path;
    }
    
    // state methods

    @Override
    public NodeState getNodeState() {
        // TODO - cache?
        return new MultiplexingNodeState(path, wrappedBuilder.getNodeState(), ctx, this);
    }

    @Override
    public NodeState getBaseState() {
        
        // TODO - refactor into a common data structure MountedNodeStore -> {NodeState, NodeBuilder}
        Map<MountedNodeStore, NodeState> nodeStates = Maps.newHashMap();
        for ( Map.Entry<MountedNodeStore, NodeBuilder> entry : affectedBuilders.entrySet()) {
            nodeStates.put(entry.getKey(), entry.getValue().getBaseState());
        }
        
        // TODO - cache?
        return new MultiplexingNodeState(path, wrappedBuilder.getBaseState(), ctx, nodeStates);
    }
    
    // node or property-related methods ; directly delegate to wrapped builder

    @Override
    public boolean exists() {
        return wrappedBuilder.exists();
    }

    @Override
    public boolean isNew() {
        return wrappedBuilder.isNew();
    }

    @Override
    public boolean isNew(String name) {
        return wrappedBuilder.isNew(name);
    }

    @Override
    public boolean isModified() {
        return wrappedBuilder.isModified();
    }

    @Override
    public boolean isReplaced() {
        return wrappedBuilder.isReplaced();
    }

    @Override
    public boolean isReplaced(String name) {
        return wrappedBuilder.isReplaced(name);
    }

    @Override
    public long getPropertyCount() {
        return wrappedBuilder.getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return wrappedBuilder.getProperties();
    }

    @Override
    public boolean hasProperty(String name) {
        return wrappedBuilder.hasProperty(name);
    }

    @Override
    public PropertyState getProperty(String name) {
        return wrappedBuilder.getProperty(name);
    }

    @Override
    public boolean getBoolean(String name) {
        return wrappedBuilder.getBoolean(name);
    }

    @Override
    public String getString(String name) {
        return wrappedBuilder.getString(name);
    }

    @Override
    public String getName(String name) {
        return wrappedBuilder.getName(name);
    }

    @Override
    public Iterable<String> getNames(String name) {
        return wrappedBuilder.getNames(name);
    }

    @Override
    public NodeBuilder setProperty(PropertyState property) throws IllegalArgumentException {
        wrappedBuilder.setProperty(property);
        
        return this;
    }

    @Override
    public <T> NodeBuilder setProperty(String name, T value) throws IllegalArgumentException {
        wrappedBuilder.setProperty(name, value);
        
        return this;
    }

    @Override
    public <T> NodeBuilder setProperty(String name, T value, Type<T> type) throws IllegalArgumentException {
        wrappedBuilder.setProperty(name, value, type);
        
        return this;
    }

    @Override
    public NodeBuilder removeProperty(String name) {
        wrappedBuilder.removeProperty(name);
        
        return this;
    }
    
    // child-related methods, require multiplexing
    
    @Override
    public long getChildNodeCount(long max) {
        return getNodeState().getChildNodeCount(max);
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return getNodeState().getChildNodeNames();
    }

    @Override
    public boolean hasChildNode(String name) {
        return getChildNode(name).exists();
    }

    @Override
    public NodeBuilder child(String name) throws IllegalArgumentException {
        
        String childPath = PathUtils.concat(path, name);
        
        MountedNodeStore owningStore = ctx.getOwningStore(childPath);
        
        NodeBuilder builder = getNodeBuilder(owningStore);
        
        for ( String segment : PathUtils.elements(path) ) {
            builder = builder.child(segment);
        }
        
        return wrap(childPath, builder.child(name));
    }

    @Override
    public NodeBuilder getChildNode(String name) throws IllegalArgumentException {
        String childPath = PathUtils.concat(path, name);
        
        MountedNodeStore owningStore = ctx.getOwningStore(childPath);

        NodeBuilder builder = getNodeBuilder(owningStore);
        
        for ( String segment : PathUtils.elements(childPath) ) {
            if (builder.hasChildNode(segment)) {
                builder = builder.getChildNode(segment);
            } else {
                return MISSING_NODE.builder();
            }
        }
        
        return wrap(childPath, builder);
    }

    @Override
    public NodeBuilder setChildNode(String name) throws IllegalArgumentException {
        
        return setChildNode(name, EmptyNodeState.EMPTY_NODE);
    }

    @Override
    public NodeBuilder setChildNode(String name, NodeState nodeState) throws IllegalArgumentException {
        
        String childPath = PathUtils.concat(path, name);
        
        MountedNodeStore owningStore = ctx.getOwningStore(childPath);

        NodeBuilder builder = getNodeBuilder(owningStore);
        
        for ( String segment : PathUtils.elements(path) ) {
            builder = builder.child(segment);
        }
        
        return wrap(childPath, builder.setChildNode(name, nodeState));
    }
    
    // operations potentially affecting other mounts

    @Override
    public boolean remove() {
        // TODO - do we need to propagate to mount builders?
        return wrappedBuilder.remove();
    }

    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) throws IllegalArgumentException {
        
        return wrappedBuilder.moveTo(newParent, newName);
    }
    
    // blobs

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return ctx.getMultiplexingNodeStore().createBlob(stream);
    }
    
    // utility methods
    private NodeBuilder wrap(String childPath, NodeBuilder wrappedBuilder) {
        return new MultiplexingNodeBuilder(childPath, wrappedBuilder, ctx, affectedBuilders);
    }
    
    private NodeBuilder getNodeBuilder(MountedNodeStore nodeStore) {
        
        return checkNotNull(affectedBuilders.get(nodeStore));
    }


    @Override
    public NodeBuilder getNativeRootBuilder() {
        return affectedBuilders.get(ctx.getOwningStore("/"));
    }
}
