package org.apache.jackrabbit.oak.plugins.multiplex;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class MultiplexingNodeBuilder implements NodeBuilder {

    private final String path;
    private final NodeBuilder wrappedBuilder;
    private final MultiplexingContext ctx;
    private final Map<MountedNodeStore, NodeBuilder> affectedBuilders;

    public MultiplexingNodeBuilder(String path, NodeBuilder builder, MultiplexingContext ctx, Map<MountedNodeStore, NodeBuilder> affectedBuilders) {
        
        this.path = path;
        this.wrappedBuilder = builder;
        this.ctx = ctx;
        this.affectedBuilders = affectedBuilders;
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
        return new MultiplexingNodeState(path, wrappedBuilder.getNodeState(), ctx);
    }

    @Override
    public NodeState getBaseState() {
        // TODO - cache?
        return new MultiplexingNodeState(path, wrappedBuilder.getBaseState(), ctx);
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
        return getNodeState().hasChildNode(name);
    }

    @Override
    public NodeBuilder child(String name) throws IllegalArgumentException {
        
        String childPath = PathUtils.concat(path, name);
        
        MountedNodeStore owningStore = ctx.getOwningStore(childPath);
        
        NodeBuilder builder = getOrCreateNodeBuilder(owningStore);
        
        for ( String segment : PathUtils.elements(path) ) {
            builder = builder.child(segment);
        }
        
        return wrap(childPath, builder.child(name));
    }

    @Override
    public NodeBuilder getChildNode(String name) throws IllegalArgumentException {

        String childPath = PathUtils.concat(path, name);
        
        MountedNodeStore owningStore = ctx.getOwningStore(childPath);

        NodeBuilder builder = getOrCreateNodeBuilder(owningStore);
        
        for ( String segment : PathUtils.elements(path) ) {
            builder = builder.child(segment);
        }
        
        return wrap(childPath, builder.getChildNode(name));
    }

    @Override
    public NodeBuilder setChildNode(String name) throws IllegalArgumentException {
        
        return setChildNode(name, EmptyNodeState.EMPTY_NODE);
    }

    @Override
    public NodeBuilder setChildNode(String name, NodeState nodeState) throws IllegalArgumentException {
        
        String childPath = PathUtils.concat(path, name);
        
        MountedNodeStore owningStore = ctx.getOwningStore(childPath);

        NodeBuilder builder = getOrCreateNodeBuilder(owningStore);
        
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
        
        checkArgument(newParent instanceof MultiplexingNodeBuilder, 
            "Expected a %s instance, but instead got %s", MultiplexingNodeBuilder.class.getName(), newParent);
        
        MultiplexingNodeBuilder newPar = (MultiplexingNodeBuilder) newParent;
        
        String newPath = PathUtils.concat(newPar.getPath(), newName);
        
        if ( ctx.getOwningStore(path) == ctx.getOwningStore(newPath)) {
            NodeBuilder nativeNewParent = getOrCreateNodeBuilder(ctx.getOwningStore(newPath));
            for ( String element : PathUtils.elements(newPar.getPath())) {
                nativeNewParent = nativeNewParent.getChildNode(element);
            }
                
            
            wrappedBuilder.moveTo(nativeNewParent, newName);
            
            return true;
        }
        
        // TODO handle cross-store moves
        return false;
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
    
    private NodeBuilder getOrCreateNodeBuilder(MountedNodeStore nodeStore) {
        
        NodeBuilder builder = affectedBuilders.get(nodeStore);
        if ( builder == null ) {
            builder = nodeStore.getNodeStore().getRoot().builder();
            affectedBuilders.put(nodeStore, builder);
        }
        
        return builder;
    }

}
