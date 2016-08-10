package org.apache.jackrabbit.oak.plugins.multiplex;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Maps;

public class MultiplexingNodeBuilder implements NodeBuilder {

    private final String path;
    private final NodeBuilder wrappedBuilder;
    private final MountInfoProvider mip;
    private final MountedNodeStore globalStore;
    private final List<MountedNodeStore> nonDefaultStores;
    private final Map<MountedNodeStore, NodeBuilder> nonDefaultBuilders = Maps.newHashMap();

    public MultiplexingNodeBuilder(String path, NodeBuilder builder, MountInfoProvider mip, MountedNodeStore globalStore,
            List<MountedNodeStore> nonDefaultStores) {
        
        this.path = path;
        this.wrappedBuilder = builder;
        this.mip = mip;
        this.globalStore = globalStore;
        this.nonDefaultStores = nonDefaultStores;
    }
    
    // multiplexing-specific APIs

    public @Nullable NodeBuilder builderFor(MountedNodeStore mountedNodeStore) {
        
        if ( mountedNodeStore == globalStore )
            return wrappedBuilder;

        return nonDefaultBuilders.get(mountedNodeStore);
    }
    
    // state methods

    @Override
    public NodeState getNodeState() {
        // TODO - cache?
        return new MultiplexingNodeState(path, wrappedBuilder.getNodeState(), mip, globalStore, nonDefaultStores);
    }

    @Override
    public NodeState getBaseState() {
        // TODO - cache?
        return new MultiplexingNodeState(path, wrappedBuilder.getBaseState(), mip, globalStore, nonDefaultStores);
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
        
        Mount childMount = mip.getMountByPath(childPath);
        Mount ourMount = mip.getMountByPath(path);
        
        if ( childMount == ourMount ) {
            // same mount, no need to ask other stores
            return wrap(childPath, wrappedBuilder.child(name));
        }
        
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if ( mountedNodeStore.getMount() == childMount ) {
                NodeBuilder mountBuilder = mountedNodeStore.getNodeStore().getRoot().builder();

                nonDefaultBuilders.put(mountedNodeStore, mountBuilder);
                
                for ( String segment : PathUtils.elements(childPath )) {
                    mountBuilder = mountBuilder.child(segment);
                }
                
                return wrap(childPath, mountBuilder);
            }
        }

        // 'never' happens
        throw new IllegalArgumentException("Could not find a mount for path " + childPath);
    }

    @Override
    public NodeBuilder getChildNode(String name) throws IllegalArgumentException {
        String childPath = PathUtils.concat(path, name);
        
        Mount childMount = mip.getMountByPath(childPath);
        Mount ourMount = mip.getMountByPath(path);
        
        if ( childMount == ourMount ) {
            // same mount, no need to ask other stores
            return wrap(childPath, wrappedBuilder.child(name));
        }
        
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if ( mountedNodeStore.getMount() == childMount ) {
                NodeBuilder mountBuilder = mountedNodeStore.getNodeStore().getRoot().builder();
                nonDefaultBuilders.put(mountedNodeStore, mountBuilder);
                for ( String segment : PathUtils.elements(childPath )) {
                    mountBuilder = mountBuilder.child(segment);
                }
                
                return wrap(childPath, mountBuilder);
            }
        }

        // 'never' happens
        throw new IllegalArgumentException("Could not find a mount for path " + childPath);      
    }

    @Override
    public NodeBuilder setChildNode(String name) throws IllegalArgumentException {
        
        return setChildNode(name, null);
    }

    @Override
    public NodeBuilder setChildNode(String name, NodeState nodeState) throws IllegalArgumentException {
        String childPath = PathUtils.concat(path, name);
        
        Mount childMount = mip.getMountByPath(childPath);
        Mount ourMount = mip.getMountByPath(path);
        
        if ( childMount == ourMount ) {
            // same mount, no need to ask other stores
            NodeBuilder ret = nodeState != null ? wrappedBuilder.setChildNode(name, nodeState) : wrappedBuilder.setChildNode(name);
            return wrap(childPath, ret);
        }
        
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if ( mountedNodeStore.getMount() == childMount ) {
                NodeBuilder mountBuilder = mountedNodeStore.getNodeStore().getRoot().builder();
                nonDefaultBuilders.put(mountedNodeStore, mountBuilder);
                
                for ( String segment : PathUtils.elements(path)) {
                    mountBuilder = mountBuilder.child(segment);
                }
                
                NodeBuilder ret = nodeState != null ? mountBuilder.setChildNode(name, nodeState) : mountBuilder.setChildNode(name);
                
                return wrap(childPath, ret);
            }
        }

        // 'never' happens
        throw new IllegalArgumentException("Could not find a mount for path " + childPath);
    }
    
    // operations potentially affecting other mounts

    @Override
    public boolean remove() {
        // TODO - do we need to propagate to mount builders?
        return wrappedBuilder.remove();
    }

    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return false;
    }
    
    // blobs

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    
    // utility methods
    private NodeBuilder wrap(String childPath, NodeBuilder wrappedBuilder) {
        return new MultiplexingNodeBuilder(childPath, wrappedBuilder, mip, globalStore, nonDefaultStores);
    }

}
