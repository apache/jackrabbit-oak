package org.apache.jackrabbit.oak.composite.checks;

import java.util.Collections;
import java.util.UUID;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.composite.checks.NodeTypeMountedNodeStoreChecker.Context;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

public class NodeTypeMountedNodeStoreCheckerTest {

    @Test(expected = IllegalRepositoryStateException.class)
    public void referenceableNodeIsDetected() throws CommitFailedException {
        
        MemoryNodeStore root = new MemoryNodeStore();
        MemoryNodeStore mount = new MemoryNodeStore();
        
        NodeBuilder builder = mount.getRoot().builder();
        builder.child("first")
            .setProperty(PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES, 
                Collections.singletonList(JcrConstants.MIX_REFERENCEABLE), Type.NAMES))
            .setProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString());
        
        mount.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        MountInfoProvider mip = Mounts.newBuilder()
                .readOnlyMount("first", "/first")
                .build();
        
        NodeTypeMountedNodeStoreChecker checker = new NodeTypeMountedNodeStoreChecker(JcrConstants.MIX_REFERENCEABLE, "test error");
        Context context = checker.createContext(root);
        ErrorHolder errorHolder = new ErrorHolder();
        
        checker.check(new MountedNodeStore(mip.getMountByName("first"), mount), TreeFactory.createReadOnlyTree(mount.getRoot()).getChild("first"), errorHolder, context);
        
        errorHolder.end();
    }
    
    @Test
    public void referenceableNodeInWhitelistIsSkipped() throws CommitFailedException {
        
        MemoryNodeStore root = new MemoryNodeStore();
        MemoryNodeStore mount = new MemoryNodeStore();
        
        NodeBuilder builder = mount.getRoot().builder();
        builder.child("first")
        .setProperty(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, 
                JcrConstants.NT_RESOURCE, Type.NAME))
        .setProperty(PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES, 
                Collections.singletonList(JcrConstants.MIX_REFERENCEABLE), Type.NAMES))
        .setProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString());
        
        mount.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        MountInfoProvider mip = Mounts.newBuilder()
                .readOnlyMount("first", "/first")
                .build();
        
        NodeTypeMountedNodeStoreChecker checker = new NodeTypeMountedNodeStoreChecker(JcrConstants.MIX_REFERENCEABLE, "test error", 
                JcrConstants.NT_RESOURCE);
        Context context = checker.createContext(root);
        ErrorHolder errorHolder = new ErrorHolder();
        
        checker.check(new MountedNodeStore(mip.getMountByName("first"), mount), TreeFactory.createReadOnlyTree(mount.getRoot()).getChild("first"), errorHolder, context);
        
        errorHolder.end();
    }
}
