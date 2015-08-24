package org.apache.jackrabbit.oak.plugins.blob.migration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class BlobMigrator {

    private final SplitBlobStore blobStore;

    private final NodeStore nodeStore;

    public BlobMigrator(SplitBlobStore blobStore, NodeStore nodeStore) {
        this.blobStore = blobStore;
        this.nodeStore = nodeStore;
    }

    public void migrate() throws IOException, CommitFailedException {
        final DfsNodeIterator nodeIterator = new DfsNodeIterator(nodeStore.getRoot());
        while (nodeIterator.hasNext()) {
            migrateNode(nodeIterator.next(), nodeIterator.getPath());
        }
    }

    public void migrateNode(ChildNodeEntry node, Iterable<String> parentPath)
            throws IOException, CommitFailedException {
        final NodeState state = node.getNodeState();
        for (PropertyState property : state.getProperties()) {
            final PropertyState newProperty;
            if (property.getType() == Type.BINARY) {
                newProperty = migrateProperty(property);
            } else if (property.getType() == Type.BINARIES) {
                newProperty = migrateMultiProperty(property);
            } else {
                newProperty = null;
            }
            if (newProperty != null) {
                final NodeBuilder rootBuilder = nodeStore.getRoot().builder();
                final NodeBuilder builder = getBuilder(rootBuilder, parentPath);
                builder.getChildNode(node.getName()).setProperty(newProperty);
                nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
        }
    }

    private NodeBuilder getBuilder(NodeBuilder rootBuilder, Iterable<String> path) {
        NodeBuilder builder = rootBuilder;
        for (String name : path) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }

    private PropertyState migrateProperty(PropertyState propertyState) throws IOException {
        final Blob oldBlob = propertyState.getValue(Type.BINARY);
        final String blobId = oldBlob.getContentIdentity();
        if (blobStore.isMigrated(blobId)) {
            return null;
        }

        final Blob newBlob = nodeStore.createBlob(oldBlob.getNewStream());
        final PropertyBuilder<Blob> builder = new PropertyBuilder<Blob>(Type.BINARY);
        builder.assignFrom(propertyState);
        builder.setValue(newBlob);
        return builder.getPropertyState();
    }

    private PropertyState migrateMultiProperty(PropertyState propertyState) throws IOException {
        final Iterable<Blob> oldBlobs = propertyState.getValue(Type.BINARIES);
        final List<Blob> newBlobs = new ArrayList<Blob>();
        final PropertyBuilder<Iterable<Blob>> builder = new PropertyBuilder<Iterable<Blob>>(Type.BINARIES);
        builder.assignFrom(propertyState);
        boolean blobUpdated = false;
        for (final Blob oldBlob : oldBlobs) {
            final String blobId = oldBlob.getContentIdentity();
            if (blobStore.isMigrated(blobId)) {
                newBlobs.add(oldBlob);
            } else {
                newBlobs.add(nodeStore.createBlob(oldBlob.getNewStream()));
                blobUpdated = true;
            }
        }
        if (blobUpdated) {
            return builder.getPropertyState();
        } else {
            return null;
        }
    }

}