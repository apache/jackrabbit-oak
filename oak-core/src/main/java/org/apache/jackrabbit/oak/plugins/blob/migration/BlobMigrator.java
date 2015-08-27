package org.apache.jackrabbit.oak.plugins.blob.migration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobMigrator {

    private static final Logger log = LoggerFactory.getLogger(BlobMigrator.class);

    private static final int MERGE_LIMIT = 100;

    private static final int MERGE_TIMEOUT = 30;

    private final SplitBlobStore blobStore;

    private final NodeStore nodeStore;

    private final AtomicBoolean stopMigration = new AtomicBoolean(false);

    private DfsNodeIterator nodeIterator;

    private NodeBuilder rootBuilder;

    private long lastCommit;

    private int migratedNodes;

    private volatile String lastPath;

    private volatile int totalMigratedNodes;

    public BlobMigrator(SplitBlobStore blobStore, NodeStore nodeStore) {
        this.blobStore = blobStore;
        this.nodeStore = nodeStore;
        refreshAndReset();
    }

    public boolean start() throws IOException {
        totalMigratedNodes = 0;
        refreshAndReset();
        return migrate();
    }

    public boolean migrate() throws IOException {
        do {
            while (nodeIterator.hasNext()) {
                lastPath = nodeIterator.getPath();
                if (stopMigration.getAndSet(false)) {
                    if (migratedNodes > 0) {
                        commit();
                    }
                    return false;
                }
                migrateNode(rootBuilder, nodeIterator);
                if (timeToCommit()) {
                    commit();
                }
            }
            // at this point we iterated over the whole repository
            // the last thing to do is to check if we don't have
            // any nodes waiting to be migrated. if the operation
            // fails we have to start from the beginning
        } while (migratedNodes > 0 && !commit());
        return true;
    }

    private boolean commit() {
        try {
            nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            totalMigratedNodes += migratedNodes;
            log.info("{} nodes merged succesfully. Nodes migrated in this session: {}", migratedNodes, totalMigratedNodes);
            lastCommit = System.currentTimeMillis();
            migratedNodes = 0;
            return true;
        } catch (CommitFailedException e) {
            log.error("Can't commit. Resetting the migrator", e);
            refreshAndReset();
            return false;
        }
    }

    private boolean timeToCommit() {
        final long changesMerged = (System.currentTimeMillis() - lastCommit) / 1000;
        if (migratedNodes >= MERGE_LIMIT) {
            log.info("Migrated nodes count: {}. Merging changes.", migratedNodes);
            return true;
        } else if (migratedNodes > 0 && changesMerged >= MERGE_TIMEOUT) {
            log.info("Changes have been merged {}s ago. Merging {} nodes.", changesMerged, migratedNodes);
            return true;
        }
        return false;
    }

    public void stop() {
        stopMigration.set(true);
    }

    public String getLastProcessedPath() {
        return lastPath;
    }

    public int getTotalMigratedNodes() {
        return totalMigratedNodes;
    }

    private void refreshAndReset() {
        final NodeState rootState = nodeStore.getRoot();
        rootBuilder = rootState.builder();
        nodeIterator = new DfsNodeIterator(rootState);
        lastPath = null;
        lastCommit = System.currentTimeMillis();
        migratedNodes = 0;
    }

    private void migrateNode(NodeBuilder rootBuilder, DfsNodeIterator iterator) throws IOException {
        final ChildNodeEntry node = iterator.next();
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
                final NodeBuilder builder = iterator.getBuilder(rootBuilder);
                if (builder.exists()) {
                    builder.setProperty(newProperty);
                    migratedNodes++;
                    log.info("Migrated property {}/{}", lastPath, property.getName());
                } else {
                    log.warn("Can't migrate blobs for a non-existing node: {}", lastPath);
                }
            }
        }
    }

    private PropertyState migrateProperty(PropertyState propertyState) throws IOException {
        final Blob oldBlob = propertyState.getValue(Type.BINARY);
        final String blobId = oldBlob.getContentIdentity();
        if (blobStore.isMigrated(blobId)) {
            return null;
        }

        final String newBlobId = blobStore.writeBlob(oldBlob.getNewStream());
        final Blob newBlob = new BlobStoreBlob(blobStore, newBlobId);
        final PropertyBuilder<Blob> builder = new PropertyBuilder<Blob>(Type.BINARY);
        builder.assignFrom(propertyState);
        builder.setValue(newBlob);
        return builder.getPropertyState();
    }

    private PropertyState migrateMultiProperty(PropertyState propertyState) throws IOException {
        final Iterable<Blob> oldBlobs = propertyState.getValue(Type.BINARIES);
        final List<Blob> newBlobs = new ArrayList<Blob>();
        final PropertyBuilder<Blob> builder = new PropertyBuilder<Blob>(Type.BINARY);
        builder.assignFrom(propertyState);
        boolean blobUpdated = false;
        for (final Blob oldBlob : oldBlobs) {
            final String blobId = oldBlob.getContentIdentity();
            if (blobStore.isMigrated(blobId)) {
                newBlobs.add(new BlobStoreBlob(blobStore, blobId));
            } else {
                final String newBlobId = blobStore.writeBlob(oldBlob.getNewStream());
                final Blob newBlob = new BlobStoreBlob(blobStore, newBlobId);
                newBlobs.add(newBlob);
                blobUpdated = true;
            }
        }
        if (blobUpdated) {
            builder.setValues(newBlobs);
            return builder.getPropertyState();
        } else {
            return null;
        }
    }
}