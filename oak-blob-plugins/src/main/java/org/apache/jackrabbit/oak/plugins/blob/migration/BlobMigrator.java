/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.blob.migration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.codec.digest.DigestUtils;
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

    private DepthFirstNodeIterator nodeIterator;

    private NodeBuilder rootBuilder;

    private long lastCommit;

    private int migratedNodes;

    private volatile String lastPath;

    private volatile int totalMigratedNodes;

    public BlobMigrator(SplitBlobStore blobStore, NodeStore nodeStore) {
        this.blobStore = blobStore;
        this.nodeStore = nodeStore;
    }

    public boolean start() throws IOException {
        totalMigratedNodes = 0;
        refreshAndReset(nodeStore.getRoot());
        return migrate();
    }

    public boolean migrate() throws IOException {
        if (nodeIterator == null) {
            refreshAndReset(nodeStore.getRoot());
        }

        do {
            while (nodeIterator.hasNext()) {
                lastPath = nodeIterator.getPath();
                if (stopMigration.getAndSet(false)) {
                    if (migratedNodes > 0) {
                        tryCommit();
                    }
                    return false;
                }
                migrateNode(rootBuilder, nodeIterator);
                if (timeToCommit()) {
                    tryCommit();
                }
            }
            // at this point we iterated over the whole repository
            // the last thing to do is to check if we don't have
            // any nodes waiting to be migrated. if the operation
            // fails we have to start from the beginning
        } while (migratedNodes > 0 && !tryCommit());
        return true;
    }

    private boolean tryCommit() {
        try {
            NodeState newRoot = nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            totalMigratedNodes += migratedNodes;
            log.info("{} nodes merged succesfully. Nodes migrated in this session: {}", migratedNodes, totalMigratedNodes);
            lastCommit = System.currentTimeMillis();
            migratedNodes = 0;

            rootBuilder = newRoot.builder();
            nodeIterator = nodeIterator.switchRoot(newRoot);

            return true;
        } catch (CommitFailedException e) {
            log.error("Can't commit. Resetting the migrator", e);
            refreshAndReset(nodeStore.getRoot());
            return false;
        }
    }

    private boolean timeToCommit() {
        long changesMerged = (System.currentTimeMillis() - lastCommit) / 1000;
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

    private void refreshAndReset(NodeState rootState) {
        rootBuilder = rootState.builder();
        nodeIterator = new DepthFirstNodeIterator(rootState);
        lastPath = null;
        lastCommit = System.currentTimeMillis();
        migratedNodes = 0;
    }

    private void migrateNode(NodeBuilder rootBuilder, DepthFirstNodeIterator iterator) throws IOException {
        ChildNodeEntry node = iterator.next();
        NodeState state = node.getNodeState();
        for (PropertyState property : state.getProperties()) {
            PropertyState newProperty;
            if (property.getType() == Type.BINARY) {
                newProperty = migrateProperty(property);
            } else if (property.getType() == Type.BINARIES) {
                newProperty = migrateMultiProperty(property);
            } else {
                newProperty = null;
            }
            if (newProperty != null) {
                NodeBuilder builder = iterator.getBuilder(rootBuilder);
                if (builder.exists()) {
                    builder.setProperty(newProperty);
                    migratedNodes++;
                    log.debug("Migrated property {}/{}", lastPath, property.getName());
                } else {
                    log.warn("Can't migrate blobs for a non-existing node: {}", lastPath);
                }
            }
        }
    }

    private PropertyState migrateProperty(PropertyState propertyState) throws IOException {
        Blob oldBlob = propertyState.getValue(Type.BINARY);
        String blobId = getIdentity(oldBlob);
        if (blobStore.isMigrated(blobId)) {
            return null;
        }

        String newBlobId = blobStore.writeBlob(oldBlob.getNewStream());
        Blob newBlob = new BlobStoreBlob(blobStore, newBlobId);
        PropertyBuilder<Blob> builder = new PropertyBuilder<Blob>(Type.BINARY);
        builder.assignFrom(propertyState);
        builder.setValue(newBlob);
        return builder.getPropertyState();
    }

    private PropertyState migrateMultiProperty(PropertyState propertyState) throws IOException {
        Iterable<Blob> oldBlobs = propertyState.getValue(Type.BINARIES);
        List<Blob> newBlobs = new ArrayList<Blob>();
        PropertyBuilder<Blob> builder = new PropertyBuilder<Blob>(Type.BINARY);
        builder.assignFrom(propertyState);
        boolean blobUpdated = false;
        for (Blob oldBlob : oldBlobs) {
            String blobId = getIdentity(oldBlob);
            if (blobStore.isMigrated(blobId)) {
                newBlobs.add(new BlobStoreBlob(blobStore, blobId));
            } else {
                String newBlobId = blobStore.writeBlob(oldBlob.getNewStream());
                Blob newBlob = new BlobStoreBlob(blobStore, newBlobId);
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

    private String getIdentity(Blob blob) throws IOException {
        String id = blob.getContentIdentity();
        if (id == null) {
            id = DigestUtils.shaHex(blob.getNewStream());
        }
        return id;
    }
}