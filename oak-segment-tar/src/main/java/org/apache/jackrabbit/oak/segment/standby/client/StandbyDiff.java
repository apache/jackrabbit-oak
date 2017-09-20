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

package org.apache.jackrabbit.oak.segment.standby.client;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.CancelableDiff;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StandbyDiff implements NodeStateDiff {

    private static final Logger log = LoggerFactory.getLogger(StandbyDiff.class);

    private final NodeBuilder builder;

    private final FileStore store;

    private final StandbyClient client;

    private final boolean hasDataStore;

    private final String path;

    private final Supplier<Boolean> running;

    StandbyDiff(NodeBuilder builder, FileStore store, StandbyClient client, Supplier<Boolean> running) {
        this(builder, store, client, "/", running);
    }

    private StandbyDiff(NodeBuilder builder, FileStore store, StandbyClient client, String path,
            Supplier<Boolean> running) {
        this.builder = builder;
        this.store = store;
        this.hasDataStore = store.getBlobStore() != null;
        this.client = client;
        this.path = path;
        this.running = running;
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        builder.setProperty(after);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        builder.setProperty(after);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        builder.removeProperty(before.getName());
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        SegmentNodeState processed = process(name, EMPTY_NODE, after, EMPTY_NODE.builder());
        if (processed != null) {
            builder.setChildNode(name, processed);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        SegmentNodeState processed = process(name, before, after, builder.getChildNode(name));
        if (processed != null) {
            builder.setChildNode(name, processed);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        builder.getChildNode(name).remove();
        return true;
    }

    public SegmentNodeState process(String name, NodeState before, NodeState after, NodeBuilder onto) {
        return new StandbyDiff(onto, store, client, path + name + "/", running).diff(name, before, after);
    }

    SegmentNodeState diff(String name, NodeState before, NodeState after) {
        if (after instanceof SegmentNodeState) {
            if ("checkpoints".equals(name)) {
                // if we're on the /checkpoints path, there's no need for a deep
                // traversal to verify binaries
                return (SegmentNodeState) after;
            }

            if (!hasDataStore) {
                return (SegmentNodeState) after;
            }

            // has external data store, we need a deep
            // traversal to verify binaries

            for (PropertyState propertyState : after.getProperties()) {
                fetchBinary(propertyState);
            }

            boolean success = after.compareAgainstBaseState(before, new CancelableDiff(this, newCanceledSupplier()));
            if (success) {
                return (SegmentNodeState) after;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private Supplier<Boolean> newCanceledSupplier() {
        return new Supplier<Boolean>() {

            @Override
            public Boolean get() {
                return !running.get();
            }

        };
    }

    private PropertyState fetchBinary(PropertyState property) {
        Type<?> type = property.getType();

        if (type == BINARY) {
            fetchBinary(property.getValue(Type.BINARY), property.getName());
        } else if (type == BINARIES) {
            for (Blob blob : property.getValue(BINARIES)) {
                fetchBinary(blob, property.getName());
            }
        }

        return property;
    }

    private void fetchBinary(Blob b, String pName) {
        if (b instanceof SegmentBlob) {
            fetchBinary((SegmentBlob) b, pName);
        } else {
            log.warn("Unknown Blob {} at {}, ignoring", b.getClass().getName(), path + "#" + pName);
        }
    }

    private void fetchBinary(SegmentBlob sb, String pName) {
        if (sb.isExternal() && hasDataStore && sb.getReference() == null) {
            String blobId = sb.getBlobId();

            if (blobId == null) {
                return;
            }

            try {
                fetchAndStoreBlob(blobId, pName);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void fetchAndStoreBlob(String blobId, String pName) throws InterruptedException {
        InputStream in = client.getBlob(blobId);

        if (in == null) {
            throw new IllegalStateException("Unable to load remote blob " + blobId + " at " + path + "#" + pName);
        }

        try {
            BlobStore blobStore = store.getBlobStore();
            assert blobStore != null : "Blob store must not be null";
            blobStore.writeBlob(in);
            in.close();
        } catch (IOException f) {
            throw new IllegalStateException("Unable to persist blob " + blobId + " at " + path + "#" + pName, f);
        }
    }

}
