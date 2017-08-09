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

import java.io.InputStream;
import java.io.IOException;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.RecordId;
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

    /**
     * read-only traversal of the diff that has 2 properties: one is to log all
     * the content changes, second is to drill down to properly level, so that
     * missing binaries can be sync'ed if needed
     */
    private final boolean logOnly;

    StandbyDiff(NodeBuilder builder, FileStore store, StandbyClient client, Supplier<Boolean> running) {
        this(builder, store, client, "/", false, running);
    }

    private StandbyDiff(NodeBuilder builder, FileStore store, StandbyClient client, String path, boolean logOnly, Supplier<Boolean> running) {
        this.builder = builder;
        this.store = store;
        this.hasDataStore = store.getBlobStore() != null;
        this.client = client;
        this.path = path;
        this.logOnly = logOnly;
        this.running = running;
    }

    private boolean stop() {
        return !running.get();
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        if (stop()) {
            return false;
        }

        if (logOnly) {
            binaryCheck(after);
        } else {
            builder.setProperty(binaryCheck(after));
        }

        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        if (stop()) {
            return false;
        }

        if (logOnly) {
            binaryCheck(after);
        } else {
            builder.setProperty(binaryCheck(after));
        }

        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        if (stop()) {
            return false;
        }

        if (!logOnly) {
            builder.removeProperty(before.getName());
        }

        return true;
    }

    private PropertyState binaryCheck(PropertyState property) {
        Type<?> type = property.getType();

        if (type == BINARY) {
            binaryCheck(property.getValue(Type.BINARY), property.getName());
        } else if (type == BINARIES) {
            for (Blob blob : property.getValue(BINARIES)) {
                binaryCheck(blob, property.getName());
            }
        }

        return property;
    }

    private void binaryCheck(Blob b, String pName) {
        if (b instanceof SegmentBlob) {
            binaryCheck((SegmentBlob) b, pName);
        } else {
            log.warn("Unknown Blob {} at {}, ignoring", b.getClass().getName(), path + "#" + pName);
        }
    }

    private void binaryCheck(SegmentBlob sb, String pName) {
        if (sb.isExternal() && hasDataStore && sb.getReference() == null) {
            String blobId = sb.getBlobId();

            if (blobId == null) {
                return;
            }

            try {
                readBlob(blobId, pName);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void readBlob(String blobId, String pName) throws InterruptedException {
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

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        return process(name, "childNodeAdded", EmptyNodeState.EMPTY_NODE, after);
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        try {
            return process(name, "childNodeChanged", before, after);
        } catch (RuntimeException e) {
            log.trace("Check binaries for node {} and retry to process childNodeChanged", name);
            // Attempt to load the binaries and retry, see OAK-4969
            for (PropertyState propertyState : after.getProperties()) {
                binaryCheck(propertyState);
            }
            return process(name, "childNodeChanged", before, after);
        }
    }

    private boolean process(String name, String op, NodeState before, NodeState after) {
        if (stop()) {
            return false;
        }

        if (after instanceof SegmentNodeState) {
            if (log.isTraceEnabled()) {
                log.trace("{} {}, readonly binary check {}", op, path + name, logOnly);
            }

            if (!logOnly) {
                RecordId id = ((SegmentNodeState) after).getRecordId();
                builder.setChildNode(name, store.getReader().readNode(id));
            }

            if ("checkpoints".equals(name)) {
                // if we're on the /checkpoints path, there's no need for a deep
                // traversal to verify binaries
                return true;
            }

            if (!hasDataStore) {
                return true;

            }
            // has external datastore, we need a deep
            // traversal to verify binaries
            return after.compareAgainstBaseState(before, new StandbyDiff(builder.getChildNode(name), store, client, path + name + "/", true, running));
        }

        return false;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        log.trace("childNodeDeleted {}, RO:{}", path + name, logOnly);

        if (!logOnly) {
            builder.getChildNode(name).remove();
        }

        return true;
    }

}
