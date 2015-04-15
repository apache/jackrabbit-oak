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
package org.apache.jackrabbit.oak.plugins.segment.standby.client;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;

import java.io.IOException;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.standby.store.RemoteSegmentLoader;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StandbyApplyDiff implements NodeStateDiff {

    private static final Logger log = LoggerFactory
            .getLogger(StandbyApplyDiff.class);

    private final NodeBuilder builder;

    private final SegmentStore store;

    private final RemoteSegmentLoader loader;

    private final String path;

    /**
     * read-only traversal of the diff that has 2 properties: one is to log all
     * the content changes, second is to drill down to properly level, so that
     * missing binaries can be sync'ed if needed
     */
    private final boolean logOnly;

    public StandbyApplyDiff(NodeBuilder builder, SegmentStore store,
            RemoteSegmentLoader loader) {
        this(builder, store, loader, "/", false);
    }

    private StandbyApplyDiff(NodeBuilder builder, SegmentStore store,
            RemoteSegmentLoader loader, String path, boolean logOnly) {
        this.builder = builder;
        this.store = store;
        this.loader = loader;
        this.path = path;
        this.logOnly = logOnly;
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        if (!loader.isRunning()) {
            return false;
        }
        if (!logOnly) {
            builder.setProperty(binaryCheck(after));
        } else {
            binaryCheck(after);
        }
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        if (!loader.isRunning()) {
            return false;
        }
        if (!logOnly) {
            builder.setProperty(binaryCheck(after));
        } else {
            binaryCheck(after);
        }
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        if (!loader.isRunning()) {
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
            SegmentBlob sb = (SegmentBlob) b;
            // verify if the blob exists
            if (sb.isExternal() && b.getReference() == null) {
                String blobId = sb.getBlobId();
                if (blobId != null) {
                    readBlob(blobId, pName);
                }
            }
        } else {
            log.warn("Unknown Blob {} at {}, ignoring", b.getClass().getName(),
                    path + "#" + pName);
        }
    }

    private void readBlob(String blobId, String pName) {
        Blob read = loader.readBlob(blobId);
        if (read != null) {
            try {
                store.getBlobStore().writeBlob(read.getNewStream());
            } catch (IOException f) {
                throw new IllegalStateException("Unable to persist blob "
                        + blobId + " at " + path + "#" + pName, f);
            }
        } else {
            throw new IllegalStateException("Unable to load remote blob "
                    + blobId + " at " + path + "#" + pName);
        }
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        if (!loader.isRunning()) {
            return false;
        }

        if (after instanceof SegmentNodeState) {
            if (log.isTraceEnabled()) {
                log.trace("childNodeAdded {}, RO:{}", path + name, logOnly);
            }
            if (!logOnly) {
                RecordId id = ((SegmentNodeState) after).getRecordId();
                builder.setChildNode(name, new SegmentNodeState(id));
            }
            return after.compareAgainstBaseState(EmptyNodeState.EMPTY_NODE,
                    new StandbyApplyDiff(builder.getChildNode(name), store,
                            loader, path + name + "/", true));
        }
        return false;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before,
            NodeState after) {
        if (!loader.isRunning()) {
            return false;
        }

        if (after instanceof SegmentNodeState) {
            RecordId id = ((SegmentNodeState) after).getRecordId();

            if (log.isTraceEnabled()) {
                RecordId oldId = ((SegmentNodeState) before).getRecordId();
                log.trace("childNodeChanged {}, {} -> {}, RO:{}", path + name,
                        oldId, id, logOnly);
            }
            if (!logOnly) {
                builder.setChildNode(name, new SegmentNodeState(id));
            }

            return after.compareAgainstBaseState(before, new StandbyApplyDiff(
                    builder.getChildNode(name), store, loader, path + name
                            + "/", true));
        }
        return false;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        if (!loader.isRunning()) {
            return false;
        }
        log.trace("childNodeDeleted {}, RO:{}", path + name, logOnly);
        if (!logOnly) {
            builder.getChildNode(name).remove();
        }
        return true;
    }
}
