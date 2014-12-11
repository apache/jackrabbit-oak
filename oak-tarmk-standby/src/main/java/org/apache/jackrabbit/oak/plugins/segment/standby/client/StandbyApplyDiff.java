package org.apache.jackrabbit.oak.plugins.segment.standby.client;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;

import java.io.IOException;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
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

    public StandbyApplyDiff(NodeBuilder builder, SegmentStore store,
            RemoteSegmentLoader loader) {
        this(builder, store, loader, "/");
    }

    private StandbyApplyDiff(NodeBuilder builder, SegmentStore store,
            RemoteSegmentLoader loader, String path) {
        this.builder = builder;
        this.store = store;
        this.loader = loader;
        this.path = path;
        if (log.isTraceEnabled()) {
            if (PathUtils.getDepth(path) < 5) {
                log.trace("running diff on {}", path);
            }
        }
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        if (!loader.isRunning()) {
            return false;
        }
        builder.setProperty(binaryCheck(after));
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        if (!loader.isRunning()) {
            return false;
        }
        builder.setProperty(binaryCheck(after));
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        if (!loader.isRunning()) {
            return false;
        }
        builder.removeProperty(before.getName());
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
        NodeBuilder child = EmptyNodeState.EMPTY_NODE.builder();
        boolean success = EmptyNodeState.compareAgainstEmptyState(after,
                new StandbyApplyDiff(child, store, loader, path + name + "/"));
        if (success) {
            builder.setChildNode(name, child.getNodeState());
        }
        return success;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before,
            NodeState after) {
        if (!loader.isRunning()) {
            return false;
        }

        return after.compareAgainstBaseState(before, new StandbyApplyDiff(
                builder.getChildNode(name), store, loader, path + name + "/"));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        if (!loader.isRunning()) {
            return false;
        }
        builder.getChildNode(name).remove();
        return true;
    }
}
