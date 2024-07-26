package org.apache.jackrabbit.oak.upgrade;

import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

public class UUIDConflictDetector {

    private final NodeStore sourceStore;
    private final NodeStore targetStore;

    public UUIDConflictDetector(NodeStore sourceStore, NodeStore targetStore) {
        this.sourceStore = sourceStore;
        this.targetStore = targetStore;
    }

    public Map<String, Map.Entry<String, String>> detectConflicts() {
        Map<String, Map.Entry<String, String>> conflictingUUIDs = new HashMap<>();
        checkNodeForConflict(sourceStore.getRoot(), "", conflictingUUIDs);
        return conflictingUUIDs;
    }

    public Map<String, Map.Entry<String, String>> detectConflicts(String subtreePath) {
        Map<String, Map.Entry<String, String>> conflictingUUIDs = new HashMap<>();
        NodeState subtreeNode = getNodeAtPath(sourceStore.getRoot(), subtreePath);
        checkNodeForConflict(subtreeNode, subtreePath, conflictingUUIDs);
        return conflictingUUIDs;
    }

    private NodeState getNodeAtPath(NodeState node, String path) {
        for (String name : path.substring(1).split("/")) {
            node = node.getChildNode(name);
        }
        return node;
    }

    private void checkNodeForConflict(NodeState node, String path, Map<String, Map.Entry<String, String>> conflictingUUIDs) {
        PropertyState uuidProperty = node.getProperty("jcr:uuid");
        if (uuidProperty != null) {
            String uuid = uuidProperty.getValue(Type.STRING);
            String conflictPath = nodeWithUUIDExists(targetStore.getRoot(), path, "", uuid);
            if (conflictPath != null) {
                conflictingUUIDs.put(uuid, new AbstractMap.SimpleEntry<>(path, conflictPath));
            }
        }

        for (ChildNodeEntry child : node.getChildNodeEntries()) {
            checkNodeForConflict(child.getNodeState(), path + "/" + child.getName(), conflictingUUIDs);
        }
    }

    private String nodeWithUUIDExists(NodeState node, String sourceNodePath, String path, String uuid) {
        PropertyState uuidProperty = node.getProperty("jcr:uuid");
        if (uuidProperty != null && uuidProperty.getValue(Type.STRING).equals(uuid) && !StringUtils.equals(sourceNodePath, path)) {
            return path;
        }

        for (ChildNodeEntry child : node.getChildNodeEntries()) {
            String conflictPath = nodeWithUUIDExists(child.getNodeState(), sourceNodePath, path + "/" + child.getName(), uuid);
            if (conflictPath != null) {
                return conflictPath;
            }
        }

        return null;
    }
}