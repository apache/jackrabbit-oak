/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mongomk.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.oak.commons.PathUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * The {@code MongoDB} representation of a node.
 */
public class NodeMongo extends BasicDBObject {

    public static final String KEY_BASE_REVISION_ID = "baseRevId";
    public static final String KEY_CHILDREN = "children";
    public static final String KEY_PATH = "path";
    public static final String KEY_PROPERTIES = "props";
    public static final String KEY_REVISION_ID = "revId";

    private static final long serialVersionUID = 3153393934945155106L;

    private List<String> addedChildren;
    private Map<String, Object> addedProps;
    private List<String> removedChildren;
    private Map<String, Object> removedProps;

    public static NodeMongo createClone(NodeMongo node) {
        NodeMongo nodeMongo = new NodeMongo();
        nodeMongo.putAll((DBObject)node);
        return nodeMongo;
    }

    public static List<Node> toNode(Collection<NodeMongo> nodeMongos) {
        List<Node> nodes = new ArrayList<Node>(nodeMongos.size());
        for (NodeMongo nodeMongo : nodeMongos) {
            Node node = NodeMongo.toNode(nodeMongo);
            nodes.add(node);
        }

        return nodes;
    }

    public static NodeImpl toNode(NodeMongo nodeMongo) {
        String path = nodeMongo.getPath();
        NodeImpl nodeImpl = new NodeImpl(path);

        List<String> childNames = nodeMongo.getChildren();
        if (childNames != null) {
            for (String childName : childNames) {
                String childPath = PathUtils.concat(path, childName);
                NodeImpl child = new NodeImpl(childPath);
                nodeImpl.addChild(child);
            }
        }

        nodeImpl.setRevisionId(nodeMongo.getRevisionId());
        nodeImpl.setProperties(nodeMongo.getProperties());
        return nodeImpl;
    }

    /**
     * These properties are persisted to MongoDB.
     */

    public void setBaseRevisionId(long baseRevisionId) {
        put(KEY_BASE_REVISION_ID, baseRevisionId);
    }

    @SuppressWarnings("unchecked")
    public List<String> getChildren() {
        return (List<String>)get(KEY_CHILDREN);
    }

    public void setChildren(List<String> children) {
        if (children != null) {
            put(KEY_CHILDREN, children);
        } else {
            removeField(KEY_CHILDREN);
        }
    }

    public String getPath() {
        return getString(KEY_PATH);
    }

    public void setPath(String path) {
        put(KEY_PATH, path);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getProperties() {
        return (Map<String, Object>) this.get(KEY_PROPERTIES);
    }

    public void setProperties(Map<String, Object> properties) {
        if (properties != null) {
            put(KEY_PROPERTIES, properties);
        } else {
            removeField(KEY_PROPERTIES);
        }
    }

    public Long getRevisionId() {
        return getLong(KEY_REVISION_ID);
    }

    public void setRevisionId(long revisionId) {
        put(KEY_REVISION_ID, revisionId);
    }

    /**
     * These properties are used to keep track of changes but not persisted to MongoDB.
     */

    public void addChild(String childName) {
        if (addedChildren == null) {
            addedChildren = new LinkedList<String>();
        }
        addedChildren.add(childName);
    }

    public List<String> getAddedChildren() {
        return addedChildren;
    }

    public void removeChild(String childName) {
        if (removedChildren == null) {
            removedChildren = new LinkedList<String>();
        }
        removedChildren.add(childName);
    }

    public List<String> getRemovedChildren() {
        return removedChildren;
    }

    public void addProperty(String key, Object value) {
        if (addedProps == null) {
            addedProps = new HashMap<String, Object>();
        }
        addedProps.put(key, value);
    }

    public Map<String, Object> getAddedProps() {
        return addedProps;
    }

    public void removeProp(String key) {
        if (removedProps == null) {
            removedProps = new HashMap<String, Object>();
        }
        removedProps.put(key, null);
    }

    public Map<String, Object> getRemovedProps() {
        return removedProps;
    }

    /**
     * Other methods
     * @param childName
     * @return
     */
    public boolean childExists(String childName) {
        List<String> children = getChildren();
        if (children != null && !children.isEmpty()) {
            if (children.contains(childName)) {
                return true;
            }
        }
        return childExistsInAddedChildren(childName);
    }

    private boolean childExistsInAddedChildren(String childName) {
        return addedChildren != null && !addedChildren.isEmpty()?
                addedChildren.contains(childName) : false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.deleteCharAt(sb.length() - 1);
        sb.append(", addedChildren : ");
        sb.append(addedChildren);
        sb.append(", removedChildren : ");
        sb.append(removedChildren);
        sb.append(", addedProps : ");
        sb.append(addedProps);
        sb.append(", removedProps : ");
        sb.append(removedProps);
        sb.append(" }");
        return sb.toString();
    }
}
