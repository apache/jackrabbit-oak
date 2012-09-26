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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.jackrabbit.oak.commons.PathUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * The {@code MongoDB} representation of a node.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class NodeMongo extends BasicDBObject {

    public static final String KEY_BASE_REVISION_ID = "baseRevId";
    public static final String KEY_CHILDREN = "kids";
    public static final String KEY_PATH = "path";
    public static final String KEY_PROPERTIES = "props";
    public static final String KEY_REVISION_ID = "revId";
    private static final long serialVersionUID = 3153393934945155106L;

    public static NodeMongo fromDBObject(DBObject node) {
        NodeMongo nodeMongo = new NodeMongo();
        nodeMongo.putAll(node);

        return nodeMongo;
    }

    public static NodeMongo fromNode(Node node) {
        NodeMongo nodeMongo = new NodeMongo();

        String path = node.getPath();
        nodeMongo.setPath(path);

        String revisionId = node.getRevisionId();
        if (revisionId != null) {
            nodeMongo.setRevisionId(revisionId);
        }

        Map<String, Object> properties = node.getProperties();
        if (properties != null) {
            nodeMongo.setProperties(properties);
        }

        Set<Node> children = node.getChildren();
        if (children != null) {
            List<String> childNames = new LinkedList<String>();
            for (Node child : children) {
                childNames.add(child.getName());
            }
            nodeMongo.setChildren(childNames);
        }

        return nodeMongo;
    }

    public static Set<NodeMongo> fromNodes(Collection<Node> nodes) {
        Set<NodeMongo> nodeMongos = new HashSet<NodeMongo>(nodes.size());
        for (Node node : nodes) {
            NodeMongo nodeMongo = NodeMongo.fromNode(node);
            nodeMongos.add(nodeMongo);
        }

        return nodeMongos;
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
        String revisionId = String.valueOf(nodeMongo.getRevisionId());
        String path = nodeMongo.getPath();
        List<String> childNames = nodeMongo.getChildren();
        long childCount = childNames != null ? childNames.size() : 0;
        Map<String, Object> properties = nodeMongo.getProperties();
        Set<Node> children = null;
        if (childNames != null) {
            children = new HashSet<Node>();
            for (String childName : childNames) {
                NodeImpl child = new NodeImpl();
                child.setPath(PathUtils.concat(path, childName));
                children.add(child);
            }
        }

        NodeImpl nodeImpl = new NodeImpl();
        nodeImpl.setPath(path);
        nodeImpl.setChildCount(childCount);
        nodeImpl.setRevisionId(revisionId);
        nodeImpl.setProperties(properties);
        nodeImpl.setChildren(children);

        return nodeImpl;
    }

    private List<String> addedChildren;
    private Map<String, Object> addedProps;
    private List<String> removedChildren;
    private Map<String, Object> removedProps;

    public void addChild(String childName) {
        if (addedChildren == null) {
            addedChildren = new LinkedList<String>();
        }

        addedChildren.add(childName);
    }

    public void addProperty(String key, Object value) {
        if (addedProps == null) {
            addedProps = new HashMap<String, Object>();
        }

        addedProps.put(key, value);
    }

    public List<String> getAddedChildren() {
        return addedChildren;
    }

    public Map<String, Object> getAddedProps() {
        return addedProps;
    }

    @SuppressWarnings("unchecked")
    public List<String> getChildren() {
        return (List<String>) this.get(KEY_CHILDREN);
    }

    public boolean childExists(String childName) {
        List<String> children = getChildren();
        if (children != null && !children.isEmpty()) {
            if (children.contains(childName)) {
                return true;
            }
        }
        return addedChildExists(childName);
    }

    private boolean addedChildExists(String childName) {
        return addedChildren != null && !addedChildren.isEmpty()?
                addedChildren.contains(childName) : false;
    }

    public String getName() {
        return PathUtils.getName(getString(KEY_PATH));
    }

    public String getPath() {
        return getString(KEY_PATH);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getProperties() {
        return (Map<String, Object>) this.get(KEY_PROPERTIES);
    }

    public List<String> getRemovedChildren() {
        return removedChildren;
    }

    public Map<String, Object> getRemovedProps() {
        return removedProps;
    }

    public Long getRevisionId() {
        return getLong(KEY_REVISION_ID);
    }

    public void removeChild(String childName) {
        if (removedChildren == null) {
            removedChildren = new LinkedList<String>();
        }

        removedChildren.add(childName);
    }

    public void removeProp(String key) {
        if (removedProps == null) {
            removedProps = new HashMap<String, Object>();
        }

        removedProps.put(key, null);
    }

    public void setBaseRevisionId(long baseRevisionId) {
        put(KEY_BASE_REVISION_ID, baseRevisionId);
    }

    public void setChildren(List<String> children) {
        if (children != null) {
            put(KEY_CHILDREN, children);
        } else {
            removeField(KEY_CHILDREN);
        }
    }

    public void setPath(String path) {
        put(KEY_PATH, path);
    }

    public void setProperties(Map<String, Object> properties) {
        if (properties != null) {
            put(KEY_PROPERTIES, properties);
        } else {
            removeField(KEY_PROPERTIES);
        }
    }

    public void setRevisionId(long revisionId) {
        put(KEY_REVISION_ID, revisionId);
    }

    public void setRevisionId(String revisionId) {
        this.setRevisionId(MongoUtil.toMongoRepresentation(revisionId));
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
