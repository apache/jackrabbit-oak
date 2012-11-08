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
package org.apache.jackrabbit.mongomk.impl.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.PathUtils;

import com.mongodb.BasicDBObject;

/**
 * The {@code MongoDB} representation of a node.
 */
public class MongoNode extends BasicDBObject {

    public static final String KEY_BASE_REVISION_ID = "baseRevId";
    public static final String KEY_CHILDREN = "children";
    public static final String KEY_PATH = "path";
    public static final String KEY_PROPERTIES = "props";
    public static final String KEY_REVISION_ID = "revId";
    public static final String KEY_BRANCH_ID = "branchId";

    private static final long serialVersionUID = 3153393934945155106L;

    private List<String> addedChildren;
    private Map<String, Object> addedProps;
    private List<String> removedChildren;
    private Map<String, Object> removedProps;

    public static NodeImpl toNode(MongoNode nodeMongo) {
        String path = nodeMongo.getPath();
        NodeImpl nodeImpl = new NodeImpl(path);

        List<String> childNames = nodeMongo.getChildren();
        if (childNames != null) {
            for (String childName : childNames) {
                String childPath = PathUtils.concat(path, childName);
                NodeImpl child = new NodeImpl(childPath);
                nodeImpl.addChildNodeEntry(child);
            }
        }

        nodeImpl.setRevisionId(nodeMongo.getRevisionId());
        for (Map.Entry<String, Object> entry : nodeMongo.getProperties().entrySet()) {
            nodeImpl.addProperty(entry.getKey(), convertObjectValue(entry.getValue()));
        }
        return nodeImpl;
    }

    private static String convertObjectValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return JsopBuilder.encode(value.toString());
        }
        return value.toString();
    }

    //--------------------------------------------------------------------------
    //
    // These properties are persisted to MongoDB
    //
    //--------------------------------------------------------------------------

    public void setBaseRevisionId(long baseRevisionId) {
        put(KEY_BASE_REVISION_ID, baseRevisionId);
    }

    public String getBranchId() {
        return getString(KEY_BRANCH_ID);
    }

    public void setBranchId(String branchId) {
        put(KEY_BRANCH_ID, branchId);
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
        Object properties = get(KEY_PROPERTIES);
        return properties != null? (Map<String, Object>)properties : new HashMap<String, Object>();
    }

    public void setProperties(Map<String, Object> properties) {
        if (properties != null && !properties.isEmpty()) {
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

    //--------------------------------------------------------------------------
    //
    // These properties are used to keep track of changes but not persisted
    //
    //--------------------------------------------------------------------------

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

    //--------------------------------------------------------------------------
    //
    // Other methods
    //
    //--------------------------------------------------------------------------

    public boolean childExists(String childName) {
        List<String> children = getChildren();
        if (children != null && !children.isEmpty()) {
            if (children.contains(childName) && !childExistsInRemovedChildren(childName)) {
                return true;
            }
        }
        return childExistsInAddedChildren(childName);
    }

    private boolean childExistsInAddedChildren(String childName) {
        return addedChildren != null && !addedChildren.isEmpty()?
                addedChildren.contains(childName) : false;
    }

    private boolean childExistsInRemovedChildren(String childName) {
        return removedChildren != null && !removedChildren.isEmpty()?
                removedChildren.contains(childName) : false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.deleteCharAt(sb.length() - 1);
        if (addedChildren != null && !addedChildren.isEmpty()) {
            sb.append(", addedChildren : ");
            sb.append(addedChildren);
        }
        if (removedChildren != null && !removedChildren.isEmpty()) {
            sb.append(", removedChildren : ");
            sb.append(removedChildren);
        }
        if (addedProps != null && !addedProps.isEmpty()) {
            sb.append(", addedProps : ");
            sb.append(addedProps);
        }
        if (removedProps != null && !removedProps.isEmpty()) {
            sb.append(", removedProps : ");
            sb.append(removedProps);
        }
        sb.append(" }");
        return sb.toString();
    }
}
