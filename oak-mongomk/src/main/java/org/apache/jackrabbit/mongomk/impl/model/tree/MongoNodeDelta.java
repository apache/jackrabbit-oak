/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mongomk.impl.model.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.model.tree.NodeStateDiff;
import org.apache.jackrabbit.mk.model.tree.NodeStore;
import org.apache.jackrabbit.mk.model.tree.PropertyState;

/**
 * Note: Most of this functionality is mirrored from NodeDelta with the hopes
 * that the two functionality can be consolidated at some point.
 */
public class MongoNodeDelta {

    public static enum ConflictType {
        /**
         * same property has been added or set, but with differing values
         */
        PROPERTY_VALUE_CONFLICT,
        /**
         * child nodes with identical name have been added or modified, but
         * with differing id's; the corresponding node subtrees are hence differing
         * and potentially conflicting.
         */
        NODE_CONTENT_CONFLICT,
        /**
         * a modified property has been deleted
         */
        REMOVED_DIRTY_PROPERTY_CONFLICT,
        /**
         * a child node entry pointing to a modified subtree has been deleted
         */
        REMOVED_DIRTY_NODE_CONFLICT
    }

    private final NodeStore provider;

    private final NodeState node1;

    Map<String, String> addedProperties = new HashMap<String, String>();
    Map<String, String> removedProperties = new HashMap<String, String>();
    Map<String, String> changedProperties = new HashMap<String, String>();

    Map<String, NodeState> addedChildNodes = new HashMap<String, NodeState>();
    Map<String, Id> removedChildNodes = new HashMap<String, Id>();
    Map<String, NodeState> changedChildNodes = new HashMap<String, NodeState>();

    public MongoNodeDelta(NodeStore provider, NodeState node1, NodeState node2) {
        this.provider = provider;
        this.node1 = node1;
        this.provider.compare(node1, node2, new DiffHandler());
    }

    public Map<String, String> getAddedProperties() {
        return addedProperties;
    }

    public Map<String, String> getRemovedProperties() {
        return removedProperties;
    }

    public Map<String, String> getChangedProperties() {
        return changedProperties;
    }

    public Map<String, NodeState> getAddedChildNodes() {
        return addedChildNodes;
    }

    public Map<String, Id> getRemovedChildNodes() {
        return removedChildNodes;
    }

    public Map<String, NodeState> getChangedChildNodes() {
        return changedChildNodes;
    }

    public boolean conflictsWith(MongoNodeDelta other) {
        return !listConflicts(other).isEmpty();
    }

    public List<Conflict> listConflicts(MongoNodeDelta other) {
        // assume that both delta's were built using the *same* base node revision
        if (!node1.equals(other.node1)) {
            throw new IllegalArgumentException("other and this NodeDelta object are expected to share common node1 instance");
        }

        List<Conflict> conflicts = new ArrayList<Conflict>();

        // properties

        Map<String, String> otherAddedProps = other.getAddedProperties();
        for (Map.Entry<String, String> added : addedProperties.entrySet()) {
            String otherValue = otherAddedProps.get(added.getKey());
            if (otherValue != null && !added.getValue().equals(otherValue)) {
                // same property added with conflicting values
                conflicts.add(new Conflict(ConflictType.PROPERTY_VALUE_CONFLICT, added.getKey()));
            }
        }

        Map<String, String> otherChangedProps = other.getChangedProperties();
        Map<String, String> otherRemovedProps = other.getRemovedProperties();
        for (Map.Entry<String, String> changed : changedProperties.entrySet()) {
            String otherValue = otherChangedProps.get(changed.getKey());
            if (otherValue != null && !changed.getValue().equals(otherValue)) {
                // same property changed with conflicting values
                conflicts.add(new Conflict(ConflictType.PROPERTY_VALUE_CONFLICT, changed.getKey()));
            }
            if (otherRemovedProps.containsKey(changed.getKey())) {
                // changed property has been removed
                conflicts.add(new Conflict(ConflictType.REMOVED_DIRTY_PROPERTY_CONFLICT, changed.getKey()));
            }
        }

        for (Map.Entry<String, String> removed : removedProperties.entrySet()) {
            if (otherChangedProps.containsKey(removed.getKey())) {
                // removed property has been changed
                conflicts.add(new Conflict(ConflictType.REMOVED_DIRTY_PROPERTY_CONFLICT, removed.getKey()));
            }
        }

        // child node entries

        //Map<String, Id> otherAddedChildNodes = other.getAddedChildNodes();
        Map<String, NodeState> otherAddedChildNodes = other.getAddedChildNodes();
        for (Map.Entry<String, NodeState> added : addedChildNodes.entrySet()) {
            NodeState otherValue = otherAddedChildNodes.get(added.getKey());
            if (otherValue != null && !added.getValue().equals(otherValue)) {
                // same child node entry added with different target id's
                conflicts.add(new Conflict(ConflictType.NODE_CONTENT_CONFLICT, added.getKey()));
            }
        }

        //Map<String, Id> otherChangedChildNodes = other.getChangedChildNodes();
        Map<String, NodeState> otherChangedChildNodes = other.getChangedChildNodes();
        Map<String, Id> otherRemovedChildNodes = other.getRemovedChildNodes();
        for (Map.Entry<String, NodeState> changed : changedChildNodes.entrySet()) {
            NodeState otherValue = otherChangedChildNodes.get(changed.getKey());
            if (otherValue != null && !changed.getValue().equals(otherValue)) {
                // same child node entry changed with different target id's
                conflicts.add(new Conflict(ConflictType.NODE_CONTENT_CONFLICT, changed.getKey()));
            }
            if (otherRemovedChildNodes.containsKey(changed.getKey())) {
                // changed child node entry has been removed
                conflicts.add(new Conflict(ConflictType.REMOVED_DIRTY_NODE_CONFLICT, changed.getKey()));
            }
        }

        for (Map.Entry<String, Id> removed : removedChildNodes.entrySet()) {
            if (otherChangedChildNodes.containsKey(removed.getKey())) {
                // removed child node entry has been changed
                conflicts.add(new Conflict(ConflictType.REMOVED_DIRTY_NODE_CONFLICT, removed.getKey()));
            }
        }

        return conflicts;
    }

    //--------------------------------------------------------< inner classes >

    private class DiffHandler implements NodeStateDiff {

        @Override
        public void propertyAdded(PropertyState after) {
            addedProperties.put(after.getName(), after.getEncodedValue());
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            changedProperties.put(after.getName(), after.getEncodedValue());
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            removedProperties.put(before.getName(), before.getEncodedValue());
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            addedChildNodes.put(name, after);
        }

        @Override
        public void childNodeChanged(
                String name, NodeState before, NodeState after) {
            changedChildNodes.put(name, after /*provider.getId(after)*/);
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            removedChildNodes.put(name, null /*provider.getId(before)*/);
        }
    }

    public static class Conflict {

        final ConflictType type;
        final String name;

        /**
         * @param type conflict type
         * @param name name of conflicting property or child node
         */
        Conflict(ConflictType type, String name) {
            this.type = type;
            this.name = name;
        }

        public ConflictType getType() {
            return type;
        }

        public String getName() {
            return name;
        }
    }
}
