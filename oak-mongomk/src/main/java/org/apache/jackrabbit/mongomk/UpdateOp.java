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
package org.apache.jackrabbit.mongomk;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * A MongoDB "update" operation for one node.
 */
public class UpdateOp {
    
    /**
     * The node id, which contains the depth of the path
     * (0 for root, 1 for children of the root), and then the path.
     */
    static final String ID = "_id";
    
    /**
     * The last revision. Key: machine id, value: revision.
     */
    static final String LAST_REV = "_lastRev";
    
    /**
     * The list of recent revisions for this node, where this node is the
     * root of the commit. Key: revision, value: true or the base revision of an
     * un-merged branch commit.
     */
    static final String REVISIONS = "_revisions";

    /**
     * The list of revision to root commit depth mappings to find out if a
     * revision is actually committed.
     */
    static final String COMMIT_ROOT = "_commitRoot";

    /**
     * The number of previous documents (documents that contain old revisions of
     * this node). This property is only set if multiple documents per node
     * exist. This is the case when a node is updated very often in a short
     * time, such that the document gets very big.
     */
    static final String PREVIOUS = "_prev";
    
    /**
     * Whether this node is deleted. Key: revision, value: true/false.
     */
    static final String DELETED = "_deleted";

    /**
     * Revision collision markers set by commits with modifications, which
     * overlap with un-merged branch commits.
     * Key: revision, value:
     */
    static final String COLLISIONS = "_collisions";

    /**
     * The modified time (5 second resolution).
     */
    static final String MODIFIED = "_modified";
    
    final String path;
    
    final String key;
    
    final boolean isNew;
    boolean isDelete;
    
    final Map<String, Operation> changes = new TreeMap<String, Operation>();
    
    /**
     * Create an update operation for the given document. The commit root is assumed
     * to be the path, unless this is changed later on.
     * 
     * @param path the node path (for nodes)
     * @param key the primary key
     * @param isNew whether this is a new document
     */
    UpdateOp(String path, String key, boolean isNew) {
        this.path = path;
        this.key = key;
        this.isNew = isNew;
    }
    
    String getPath() {
        return path;
    }
    
    String getKey() {
        return key;
    }
    
    boolean isNew() {
        return isNew;
    }
    
    void setDelete(boolean isDelete) {
        this.isDelete = isDelete;
    }
    
    /**
     * Add a new or update an existing map entry.
     * The property is a map of sub-names / values.
     * 
     * @param property the property
     * @param subName the entry name
     * @param value the value
     */
    void setMapEntry(String property, String subName, Object value) {
        Operation op = new Operation();
        op.type = Operation.Type.SET_MAP_ENTRY;
        op.value = value;
        changes.put(property + "." + subName, op);
    }
    
    /**
     * Remove a map entry.
     * The property is a map of sub-names / values.
     * 
     * @param property the property
     * @param subName the entry name
     */
    public void removeMapEntry(String property, String subName) {
        Operation op = new Operation();
        op.type = Operation.Type.REMOVE_MAP_ENTRY;
        changes.put(property + "." + subName, op);
    }
    
    /**
     * Set a map to a single key-value pair.
     * The property is a map of sub-names / values.
     * 
     * @param property the property
     * @param subName the entry name
     * @param value the value
     */
    public void setMap(String property, String subName, Object value) {
        Operation op = new Operation();
        op.type = Operation.Type.SET_MAP;
        op.value = value;
        changes.put(property + "." + subName, op);
    }

    /**
     * Set the property to the given value.
     * 
     * @param property the property name
     * @param value the value
     */
    void set(String property, Object value) {
        Operation op = new Operation();
        op.type = Operation.Type.SET;
        op.value = value;
        changes.put(property, op);
    }
    
    /**
     * Do not set the property (after it has been set).
     * 
     * @param property the property name
     */
    void unset(String property) {
        changes.remove(property);
    }
    
    /**
     * Do not set the property entry (after it has been set).
     * The property is a map of sub-names / values.
     * 
     * @param property the property name
     * @param subName the entry name
     */
    void unsetMapEntry(String property, String subName) {
        changes.remove(property + "." + subName);
    }

    /**
     * Checks if the named key exists or is absent in the MongoDB document. This
     * method can be used to make a conditional update.
     *
     * @param property the property name
     * @param subName the entry name
     */
    void containsMapEntry(String property, String subName, boolean exists) {
        if (isNew) {
            throw new IllegalStateException("Cannot use containsMapEntry() on new document");
        }
        Operation op = new Operation();
        op.type = Operation.Type.CONTAINS_MAP_ENTRY;
        op.value = exists;
        changes.put(property + "." + subName, op);
    }

    /**
     * Increment the value.
     * 
     * @param property the key
     * @param value the increment
     */
    void increment(String property, long value) {
        Operation op = new Operation();
        op.type = Operation.Type.INCREMENT;
        op.value = value;
        changes.put(property, op);
    }
    
    public Long getIncrement(String property) {
        Operation op = changes.get(property);
        if (op == null) {
            return null;
        }
        if (op.type != Operation.Type.INCREMENT) {
            throw new IllegalArgumentException("Not an increment operation");
        }
        return (Long) op.value;
    }
    
    public UpdateOp getReverseOperation() {
        UpdateOp reverse = new UpdateOp(path, key, isNew);
        for (Entry<String, Operation> e : changes.entrySet()) {
            Operation r = e.getValue().getReverse();
            if (r != null) {
                reverse.changes.put(e.getKey(), r);
            }
        }        
        return reverse;
    }

    public String toString() {
        return "key: " + key + " " + (isNew ? "new" : "update") + " " + changes;
    }
    
    /**
     * A MongoDB operation for a given key within a document. 
     */
    static class Operation {
        
        /**
         * The MongoDB operation type.
         */
        enum Type { 
            
            /**
             * Set the value. 
             * The sub-key is not used.
             */
            SET,
            
            /**
             * Increment the Long value with the provided Long value.
             * The sub-key is not used.
             */
            INCREMENT, 
            
            /**
             * Add the sub-key / value pair.
             * The value in the stored node is a map.
             */ 
            SET_MAP_ENTRY,
             
            /**
             * Remove the sub-key / value pair.
             * The value in the stored node is a map.
             */
            REMOVE_MAP_ENTRY,

            /**
             * Checks if the sub-key is present in a map or not.
             */
            CONTAINS_MAP_ENTRY,
             
            /**
             * Set the sub-key / value pair.
             * The value in the stored node is a map.
             */
            SET_MAP,

         }
             
        
        /**
         * The operation type.
         */
        Type type;
        
        /**
         * The value, if any.
         */
        Object value;
        
        public String toString() {
            return type + " " + value;
        }

        public Operation getReverse() {
            Operation reverse = null;
            switch (type) {
            case INCREMENT:
                reverse = new Operation();
                reverse.type = Type.INCREMENT;
                reverse.value = -(Long) value;
                break;
            case SET:
            case REMOVE_MAP_ENTRY:
            case SET_MAP:
                // nothing to do
                break;
            case SET_MAP_ENTRY:
                reverse = new Operation();
                reverse.type = Type.REMOVE_MAP_ENTRY;
                break;
            }
            return reverse;
        }
        
    }

}
