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
package org.apache.jackrabbit.mongomk.prototype;

import java.util.Map;
import java.util.TreeMap;

/**
 * A MongoDB "update" operation for one node.
 */
public class UpdateOp {
    
    static final String ID = "_id";
    static final String PATH = "_path";
    static final String WRITE_COUNT = "_writeCount";
    static final String REVISIONS = "_revisions";
    static final String PREVIOUS = "_prev";
    static final String DELETED = "_deleted";
    
    final String path;
    
    final String key;
    
    final boolean isNew;
    
    final Map<String, Operation> changes = new TreeMap<String, Operation>();
    
    /**
     * Create an update operation for the given document. The commit root is assumed
     * to be the path, unless this is changed later on.
     * 
     * @param path the node path (for nodes)
     * @param key the primary key
     * @param isNew whether this is a new document
     * @param rev the revision
     */
    UpdateOp(String path, String key, boolean isNew) {
        this.path = path;
        this.key = key;
        this.isNew = isNew;
    }
    
    String getPath() {
        return path;
    }
    
    boolean isNew() {
        return isNew;
    }
    
    /**
     * Add a new map entry for this revision.
     * 
     * @param property the property
     * @param value the value
     */
    void addMapEntry(String property, Object value) {
        Operation op = new Operation();
        op.type = Operation.Type.ADD_MAP_ENTRY;
        op.value = value;
        changes.put(property, op);
    }
    
    public void removeMapEntry(String property) {
        Operation op = new Operation();
        op.type = Operation.Type.REMOVE_MAP_ENTRY;
        changes.put(property, op);
    }
    
    public void setMapEntry(String property, Object value) {
        Operation op = new Operation();
        op.type = Operation.Type.SET_MAP_ENTRY;
        op.value = value;
        changes.put(property, op);
    }
    
    /**
     * Set the property.
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
     * Increment the value.
     * 
     * @param key the key
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
             ADD_MAP_ENTRY, 
             
             /**
              * Remove the sub-key / value pair.
              * The value in the stored node is a map.
              */ 
             REMOVE_MAP_ENTRY,
             
             /**
              * Set the sub-key / value pair.
              * The value in the stored node is a map.
              */
             SET_MAP_ENTRY,
             
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
        
    }

}
