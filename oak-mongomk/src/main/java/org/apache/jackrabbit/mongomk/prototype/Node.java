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

import java.util.ArrayList;
import java.util.Map;

import org.apache.jackrabbit.mk.json.JsopWriter;

/**
 * Represents a node held in memory (in the cache for example).
 */
public class Node {

    final String path;
    final Revision rev;
    final Map<String, String> properties = Utils.newMap();
    private long writeCount;
    
    Node(String path, Revision rev) {
        this.path = path;
        this.rev = rev;
    }
    
    void setProperty(String propertyName, String value) {
        if (value == null) {
            properties.remove(propertyName);
        } else {
            properties.put(propertyName, value);
        }
    }
    
    public String getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    public void copyTo(Node newNode) {
        newNode.properties.putAll(properties);
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("path: ").append(path).append('\n');
        buff.append("rev: ").append(rev).append('\n');
        buff.append("writeCount: ").append(writeCount).append('\n');
        buff.append(properties);
        buff.append('\n');
        return buff.toString();
    }
    
    /**
     * Create an add node operation for this node.
     */
    UpdateOp asOperation(boolean isNew) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(path, id, isNew);
        op.set(UpdateOp.ID, id);
        op.addMapEntry(UpdateOp.DELETED + "." + rev.toString(), "false");
        for (String p : properties.keySet()) {
            String key = Utils.escapePropertyName(p);
            op.addMapEntry(key + "." + rev.toString(), properties.get(p));
        }
        return op;
    }

    public String getId() {
        return path + "@" + writeCount;        
    }

    public void append(JsopWriter json, boolean includeId) {
        if (includeId) {
            json.key(":id").value(getId());
        }
        for (String p : properties.keySet()) {
            json.key(p).encodedValue(properties.get(p));
        }
    }
    
    /**
     * A list of children for a node.
     */
    static class Children {
        
        final String path;
        final String id;
        final Revision rev;
        
        final ArrayList<String> children = new ArrayList<String>();
        
        Children(String path, String id, Revision rev) {
            this.path = path;
            this.id = id;
            this.rev = rev;
        }
        
        public String toString() {
            return path + ": " + children.toString();
        }
        
    }

    public void setWriteCount(long writeCount) {
        this.writeCount = writeCount;
    }

}
