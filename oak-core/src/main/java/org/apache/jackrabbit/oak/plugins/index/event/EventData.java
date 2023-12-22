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
package org.apache.jackrabbit.oak.plugins.index.event;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class EventData {
    
    private final String path;
    private EventType eventType;
    private String moveSource;
    private HashMap<String, PropertyState> oldProperties = new HashMap<>();
    private HashMap<String, PropertyState> newProperties = new HashMap<>();
    
    EventData(String path) {
        this.path = path;
    }

    public void store(NodeBuilder b) {
        b.setProperty("path", path);
        b.setProperty("eventType", eventType.toString());
        if (!oldProperties.isEmpty()) {
            b.setProperty("old", toJson(oldProperties));
        }
        if (!newProperties.isEmpty()) {
            b.setProperty("new", toJson(newProperties));
        }
        if (moveSource != null) {
            b.setProperty("movedFrom", moveSource);
        }
    }
    
    public void setMoveSource(String moveSource) {
        this.moveSource = moveSource;
    }
    
    private static String toJson(HashMap<String, PropertyState> map) {
        JsopBuilder buff = new JsopBuilder();
        buff.object();
        for (Entry<String, PropertyState> e : map.entrySet()) {
            PropertyState ps = e.getValue();
            buff.key(e.getKey());
            if (ps.isArray()) {
                buff.array();
                for(int i=0; i<ps.count(); i++) {
                    String v = ps.getValue(Type.STRING, i);
                    buff.value(v);
                }
                buff.endArray();
            } else {
                String v = ps.getValue(Type.STRING);
                buff.value(v);
            }
        }
        buff.endObject();     
        return buff.toString();
    }

    public void setType(EventType eventType) {
        this.eventType = eventType;
    }
    
    public void addProperty(boolean old, String name, PropertyState ps) {
        if (old) {
            oldProperties.put(name, ps);
        } else {
            newProperties.put(name, ps);
        }
    }

}
