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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class EventIndex {

    private static final Logger LOG = LoggerFactory.getLogger(EventIndex.class);
    
    // TODO need to be hidden (":index" or ":data")
    static final String DATA = "indexData";
    private static final String NEXT_ID = "nextId";

    private final NodeBuilder definition;
    private final IndexUpdateCallback callback;
    private final int eventTypeBitMask;
    private final List<String> nodeTypes;
    private final List<String> paths;
    private final List<String> properties;
    private final String listenerClass;
    
    private final ArrayList<EventData> events = new ArrayList<>();

    EventIndex(NodeBuilder definition, IndexUpdateCallback callback) {
        this.definition = definition;
        this.callback = callback;
        this.eventTypeBitMask = getEventTypeMask(definition.getProperty("eventTypes"));
        this.nodeTypes = getStringList(definition.getProperty("nodeTypes"));
        this.paths = getStringList(definition.getProperty("paths"));
        this.properties = getStringList(definition.getProperty("properties"));
        this.listenerClass = definition.getString("listenerClass");
    }
    
    static List<String> getStringList(PropertyState p) {
        if (p == null) {
            return Collections.emptyList();
        }
        if (p.isArray()) {
            return Lists.newArrayList(p.getValue(Type.STRINGS));
        }
        return Collections.singletonList(p.getValue(Type.STRING));
    }
    
    static int getEventTypeMask(PropertyState p) {
        if (p == null) {
            return EventType.ADDED.bitMask 
                    | EventType.CHANGED.bitMask 
                    | EventType.REMOVED.bitMask
                    | EventType.MOVED.bitMask;
        }
        if (p.isArray()) {
            int mask = 0;
            for (String v : p.getValue(Type.STRINGS)) {
                mask |= getEventTypeBitMask(v);
            }
            return mask;
        }
        return getEventTypeBitMask(p.getValue(Type.STRING));
    }

    private static int getEventTypeBitMask(String value) {
        return EventType.valueOf(value).bitMask;
    }

    public IndexUpdateCallback getCallback() {
        return callback;
    }

    public List<EventData> getEvents() {
        return events;
    }

    public NodeBuilder getIndexDataBuilder() {
        return definition.child(DATA);
    }

    public long newEventId() {
        NodeBuilder b = getIndexDataBuilder();
        if (!b.hasProperty(NEXT_ID)) {
            return 0;
        }
        return b.getProperty(NEXT_ID).getValue(Type.LONG);
    }
    
    public void setNextEventId(long id) {
        getIndexDataBuilder().setProperty(NEXT_ID, id);
    }

    public boolean isEventType(EventType type) {
        return (eventTypeBitMask & type.bitMask) != 0;
    }

    public boolean matchPath(String path) {
        if (paths.isEmpty()) {
            return true;
        }
        for (String p : paths) {
            if (p.equals(path) || PathUtils.isAncestor(p, path)) {
                return true;
            }
        }
        return false;
    }

    public boolean matchNodeType(NodeState node) {
        if (nodeTypes.isEmpty()) {
            return true;
        }
        String nt = node.getName(JcrConstants.JCR_PRIMARYTYPE);
        for (String n : nodeTypes) {
            // TODO support node type hierarchy, not just node type equality
            if (n.equals(nt)) {
                return true;
            }
        }
        return false;
    }
    
    public void setEventProperties(boolean before, NodeState node, EventData target) {
        if (properties.isEmpty()) {
            return;
        }
        for (String pn : properties) {
            PropertyState ps = getPropertyOrNull(node, pn);
            if (ps != null) {
                target.addProperty(before, pn, ps);
            }
        }
    }
    
    static PropertyState getPropertyOrNull(NodeState node, String property) {
        int depth = PathUtils.getDepth(property);
        if (depth > 1) {
            Iterable<String> list = PathUtils.elements(property);
            for(String e : list) {
                if (depth == 1) {
                    break;
                }
                node = node.getChildNode(e);
                if (!node.exists()) {
                    return null;
                }
            }
            property = PathUtils.getName(property);
        }
        if (!node.hasProperty(property)) {
            return null;
        }
        return node.getProperty(property);
    }

}
