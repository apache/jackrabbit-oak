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

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;


import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.Nullable;

public class EventEditor implements Editor {
    
    private final EventIndex index;
    private final EventEditor parent;
    private final String name;
    
    EventEditor(EventIndex index, EventEditor parent, String name) {
        this.index = index;
        this.parent = parent;
        this.name = name;
    }
    
    private String getPath() {
        if (parent == null) {
            return "/";
        } else {
            return PathUtils.concat(parent.getPath(), name);
        }
    }
    
    private String getPath(String name) {
        return PathUtils.concat(getPath(), name);
    }
    
    private static boolean isHidden(String name) {
        if (name.equals(EventIndex.DATA)) {
            // TODO not needed once DATA is hidden
            // prevent indexing changes in the events itself
            return true;
        }
        return NodeStateUtils.isHidden(name);
    }    

    @Override
    @Nullable
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        if (isHidden(name)) {
            return null;
        }
        return new EventEditor(index, this, name);
    }

    @Override
    @Nullable
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        if (isHidden(name)) {
            return null;
        }
        String moveSource = after.getString(":source-path");
        if (moveSource == null) {
            if (index.isEventType(EventType.ADDED) && index.matchNodeType(after)) {
                String path = getPath(name);
                if (index.matchPath(path)) {
                    EventData e = new EventData(path);
                    e.setType(EventType.ADDED);
                    index.setEventProperties(false, after, e);
                    index.getEvents().add(e);
                }
            }
        } else {
            if (index.isEventType(EventType.MOVED) && index.matchNodeType(after)) {
                String path = getPath(name);
                if (index.matchPath(path)) {
                    EventData e = new EventData(path);
                    e.setType(EventType.MOVED);
                    e.setMoveSource(moveSource);
                    index.setEventProperties(false, after, e);
                    index.getEvents().add(e);
                }
            }
        }
        return new EventEditor(index, this, name);
    }

    @Override
    @Nullable
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        if (isHidden(name)) {
            return null;
        }
        if (index.isEventType(EventType.REMOVED) && index.matchNodeType(before)) {
            String path = getPath(name);
            if (index.matchPath(path)) {
                EventData e = new EventData(PathUtils.concat(getPath(), name));
                e.setType(EventType.REMOVED);
                index.setEventProperties(true, before, e);
                index.getEvents().add(e);
            }
        }
        return new EventEditor(index, this, name);
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        if (name == null) {
            return;
        }
        if (before == null || after == null || !before.exists() || !after.exists()) {
            // node added or removed
            return;
        }
        if (isHidden(name)) {
            return;
        }
        String path = getPath();
        if (!index.matchPath(path)) {
            return;
        }
        if (!index.matchNodeType(before) && !index.matchNodeType(after)) {
            return;
        }
        if (!index.isEventType(EventType.CHANGED)) {
            return;
        }
        EventData e = new EventData(path);
        e.setType(EventType.CHANGED);
        index.setEventProperties(true, before, e);
        index.setEventProperties(false, after, e);
        index.getEvents().add(e);
    }
    
    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        if (parent != null) {
            return;
        }
        if (index.getEvents().isEmpty()) {
            return;
        }
        index.getCallback().indexUpdate();
        NodeBuilder builder = index.getIndexDataBuilder();
        setPrimaryType(builder);
        long eventId = 0;
        for (EventData e : index.getEvents()) {
            eventId = index.newEventId();
            NodeBuilder eb = builder.child("" + eventId);
            setPrimaryType(eb);
            e.store(eb);
        }
        index.setNextEventId(eventId + 1);
    }
    
    private static void setPrimaryType(NodeBuilder builder) {
        builder.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);
    }    

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        // already processed in enter
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        // already processed in enter
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        // already processed in enter
    }

}
