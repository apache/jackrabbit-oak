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

package org.apache.jackrabbit.oak.plugins.observation2;

import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.DATE;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.DEEP;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.NO_LOCAL;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.PATH;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.TYPE;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.USER_DATA;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.USER_ID;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.UUID;

import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
public class EventQueueWriter extends DefaultEditor {
    private static final Logger log = LoggerFactory.getLogger(EventQueueWriter.class);

    private final NodeState parent;
    private final EventRecorder eventRecorder;
    private final String path;

    public EventQueueWriter(EventRecorder eventRecorder, String path, NodeState parent) {
        this.parent = parent;
        this.eventRecorder = eventRecorder;
        this.path = path;
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        eventRecorder.recordEvent(PROPERTY_ADDED, PathUtils.concat(path, after.getName()),
                getIdentifier(), getAssociatedParentTypes());
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        eventRecorder.recordEvent(PROPERTY_CHANGED, PathUtils.concat(path, after.getName()),
                getIdentifier(), getAssociatedParentTypes());
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        eventRecorder.recordEvent(PROPERTY_REMOVED, PathUtils.concat(path, before.getName()),
                getIdentifier(), getAssociatedParentTypes());
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        eventRecorder.recordEvent(NODE_ADDED, PathUtils.concat(path, name),
                getIdentifier(), getAssociatedParentTypes());
        return getEventQueueWriter(name, after);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return getEventQueueWriter(name, after);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        eventRecorder.recordEvent(NODE_REMOVED, PathUtils.concat(path, name),
                getIdentifier(), getAssociatedParentTypes());
        return getEventQueueWriter(name, before);
    }

    private EventQueueWriter getEventQueueWriter(String name, NodeState after) {
        String path = PathUtils.concat(this.path, name);
        return PathUtils.isAncestor('/' + JCR_SYSTEM, path)
            ? null
            : new EventQueueWriter(eventRecorder, path, after);
    }

    private String[] getAssociatedParentTypes() {
        Set<String> types = Sets.newHashSet();

        PropertyState jcrPrimaryType = parent.getProperty(JCR_PRIMARYTYPE);
        if (jcrPrimaryType != null) {
            types.add(jcrPrimaryType.getValue(Type.NAME));
        }

        PropertyState jcrMixinTypes = parent.getProperty(JCR_MIXINTYPES);
        if (jcrMixinTypes != null) {
            Iterables.addAll(types, jcrMixinTypes.getValue(Type.NAMES));
        }

        return types.toArray(new String[types.size()]);
    }

    private String getIdentifier() {
        PropertyState jcrUuid = parent.getProperty(JCR_UUID);
        return jcrUuid == null ? null : jcrUuid.getValue(Type.STRING);
    }

    public static class ListenerSpec {
        private final ReadOnlyNodeTypeManager ntMgr;
        private final String id;
        private final String userId;
        private final String userData;
        private final EventFilter filter;

        public static ListenerSpec create(ReadOnlyNodeTypeManager ntMgr, String name, NodeState nodeState) {
            String userId = getString(nodeState, USER_ID);
            if (userId == null) {
                log.warn("Invalid specification for observation event listener: userId missing");
                return null;
            }

            EventFilter filter = getFilter(nodeState);
            if (filter == null) {
                log.warn("Invalid specification for observation event listener: filter missing");
                return null;
            }

            String userData = getString(nodeState, USER_DATA);
            return new ListenerSpec(ntMgr, name, userId, userData, filter);
        }

        private static EventFilter getFilter(NodeState nodeState) {
            Long types = getLong(nodeState, TYPE);
            if (types == null) {
                return null;
            }

            String path = getString(nodeState, PATH);
            if (path == null) {
                return null;
            }

            Boolean deep = getBoolean(nodeState, DEEP);
            if (deep == null) {
                return null;
            }


            String[] uuids = getStrings(nodeState, UUID);
            String[] nodeTypes = getStrings(nodeState, NODE_TYPES);
            Boolean noLocal = getBoolean(nodeState, NO_LOCAL);

            return new EventFilter((int)(long)types, path, deep, uuids, nodeTypes, noLocal);
        }

        private static String[] getStrings(NodeState nodeState, String name) {
            PropertyState p = nodeState.getProperty(name);
            if (p == null) {
                return null;
            }

            return Iterables.toArray(p.getValue(Type.STRINGS), String.class);
        }

        private static Boolean getBoolean(NodeState node, String name) {
            PropertyState p = node.getProperty(name);
            return p == null ? null : p.getValue(Type.BOOLEAN);
        }

        private static String getString(NodeState node, String name) {
            PropertyState p = node.getProperty(name);
            return p == null ? null : p.getValue(Type.STRING);
        }

        private static Long getLong(NodeState node, String name) {
            PropertyState p = node.getProperty(name);
            return p == null ? null : p.getValue(Type.LONG);
        }

        private ListenerSpec(ReadOnlyNodeTypeManager ntMgr, String id, String userId, String userData,
                EventFilter filter) {
            this.ntMgr = ntMgr;
            this.id = id;
            this.userId = userId;
            this.userData = userData;
            this.filter = filter;
        }

        public boolean matches(int type, String path, String identifier, String[] associatedParentTypes) {
            return filter.include(type, path, associatedParentTypes, ntMgr);
        }

        public void persist(NodeBuilder builder) {
            NodeBuilder b = builder.child(id);
            b.setProperty(USER_ID, userId);
            if (userData != null) {
                b.setProperty(USER_DATA, userData);
            }
        }
    }

    public static class EventRecorder {
        private final NodeBuilder eventQueue;
        private final Iterable<ListenerSpec> listenerSpecs;
        private final long date;

        private long eventId;

        public EventRecorder(NodeBuilder eventQueue, Iterable<ListenerSpec> listenerSpecs) {
            this.eventQueue = eventQueue;
            this.listenerSpecs = listenerSpecs;
            this.date = System.currentTimeMillis();
        }

        private void recordEvent(int type, String path, String identifier, String[] associatedParentTypes) {
            // TODO record access restrictions
            Set<ListenerSpec> receivers = Sets.newHashSet();
            for (ListenerSpec spec : listenerSpecs) {
                if (spec.matches(type, path, identifier, associatedParentTypes)) {
                    receivers.add(spec);
                }
            }

            if (!receivers.isEmpty()) {
                NodeBuilder event = persistEvent(type, path, identifier);
                for (ListenerSpec receiver : receivers) {
                    receiver.persist(event);
                }
            }
        }

        private NodeBuilder persistEvent(int type, String path, String identifier) {
            NodeBuilder event = eventQueue.child(String.valueOf(eventId++));
            event.setProperty(TYPE, type);
            event.setProperty(PATH, path);
            if (identifier != null) {
                event.setProperty(UUID, identifier);
            }
            event.setProperty(DATE, date);
            return event;
        }

    }

}
