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
package org.apache.jackrabbit.oak.jcr.observation;

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;

import java.util.LinkedList;
import java.util.Map;

import javax.jcr.observation.Event;

import org.apache.jackrabbit.api.observation.JackrabbitEvent;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.observation.handler.ChangeHandler;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

/**
 * Change handler that generates JCR Event instances and places them
 * in an event queue.
 */
class QueueingHandler implements ChangeHandler {

    /**
     * Dummy session identifier used to identify external commits.
     */
    private static final String OAK_EXTERNAL = "oak:external";

    private final QueueingHandler parent;

    private final String name;

    private String path = null; // initialized lazily

    private String identifier = null; // initialized lazily

    private final TypePredicate beforeReferenceable;

    private final TypePredicate afterReferenceable;

    private final LinkedList<Event> queue;

    private final NamePathMapper mapper;

    private final CommitInfo info;

    private final NodeState before;

    private final NodeState after;

    QueueingHandler(
            LinkedList<Event> queue, NamePathMapper mapper, CommitInfo info,
            NodeState before, NodeState after) {
        this.parent = null;
        this.name = null;
        this.path = "/";

        this.beforeReferenceable = new TypePredicate(before, MIX_REFERENCEABLE);
        this.afterReferenceable = new TypePredicate(after, MIX_REFERENCEABLE);

        this.queue = queue;
        this.mapper = mapper;
        if (info != null) {
            this.info = info;
        } else {
            // Generate a dummy CommitInfo object to avoid extra null checks.
            // The current time is used as a rough estimate of the commit time.
            this.info = new CommitInfo(OAK_EXTERNAL, null, null);
        }

        this.before = before;
        this.after = after;
    }

    private QueueingHandler(
            QueueingHandler parent,
            String name, NodeState before, NodeState after) {
        this.parent = parent;
        this.name = name;

        this.beforeReferenceable = parent.beforeReferenceable;
        this.afterReferenceable = parent.afterReferenceable;

        this.queue = parent.queue;
        this.mapper = parent.mapper;
        this.info = parent.info;

        this.before = before;
        this.after = after;
    }

    private String getPath() {
        if (path == null) { // implies parent != null
            path = PathUtils.concat(parent.getPath(), name);
        }
        return path;
    }

    private String getIdentifier() {
        if (identifier == null) { // implies after.exists()
            String uuid = after.getString(JCR_UUID);
            if (uuid != null && afterReferenceable.apply(after)) {
                this.identifier = uuid;
            } else if (!after.exists()) {
                this.identifier = getBeforeIdentifier();
            } else if (parent == null) {
                this.identifier = "/";
            } else {
                return PathUtils.concat(parent.getIdentifier(), name);
            }
        }
        return identifier;
    }

    private String getBeforeIdentifier() {
        String uuid = before.getString(JCR_UUID);
        if (uuid != null && beforeReferenceable.apply(before)) {
            return uuid;
        } else if (parent == null) {
            return "/";
        } else if (parent.identifier != null && !parent.after.exists()) {
            return PathUtils.concat(parent.identifier, name);
        } else {
            return PathUtils.concat(parent.getBeforeIdentifier(), name);
        }
    }

    //-----------------------------------------------------< ChangeHandler >--

    @Override
    public ChangeHandler getChildHandler(
            String name, NodeState before, NodeState after) {
        return new QueueingHandler(this, name, before, after);
    }

    @Override
    public void propertyAdded(PropertyState after) {
        queue.add(new ItemEvent(after.getName()) {
            @Override
            public int getType() {
                return PROPERTY_ADDED;
            }
        });
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        queue.add(new ItemEvent(after.getName()) {
            @Override
            public int getType() {
                return PROPERTY_CHANGED;
            }
        });
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        queue.add(new ItemEvent(before.getName()) {
            @Override
            public int getType() {
                return PROPERTY_REMOVED;
            }
        });
    }

    @Override
    public void nodeAdded(String name, NodeState after) {
        queue.add(new NodeEvent(name, after, afterReferenceable) {
            @Override
            public int getType() {
                return NODE_ADDED;
            }
        });
    }

    @Override
    public void nodeDeleted(String name, NodeState before) {
        queue.add(new NodeEvent(name, before, beforeReferenceable) {
            @Override
            public int getType() {
                return NODE_REMOVED;
            }
        });
    }

    @Override
    public void nodeMoved(
            final String sourcePath, String name, NodeState moved) {
        queue.add(new NodeEvent(name, moved, afterReferenceable) {
            @Override
            public int getType() {
                return NODE_MOVED;
            }
            @Override
            public Map<?, ?> getInfo() {
                return ImmutableMap.of(
                        "srcAbsPath", mapper.getJcrPath(sourcePath),
                        "destAbsPath", getPath());
            }
        });
    }

    @Override
    public void nodeReordered(
            final String destName, final String name, NodeState reordered) {
        queue.add(new NodeEvent(name, reordered, afterReferenceable) {
            @Override
            public int getType() {
                return NODE_MOVED;
            }
            @Override
            public Map<?, ?> getInfo() {
                return ImmutableMap.of(
                        "srcChildRelPath", mapper.getJcrName(name),
                        "destChildRelPath", mapper.getJcrName(destName));
            }
        });
    }

    //-----------------------------------------------------------< private >--

    private abstract class NodeEvent extends ItemEvent {

        private final String uuid;

        NodeEvent(String name, NodeState node, TypePredicate isReferenceable) {
            super(name);
            String uuid = node.getString(JCR_UUID);
            if (uuid != null && isReferenceable.apply(node)) {
                this.uuid = uuid;
            } else {
                this.uuid = null;
            }
        }

        @Override
        public String getIdentifier() {
            if (uuid != null) {
                return uuid;
            } else if (getType() == NODE_REMOVED) {
                return PathUtils.concat(
                        QueueingHandler.this.getBeforeIdentifier(), name);
            } else {
                return PathUtils.concat(
                        QueueingHandler.this.getIdentifier(), name);
            }
        }

    }

    private abstract class ItemEvent implements JackrabbitEvent {

        protected final String name;

        ItemEvent(String name) {
            this.name = name;
        }

        @Override
        public String getPath() {
            return PathUtils.concat(
                    mapper.getJcrPath(QueueingHandler.this.getPath()),
                    mapper.getJcrName(name));
        }

        @Override
        public String getIdentifier() {
            return QueueingHandler.this.getIdentifier();
        }

        @Override
        public Map<?, ?> getInfo() {
            return emptyMap();
        }

        @Override
        public String getUserID() {
            return info.getUserId();
        }

        @Override
        public String getUserData() {
            return info.getMessage();
        }

        @Override
        public long getDate() {
            return info.getDate();
        }

        @Override
        public boolean isExternal() {
            return info.getSessionId() == OAK_EXTERNAL;
        }

        //--------------------------------------------------------< Object >--

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            } else if (object instanceof ItemEvent) {
                ItemEvent that = (ItemEvent) object;
                return getType() == that.getType()
                        && getPath().equals(that.getPath())
                        && getIdentifier().equals(that.getIdentifier())
                        && getInfo().equals(that.getInfo())
                        && Objects.equal(getUserID(), that.getUserID())
                        && Objects.equal(getUserData(), that.getUserData())
                        && getDate() == that.getDate()
                        && isExternal() == that.isExternal();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(
                    getType(), getPath(), getIdentifier(), getInfo(),
                    getUserID(), getUserData(), getDate(), isExternal());
        }

        @Override
        public String toString() {
            return Objects.toStringHelper("Event")
                    .add("type", getType())
                    .add("path", getPath())
                    .add("identifier", getIdentifier())
                    .add("info", getInfo())
                    .add("userID", getUserID())
                    .add("userData", getUserData())
                    .add("date", getDate())
                    .add("external", isExternal())
                    .toString();
        }

    }

}
