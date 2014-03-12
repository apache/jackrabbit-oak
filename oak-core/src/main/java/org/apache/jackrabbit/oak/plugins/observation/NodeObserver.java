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

package org.apache.jackrabbit.oak.plugins.observation;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.namepath.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.observation.filter.VisibleFilter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Base class for {@code Observer} instances that group changes
 * by node instead of tracking them down to individual properties.
 */
public abstract class NodeObserver implements Observer {
    private final String path;

    private NodeState previousRoot;

    /**
     * Create a new instance for observing the given path.
     * @param path
     */
    protected NodeObserver(String path) {
        this.path = path;
    }

    /**
     * A node at {@code path} has been added.
     * @param path       Path of the added node.
     * @param added      Names of the added properties.
     * @param deleted    Names of the deleted properties.
     * @param changed    Names of the changed properties.
     * @param commitInfo commit info associated with this change.
     */
    protected abstract void added(
            @Nonnull String path,
            @Nonnull Set<String> added,
            @Nonnull Set<String> deleted,
            @Nonnull Set<String> changed,
            @Nonnull CommitInfo commitInfo);

    /**
     * A node at {@code path} has been deleted.
     * @param path       Path of the deleted node.
     * @param added      Names of the added properties.
     * @param deleted    Names of the deleted properties.
     * @param changed    Names of the changed properties.
     * @param commitInfo commit info associated with this change.
     */
    protected abstract void deleted(
            @Nonnull String path,
            @Nonnull Set<String> added,
            @Nonnull Set<String> deleted,
            @Nonnull Set<String> changed,
            @Nonnull CommitInfo commitInfo);

    /**
     * A node at {@code path} has been changed.
     * @param path       Path of the changed node.
     * @param added      Names of the added properties.
     * @param deleted    Names of the deleted properties.
     * @param changed    Names of the changed properties.
     * @param commitInfo commit info associated with this change.
     */
    protected abstract void changed(
            @Nonnull String path,
            @Nonnull Set<String> added,
            @Nonnull Set<String> deleted,
            @Nonnull Set<String> changed,
            @Nonnull CommitInfo commitInfo);

    @Override
    public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
        if (previousRoot != null) {
            NamePathMapper namePathMapper = new NamePathMapperImpl(
                    new GlobalNameMapper(new ImmutableRoot(root)));

            NodeState before = previousRoot;
            NodeState after = root;
            EventHandler handler = new FilteredHandler(
                    new VisibleFilter(),
                    new NodeEventHandler("/", info, namePathMapper));
            for (String name : PathUtils.elements(path)) {
                before = before.getChildNode(name);
                after = after.getChildNode(name);
                handler = handler.getChildHandler(name, before, after);
            }

            EventGenerator generator = new EventGenerator(before, after, handler);
            while (!generator.isDone()) {
                generator.generate();
            }
        }

        previousRoot = root;
    }

    private enum EventType {ADDED, DELETED, CHANGED}

    private class NodeEventHandler extends DefaultEventHandler {
        private final String path;
        private final CommitInfo commitInfo;
        private final NamePathMapper namePathMapper;
        private final EventType eventType;
        private final Set<String> added = Sets.newHashSet();
        private final Set<String> deleted = Sets.newHashSet();
        private final Set<String> changed = Sets.newHashSet();

        public NodeEventHandler(String path, CommitInfo commitInfo, NamePathMapper namePathMapper) {
            this.path = path;
            this.commitInfo = commitInfo == null ? CommitInfo.EMPTY : commitInfo;
            this.namePathMapper = namePathMapper;
            this.eventType = EventType.CHANGED;
        }

        private NodeEventHandler(NodeEventHandler parent, String name, EventType eventType) {
            this.path = "/".equals(parent.path) ? '/' + name : parent.path + '/' + name;
            this.commitInfo = parent.commitInfo;
            this.namePathMapper = parent.namePathMapper;
            this.eventType = eventType;
        }

        @Override
        public void leave(NodeState before, NodeState after) {
            switch (eventType) {
                case ADDED:
                    added(namePathMapper.getJcrPath(path), added, deleted, changed, commitInfo);
                    break;
                case DELETED:
                    deleted(namePathMapper.getJcrPath(path), added, deleted, changed, commitInfo);
                    break;
                case CHANGED:
                    if (!added.isEmpty() || ! deleted.isEmpty() || !changed.isEmpty()) {
                        changed(namePathMapper.getJcrPath(path), added, deleted, changed, commitInfo);
                    }
                    break;
            }
        }

        @Override
        public EventHandler getChildHandler(String name, NodeState before, NodeState after) {
            if (!before.exists()) {
                return new NodeEventHandler(this, name, EventType.ADDED);
            } else if (!after.exists()) {
                return new NodeEventHandler(this, name, EventType.DELETED);
            } else {
                return new NodeEventHandler(this, name, EventType.CHANGED);
            }
        }

        @Override
        public void propertyAdded(PropertyState after) {
            added.add(namePathMapper.getJcrName(after.getName()));
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            changed.add(namePathMapper.getJcrName(after.getName()));
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            deleted.add(namePathMapper.getJcrName(before.getName()));
        }

    }
}
