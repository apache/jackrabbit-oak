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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventListener;

import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventIterator;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code ChangeProcessor} generates observation {@link javax.jcr.observation.Event}s
 * based on a {@link JcrFilterProvider filter} and delivers them to an {@link EventListener}.
 * <p>
 * After instantiation a {@code ChangeProcessor} must be started in order to start
 * delivering observation events and stopped to stop doing so.
 */
public class ChangeProcessor implements Observer {
    private static final Logger log = LoggerFactory.getLogger(ChangeProcessor.class);

    private final ContentSession contentSession;
    private final PermissionProvider permissionProvider;
    private final NamePathMapper namePathMapper;
    private final ListenerTracker tracker;
    private final EventListener eventListener;
    private final AtomicReference<JcrFilterProvider> filterProvider;

    private Closeable observer;
    private Registration mbean;
    private NodeState previousRoot;

    public ChangeProcessor(
            ContentSession contentSession,
            PermissionProvider permissionProvider,
            NamePathMapper namePathMapper,
            ListenerTracker tracker, JcrFilterProvider filter) {
        checkArgument(contentSession instanceof Observable);
        this.contentSession = contentSession;
        this.permissionProvider = permissionProvider;
        this.namePathMapper = namePathMapper;
        this.tracker = tracker;
        eventListener = tracker.getTrackedListener();
        filterProvider = new AtomicReference<JcrFilterProvider>(filter);
    }

    /**
     * Set the filter for the events this change processor will generate.
     * @param filter
     */
    public void setFilterProvider(JcrFilterProvider filter) {
        filterProvider.set(filter);
    }

    /**
     * Start this change processor
     * @param whiteboard  the whiteboard instance to used for scheduling individual
     *                    runs of this change processor.
     * @throws IllegalStateException if started already
     */
    public synchronized void start(Whiteboard whiteboard) {
        checkState(observer == null, "Change processor started already");
        observer = ((Observable) contentSession).addObserver(this);
        mbean = WhiteboardUtils.registerMBean(whiteboard, EventListenerMBean.class,
                tracker.getListenerMBean(), "EventListener", tracker.toString());

    }

    /**
     * Stop this change processor if running. After returning from this methods no further
     * events will be delivered.
     * @throws IllegalStateException if not yet started or stopped already
     */
    public synchronized void stop() {
        checkState(observer != null, "Change processor not started");
        try {
            mbean.unregister();
            observer.close();
        } catch (IOException e) {
            log.error("Error while stopping change listener", e);
        }
    }

    @Override
    public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
        if (previousRoot != null) {
            try {
                JcrFilterProvider provider = filterProvider.get();
                // FIXME don't rely on toString for session id
                if (provider.includeCommit(contentSession.toString(), info)) {
                    String path = namePathMapper.getOakPath(provider.getPath());
                    ImmutableTree beforeTree = getTree(previousRoot, path);
                    ImmutableTree afterTree = getTree(root, path);
                    TreePermission treePermission = getTreePermission(afterTree);
                    EventIterator<Event> events = new EventIterator<Event>(
                            beforeTree.getNodeState(), afterTree.getNodeState(), path,
                            provider.getFilter(beforeTree, afterTree, treePermission),
                            new JcrListener(beforeTree, afterTree, namePathMapper, info));
                    if (events.hasNext()) {
                        eventListener.onEvent(new EventIteratorAdapter(events));
                    }
                }
            } catch (Exception e) {
                log.warn("Error while dispatching observation events", e);
            }
        }
        previousRoot = root;
    }

    private static ImmutableTree getTree(NodeState nodeState, String path) {
        return new ImmutableRoot(nodeState).getTree(path);
    }

    private TreePermission getTreePermission(Tree tree) {
        return permissionProvider.getTreePermission(tree, TreePermission.EMPTY);
    }
}
