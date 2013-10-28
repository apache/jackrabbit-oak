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
import static com.google.common.collect.Lists.newArrayList;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager.getIdentifier;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.observation.Event;
import javax.jcr.observation.EventListener;

import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.ChangeSet;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.Listener;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code ChangeProcessor} generates observation {@link javax.jcr.observation.Event}s
 * based on a {@link EventFilter} and delivers them to an {@link javax.jcr.observation.EventListener}.
 * <p>
 * After instantiation a {@code ChangeProcessor} must be started in order for its
 * {@link ListenerThread listener thread's} run methods to be regularly
 * executed and stopped in order to not execute its run method anymore.
 */
public class ChangeProcessor {

    private static final Logger log =
            LoggerFactory.getLogger(ChangeProcessor.class);

    private final ContentSession contentSession;
    private final NamePathMapper namePathMapper;
    private final AtomicReference<EventFilter> filterRef;

    private final ListenerTracker tracker;
    private final EventListener listener;

    /**
     * Background thread used to wait for and deliver events.
     */
    private ListenerThread thread = null;

    private volatile boolean stopping = false;

    public ChangeProcessor(
            ContentSession contentSession, NamePathMapper namePathMapper,
            ListenerTracker tracker, EventFilter filter) {
        checkArgument(contentSession instanceof Observable);
        this.contentSession = contentSession;
        this.namePathMapper = namePathMapper;
        this.tracker = tracker;
        this.listener = tracker.getTrackedListener();
        filterRef = new AtomicReference<EventFilter>(filter);
    }

    /**
     * Set the filter for the events this change processor will generate.
     * @param filter
     */
    public void setFilter(EventFilter filter) {
        filterRef.set(filter);
    }

    /**
     * Start this change processor
     * @param whiteboard  the whiteboard instance to used for scheduling individual
     *                    runs of this change processor.
     * @throws IllegalStateException if started already
     */
    public synchronized void start(Whiteboard whiteboard) {
        checkState(thread == null, "Change processor started already");

        thread = new ListenerThread(whiteboard);
        thread.setDaemon(true);
        thread.setPriority(Thread.MIN_PRIORITY);
        thread.start();
    }

    /**
     * Stop this change processor if running. After returning from this methods no further
     * events will be delivered.
     * @throws IllegalStateException if not yet started or stopped already
     */
    public synchronized void stop() {
        checkState(thread != null, "Change processor not started");
        checkState(!stopping, "Change processor already stopped");

        stopping = true;
    }

    private static ImmutableTree getTree(NodeState beforeState, String path) {
        return new ImmutableRoot(beforeState).getTree(path);
    }

    //------------------------------------------------------------< private >---

    private class ListenerThread extends Thread {

        private final Listener changeListener =
                ((Observable) contentSession).newListener();

        private final Registration mbean;

        ListenerThread(Whiteboard whiteboard) {
            mbean = WhiteboardUtils.registerMBean(
                    whiteboard, EventListenerMBean.class,
                    tracker.getListenerMBean(), "EventListener",
                    tracker.toString());
        }

        @Override
        public void run() {
            try {
                ChangeSet changes = changeListener.getChanges(100);
                while (!stopping) {
                    EventFilter filter = filterRef.get();
                    // FIXME don't rely on toString for session id
                    if (changes != null &&
                            !(filter.excludeLocal() && changes.isLocal(contentSession.toString()))) {
                        String path = namePathMapper.getOakPath(filter.getPath());
                        ImmutableTree beforeTree = getTree(changes.getBeforeState(), path);
                        ImmutableTree afterTree = getTree(changes.getAfterState(), path);
                        EventGeneratingValidator events = new EventGeneratingValidator(
                                changes.getCommitInfo(), beforeTree, afterTree);
                        VisibleValidator visibleEvents = new VisibleValidator(events, true, true);
                        SecureValidator.compare(beforeTree, afterTree, visibleEvents);
                    }
                    changes = changeListener.getChanges(100);
                }
            } catch (Exception e) {
                log.debug("Error while dispatching observation events", e);
            } finally {
                mbean.unregister();
                changeListener.dispose();
            }
        }
    }

    private class EventGeneratingValidator extends DefaultValidator {
        public static final int EVENT_LIMIT = 8192;

        private final String userId;
        private final String message;
        private final long timestamp;
        private final boolean external;

        private final Tree beforeTree;
        private final Tree afterTree;
        private final List<Event> events;
        private final boolean isRoot;

        EventGeneratingValidator(CommitInfo info, Tree beforeTree, Tree afterTree) {
            if (info != null) {
                this.userId = info.getUserId();
                this.message = info.getMessage();
                this.timestamp = info.getDate();
                this.external = false;
            } else {
                this.userId = CommitInfo.OAK_UNKNOWN;
                this.message = null;
                // we can't tell exactly when external changes were committed,
                // so we just use a rough estimate like this
                this.timestamp = System.currentTimeMillis();
                this.external = true;
            }
            this.beforeTree = beforeTree;
            this.afterTree = afterTree;
            this.events = newArrayList();
            isRoot = true;
        }

        private EventGeneratingValidator(EventGeneratingValidator parent, String name) {
            this.userId = parent.userId;
            this.message = parent.message;
            this.timestamp = parent.timestamp;
            this.external = parent.external;
            this.beforeTree = parent.beforeTree.getChild(name);
            this.afterTree = parent.afterTree.getChild(name);
            this.events = parent.events;
            isRoot = false;
        }

        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
            // TODO instead of putting events in a list generate them on demand
            // on calls to iterator.next()
            if (isRoot || events.size() > EVENT_LIMIT) {
                Iterator<Event> eventIterator = newArrayList(events.iterator()).iterator();
                events.clear();
                if (eventIterator.hasNext()) {
                    try {
                        listener.onEvent(new EventIteratorAdapter(eventIterator) {
                            @Override
                            public boolean hasNext() {
                                return !stopping && super.hasNext();
                            }
                        });
                    }
                    catch (Exception e) {
                        log.warn("Unhandled exception in observation listener: " + listener, e);
                    }
                }
            }
        }

        @Override
        public void propertyAdded(PropertyState after) {
            if (!stopping && filterRef.get().include(PROPERTY_ADDED, afterTree)) {
                events.add(createEvent(PROPERTY_ADDED, afterTree, after));
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (!stopping && filterRef.get().include(Event.PROPERTY_CHANGED, afterTree)) {
                events.add(createEvent(Event.PROPERTY_CHANGED, afterTree, after));
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            if (!stopping && filterRef.get().include(PROPERTY_REMOVED, afterTree)) {
                events.add(createEvent(PROPERTY_REMOVED, beforeTree, before));
            }
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) {
            if (stopping) {
                return null;
            }
            EventFilter eventFilter = filterRef.get();
            if (eventFilter.include(NODE_ADDED, afterTree)) {
                events.add(createEvent(NODE_ADDED, afterTree.getChild(name)));
            }
            return createChildValidator(eventFilter, afterTree.getPath(), name);
        }

        @Override
        public Validator childNodeDeleted(String name, NodeState before) {
            if (stopping) {
                return null;
            }
            EventFilter eventFilter = filterRef.get();
            if (eventFilter.include(NODE_REMOVED, beforeTree)) {
                events.add(createEvent(NODE_REMOVED, beforeTree.getChild(name)));
            }
            return createChildValidator(eventFilter, beforeTree.getPath(), name);
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) {
            if (stopping) {
                return null;
            }
            return createChildValidator(filterRef.get(), afterTree.getPath(), name);
        }

        private Validator createChildValidator(EventFilter eventFilter, String parentPath,
                String name) {
            return eventFilter.includeChildren(parentPath)
                    ? new EventGeneratingValidator(this, name)
                    : null;
        }

        private Event createEvent(int eventType, Tree tree) {
            return createEvent(eventType, tree.getPath(), getIdentifier(tree));
        }

        private Event createEvent(int eventType, Tree parent, PropertyState property) {
            String path = PathUtils.concat(parent.getPath(), property.getName());
            return createEvent(eventType, path, getIdentifier(parent));
        }

        private Event createEvent(int eventType, String path, String id) {
            return new EventImpl(
                    eventType, namePathMapper.getJcrPath(path), userId, id,
                    null /* TODO: info map */, timestamp, message, external);
        }

    }

}
