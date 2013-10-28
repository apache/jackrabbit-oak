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
import static com.google.common.collect.Iterators.emptyIterator;
import static com.google.common.collect.Iterators.singletonIterator;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Lists.newArrayList;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager.getIdentifier;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventListener;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
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
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.RecursingNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.VisibleDiff;
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
 * {@link #run()} methods to be regularly executed and stopped in order to not
 * execute its run method anymore.
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
                        EventGeneratingNodeStateDiff diff = new EventGeneratingNodeStateDiff(
                                changes.getCommitInfo(), beforeTree, afterTree);
                        SecureNodeStateDiff.compare(VisibleDiff.wrap(diff), beforeTree, afterTree);
                        if (!stopping) {
                            diff.sendEvents();
                        }
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

    private class EventGeneratingNodeStateDiff extends RecursingNodeStateDiff {
        public static final int EVENT_LIMIT = 8192;

        private final String userId;
        private final String message;
        private final long timestamp;
        private final boolean external;

        private final Tree beforeTree;
        private final Tree afterTree;

        private List<Iterator<Event>> events;
        private int eventCount;

        EventGeneratingNodeStateDiff(
                CommitInfo info, Tree beforeTree, Tree afterTree) {
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
        }

        private EventGeneratingNodeStateDiff(
                EventGeneratingNodeStateDiff parent, String name) {
            this.userId = parent.userId;
            this.message = parent.message;
            this.timestamp = parent.timestamp;
            this.external = parent.external;
            this.beforeTree = parent.beforeTree.getChild(name);
            this.afterTree = parent.afterTree.getChild(name);
            this.events = parent.events;
        }

        public void sendEvents() {
            Iterator<Event> eventIt = Iterators.concat(events.iterator());
            if (eventIt.hasNext()) {
                try {
                    listener.onEvent(new EventIteratorAdapter(eventIt) {
                        @Override
                        public boolean hasNext() {
                            return !stopping && super.hasNext();
                        }
                    });
                }
                catch (Exception e) {
                    log.warn("Unhandled exception in observation listener: " + listener, e);
                }
                events = new ArrayList<Iterator<Event>>(EVENT_LIMIT);
            }
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (filterRef.get().include(PROPERTY_ADDED, afterTree)) {
                Event event = generatePropertyEvent(PROPERTY_ADDED, afterTree, after);
                events.add(singletonIterator(event));
            }
            return !stopping;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (filterRef.get().include(Event.PROPERTY_CHANGED, afterTree)) {
                Event event = generatePropertyEvent(Event.PROPERTY_CHANGED, afterTree, after);
                events.add(singletonIterator(event));
            }
            return !stopping;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (filterRef.get().include(PROPERTY_REMOVED, afterTree)) {
                Event event = generatePropertyEvent(PROPERTY_REMOVED, beforeTree, before);
                events.add(singletonIterator(event));
            }
            return !stopping;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (filterRef.get().includeChildren(afterTree.getPath())) {
                Iterator<Event> events = generateNodeEvents(
                        NODE_ADDED, afterTree.getChild(name));
                this.events.add(events);
                if (++eventCount > EVENT_LIMIT) {
                    sendEvents();
                }
            }
            return !stopping;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (filterRef.get().includeChildren(beforeTree.getPath())) {
                Iterator<Event> events = generateNodeEvents(
                        NODE_REMOVED, beforeTree.getChild(name));
                this.events.add(events);
            }
            return !stopping;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            return !stopping;
        }

        @Nonnull
        @Override
        public RecursingNodeStateDiff createChildDiff(String name, NodeState before, NodeState after) {
            if (filterRef.get().includeChildren(afterTree.getPath())) {
                EventGeneratingNodeStateDiff diff =
                        new EventGeneratingNodeStateDiff(this, name);
                return VisibleDiff.wrap(diff);
            } else {
                return RecursingNodeStateDiff.EMPTY;
            }
        }

        private EventImpl createEvent(int eventType, String path, String id) {
            return new EventImpl(
                    eventType, namePathMapper.getJcrPath(path), userId, id,
                    null /* TODO: info map */, timestamp, message, external);
        }

        private Event generatePropertyEvent(int eventType, Tree parent, PropertyState property) {
            String path = PathUtils.concat(parent.getPath(), property.getName());
            return createEvent(eventType, path, getIdentifier(parent));
        }

        private Iterator<Event> generateNodeEvents(int eventType, final Tree tree) {
            EventFilter filter = filterRef.get();
            Iterator<Event> nodeEvent;
            if (filter.include(eventType, tree.isRoot() ? null : tree.getParent())) {
                Event event = createEvent(eventType, tree.getPath(), getIdentifier(tree));
                nodeEvent = singletonIterator(event);
            } else {
                nodeEvent = emptyIterator();
            }

            final int propertyEventType = eventType == NODE_ADDED
                    ? PROPERTY_ADDED
                    : PROPERTY_REMOVED;

            Iterator<Event> propertyEvents;
            if (filter.include(propertyEventType, tree)) {
                propertyEvents = transform(
                        tree.getProperties().iterator(),
                        new Function<PropertyState, Event>() {
                            @Override
                            public Event apply(PropertyState property) {
                                return generatePropertyEvent(propertyEventType, tree, property);
                            }
                        });
            } else {
                propertyEvents = emptyIterator();
            }

            Iterator<Event> childNodeEvents = filter.includeChildren(tree.getPath())
                    ? Iterators.concat(generateChildEvents(eventType, tree))
                    : Iterators.<Event>emptyIterator();

            return Iterators.concat(nodeEvent, propertyEvents, childNodeEvents);
        }

        private Iterator<Iterator<Event>> generateChildEvents(final int eventType, final Tree tree) {
            return transform(
                    tree.getChildren().iterator(),
                    new Function<Tree, Iterator<Event>>() {
                        @Override
                        public Iterator<Event> apply(Tree child) {
                            return generateNodeEvents(eventType, child);
                        }
                    });
        }

    }

}
