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

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventListener;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.ChangeSet;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.Listener;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.VisibleDiff;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// michid doc
class ChangeProcessor implements Runnable {

    private static final Logger log =
            LoggerFactory.getLogger(ChangeProcessor.class);

    private final ObservationManagerImpl observationManager;
    private final NamePathMapper namePathMapper;
    private final AtomicReference<EventFilter> filterRef;
    private final AtomicReference<String> userDataRef = new AtomicReference<String>(null);

    private final ListenerTracker tracker;
    private final EventListener listener;

    private volatile Thread running = null;
    private volatile boolean stopping = false;
    private Runnable deferredUnregister;

    private Registration runnable;
    private Registration mbean;
    private Listener changeListener;

    public ChangeProcessor(
            ObservationManagerImpl observationManager,
            ListenerTracker tracker,
            EventFilter filter) {
        this.observationManager = observationManager;
        this.namePathMapper = observationManager.getNamePathMapper();
        this.tracker = tracker;
        this.listener = tracker.getTrackedListener();
        filterRef = new AtomicReference<EventFilter>(filter);
    }

    public void setFilter(EventFilter filter) {
        filterRef.set(filter);
    }

    public void setUserData(String userData) {
        userDataRef.set(userData);
    }

    /**
     * Start the change processor on the passed {@code executor}.
     * @param whiteboard
     * @throws IllegalStateException if started already
     */
    public synchronized void start(Whiteboard whiteboard) {
        checkState(runnable == null, "Change processor started already");

        stopping = false;
        changeListener = observationManager.newChangeListener();
        runnable = WhiteboardUtils.scheduleWithFixedDelay(whiteboard, this, 1);
        mbean = WhiteboardUtils.registerMBean(
                whiteboard, EventListenerMBean.class, tracker.getListenerMBean(),
                "EventListener", tracker.toString());
    }

    /**
     * Stop this change processor if running. After returning from this methods no further
     * events will be delivered.
     * @throws IllegalStateException if not yet started or stopped already
     */
    public void stop() {
        stopping = true; // do this outside synchronization

        if (running == Thread.currentThread()) {
            // Defer stopping from event listener, defer unregistering until
            // event listener is done
            deferredUnregister = new Runnable() {
                @Override
                public void run() {
                    unregister();
                }
            };
        } else {
            // Otherwise wait for the event listener to terminate and unregister immediately
            synchronized (this) {
                try {
                    while (running != null) {
                        wait();
                    }
                    unregister();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void unregister() {
        checkState(runnable != null, "Change processor not started");
        mbean.unregister();
        runnable.unregister();
        changeListener.dispose();
    }

    @Override
    public void run() {
        // guarantee that only one thread is processing changes at a time
        synchronized (this) {
            if (running != null) {
                return;
            } else {
                running = Thread.currentThread();
            }
        }

        try {
            ChangeSet changes = changeListener.getChanges();
            if (changes != null &&
                    !(filterRef.get().excludeLocal() && changes.isLocal(observationManager.getContentSession()))) {
                EventGeneratingNodeStateDiff diff = new EventGeneratingNodeStateDiff(changes);
                changes.diff(VisibleDiff.wrap(diff));
                if (!stopping) {
                    diff.sendEvents();
                }
            }
        } catch (Exception e) {
            log.error("Unable to generate or send events", e);
        } finally {
            running = null;
            synchronized (this) { notifyAll(); }
            if (deferredUnregister != null) {
                deferredUnregister.run();
            }
        }
    }

    //------------------------------------------------------------< private >---

    private class EventGeneratingNodeStateDiff extends RecursingNodeStateDiff {
        public static final int PURGE_LIMIT = 8192;

        private final ChangeSet changes;
        private final String path;
        private final NodeState beforeParentNode;
        private final NodeState afterParentNode;
        private final EventGeneratingNodeStateDiff parent;
        private final String name;

        private List<Iterator<Event>> events;
        private int childNodeCount;

        EventGeneratingNodeStateDiff(ChangeSet changes, String path, List<Iterator<Event>> events,
                NodeState beforeParentNode, NodeState afterParentNode,
                EventGeneratingNodeStateDiff parent, String name) {

            this.changes = changes;
            this.path = path;
            this.events = events;
            this.beforeParentNode = beforeParentNode;
            this.afterParentNode = afterParentNode;
            this.parent = parent;
            this.name = name;
        }

        public EventGeneratingNodeStateDiff(ChangeSet changes) {
            // michid parent nodes should be the root here
            this(changes, "/", new ArrayList<Iterator<Event>>(PURGE_LIMIT), null, null, null, "");
        }

        public void sendEvents() {
            Iterator<Event> eventIt = Iterators.concat(events.iterator());
            if (eventIt.hasNext()) {
                observationManager.setHasEvents();
                listener.onEvent(new EventIteratorAdapter(eventIt) {
                    @Override
                    public boolean hasNext() {
                        return !stopping && super.hasNext();
                    }
                });
                events = new ArrayList<Iterator<Event>>(PURGE_LIMIT);
            }
        }

        private String jcrPath() {
            return namePathMapper.getJcrPath(path);
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (filterRef.get().include(Event.PROPERTY_ADDED, jcrPath(), afterParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_ADDED, path, after, getAfterId());
                events.add(Iterators.singletonIterator(event));
            }
            return !stopping;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (filterRef.get().include(Event.PROPERTY_CHANGED, jcrPath(), afterParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_CHANGED, path, after, getAfterId());
                events.add(Iterators.singletonIterator(event));
            }
            return !stopping;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (filterRef.get().include(Event.PROPERTY_REMOVED, jcrPath(), afterParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_REMOVED, path, before, getBeforeId());
                events.add(Iterators.singletonIterator(event));
            }
            return !stopping;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (filterRef.get().includeChildren(jcrPath())) {
                Iterator<Event> events = generateNodeEvents(Event.NODE_ADDED, path, name,
                        after, afterParentNode, getAfterId(after, name));
                this.events.add(events);
                if (++childNodeCount > PURGE_LIMIT) {
                    sendEvents();
                }
            }
            return !stopping;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (filterRef.get().includeChildren(jcrPath())) {
                Iterator<Event> events = generateNodeEvents(Event.NODE_REMOVED, path, name,
                        before, beforeParentNode, getBeforeId(before, name));
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
            if (filterRef.get().includeChildren(jcrPath())) {
                EventGeneratingNodeStateDiff diff = new EventGeneratingNodeStateDiff(
                        changes, PathUtils.concat(path, name), events, before, after, this, name);
                return VisibleDiff.wrap(diff);
            } else {
                return new RecursingNodeStateDiff();
            }
        }

        private EventImpl createEvent(int eventType, String jcrPath, String id) {
            // TODO support info
            return new EventImpl(
                    eventType, jcrPath, changes.getUserId(),
                    id, null, changes.getDate(), userDataRef.get(),
                    changes.isExternal());
        }

        private String getRootId() {
            // FIXME return id of root node if available
            return "/";
        }

        private String getBeforeId(NodeState childNode, String childName) {
            if (parent == null) {
                return getRootId() + namePathMapper.getJcrName(childName);
            }

            PropertyState uuid = childNode.getProperty(JcrConstants.JCR_UUID);
            if (uuid == null) {
                return parent.getBeforeId(beforeParentNode, name) + '/' + namePathMapper.getJcrName(childName);
            }

            return uuid.getValue(Type.STRING);
        }

        private String getAfterId(NodeState childNode, String childName) {
            if (parent == null) {
                return getRootId() + namePathMapper.getJcrName(childName);
            }

            PropertyState uuid = childNode.getProperty(JcrConstants.JCR_UUID);
            if (uuid == null) {
                return parent.getAfterId(afterParentNode, name) + '/' + namePathMapper.getJcrName(childName);
            }

            return uuid.getValue(Type.STRING);
        }

        private String getBeforeId() {
            if (parent == null) {
                return getRootId();
            } else {
                return parent.getBeforeId(beforeParentNode, name);
            }
        }

        private String getAfterId() {
            if (parent == null) {
                return getRootId();
            } else {
                return parent.getBeforeId(beforeParentNode, name);
            }
        }

        private Event generatePropertyEvent(int eventType, String parentPath, PropertyState property, String id) {
            String jcrPath = namePathMapper.getJcrPath(PathUtils.concat(parentPath, property.getName()));
            return createEvent(eventType, jcrPath, id);
        }

        private Iterator<Event> generateNodeEvents(int eventType, String parentPath, String childName,
                NodeState node, NodeState parentNode, final String id) {
            EventFilter filter = filterRef.get();
            final String path = PathUtils.concat(parentPath, childName);
            String jcrParentPath = namePathMapper.getJcrPath(parentPath);
            String jcrPath = namePathMapper.getJcrPath(path);

            Iterator<Event> nodeEvent;
            if (filter.include(eventType, jcrParentPath, parentNode)) {
                Event event = createEvent(eventType, jcrPath, id);
                nodeEvent = Iterators.singletonIterator(event);
            } else {
                nodeEvent = Iterators.emptyIterator();
            }

            final int propertyEventType = eventType == Event.NODE_ADDED
                    ? Event.PROPERTY_ADDED
                    : Event.PROPERTY_REMOVED;

            Iterator<Event> propertyEvents;
            if (filter.include(propertyEventType, jcrPath, parentNode)) {
                propertyEvents = Iterators.transform(
                        Iterators.filter(
                                node.getProperties().iterator(),
                                new Predicate<PropertyState>() {
                                    @Override
                                    public boolean apply(PropertyState propertyState) {
                                        return !NodeStateUtils.isHidden(propertyState.getName());
                                    }
                                }),
                        new Function<PropertyState, Event>() {
                            @Override
                            public Event apply(PropertyState property) {
                                return generatePropertyEvent(propertyEventType, path, property, id);
                            }
                        });
            } else {
                propertyEvents = Iterators.emptyIterator();
            }

            Iterator<Event> childNodeEvents = filter.includeChildren(jcrPath)
                    ? Iterators.concat(generateChildEvents(eventType, path, node, id))
                    : Iterators.<Event>emptyIterator();

            return Iterators.concat(nodeEvent, propertyEvents, childNodeEvents);
        }

        private Iterator<Iterator<Event>> generateChildEvents(final int eventType, final String parentPath,
                final NodeState parentNode, final String parentId) {
            return Iterators.transform(
                    Iterators.filter(parentNode.getChildNodeEntries().iterator(),
                            new Predicate<ChildNodeEntry>() {
                                @Override
                                public boolean apply(ChildNodeEntry entry) {
                                    return !NodeStateUtils.isHidden(entry.getName());
                                }
                            }),
                    new Function<ChildNodeEntry, Iterator<Event>>() {
                        @Override
                        public Iterator<Event> apply(ChildNodeEntry entry) {
                            NodeState node = entry.getNodeState();
                            String name = entry.getName();
                            return generateNodeEvents(eventType, parentPath, name,
                                    node, parentNode, getId(parentId, node, name));
                        }

                        private String getId(String parentId, NodeState node, String name) {
                            PropertyState uuid = node.getProperty(JcrConstants.JCR_UUID);
                            if (uuid == null) {
                                return parentId + '/' + namePathMapper.getJcrName(name);
                            } else {
                                return uuid.getValue(Type.STRING);
                            }
                        }
                    });
        }

    }

}
