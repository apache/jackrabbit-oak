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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventListener;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

class ChangeProcessor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ChangeProcessor.class);
    private static final Marker DEPRECATED = MarkerFactory.getMarker("deprecated");

    private final ObservationManagerImpl observationManager;
    private final NamePathMapper namePathMapper;
    private final EventListener listener;
    private final AtomicReference<EventFilter> filterRef;
    private final AtomicReference<String> userDataRef = new AtomicReference<String>(null);

    private volatile boolean running;
    private volatile boolean stopping;
    private ScheduledFuture<?> future;
    private Listener changeListener;

    private boolean userInfoAccessedWithoutExternalsCheck;
    private boolean userInfoAccessedFromExternalEvent;
    private boolean dateAccessedWithoutExternalsCheck;
    private boolean dateAccessedFromExternalEvent;

    public ChangeProcessor(ObservationManagerImpl observationManager, EventListener listener, EventFilter filter) {
        this.observationManager = observationManager;
        this.namePathMapper = observationManager.getNamePathMapper();
        this.listener = listener;
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
     * @param executor
     * @throws IllegalStateException if started already
     */
    public synchronized void start(ScheduledExecutorService executor) {
        if (future != null) {
            throw new IllegalStateException("Change processor started already");
        }
        stopping = false;
        changeListener = observationManager.newChangeListener();
        future = executor.scheduleWithFixedDelay(this, 100, 1000, TimeUnit.MILLISECONDS);
    }


    /**
     * Stop this change processor if running. After returning from this methods no further
     * events will be delivered.
     * @throws IllegalStateException if not yet started or stopped already
     */
    public synchronized void stop() {
        if (future == null) {
            throw new IllegalStateException("Change processor not started");
        }

        try {
            stopping = true;
            future.cancel(true);
            while (running) {
                wait();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            changeListener.dispose();
            future = null;
        }
    }

    @Override
    public void run() {
        running = true;
        try{
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
            synchronized (this) {
                running = false;
                notifyAll();
            }
        }
    }

    synchronized void userInfoAccessedWithoutExternalCheck() {
        if (!userInfoAccessedWithoutExternalsCheck) {
            log.warn(DEPRECATED,
                    "Event listener " + listener + " is trying to access"
                    + " event user information without checking for whether"
                    + " the event is external");
            userInfoAccessedWithoutExternalsCheck = true;
        }
    }

    synchronized void userInfoAccessedFromExternalEvent() {
        if (!userInfoAccessedFromExternalEvent) {
            log.warn(DEPRECATED,
                    "Event listener " + listener + " is trying to access"
                    + " event user information from an external event");
            userInfoAccessedFromExternalEvent = true;
        }
    }

    synchronized void dateAccessedWithoutExternalCheck() {
        if (!dateAccessedWithoutExternalsCheck) {
            log.warn(DEPRECATED,
                    "Event listener " + listener + " is trying to access"
                    + " event date information without checking for whether"
                    + " the event is external");
            dateAccessedWithoutExternalsCheck = true;
        }
    }

    synchronized void dateAccessedFromExternalEvent() {
        if (!dateAccessedFromExternalEvent) {
            log.warn(DEPRECATED,
                    "Event listener " + listener + " is trying to access"
                    + " event date information from an external event");
            dateAccessedFromExternalEvent = true;
        }
    }

    //------------------------------------------------------------< private >---

    private class EventGeneratingNodeStateDiff extends RecursingNodeStateDiff {
        public static final int PURGE_LIMIT = 8192;

        private final ChangeSet changes;
        private final String path;
        private final NodeState associatedParentNode;
        private final EventGeneratingNodeStateDiff parent;
        private final String name;

        private List<Iterator<Event>> events;
        private int childNodeCount;

        EventGeneratingNodeStateDiff(ChangeSet changes, String path, List<Iterator<Event>> events,
                NodeState associatedParentNode, EventGeneratingNodeStateDiff parent, String name) {

            this.changes = changes;
            this.path = path;
            this.events = events;
            this.associatedParentNode = associatedParentNode;
            this.parent = parent;
            this.name = name;
        }

        public EventGeneratingNodeStateDiff(ChangeSet changes) {
            // FIXME associatedParentNode should be the root node here
            this(changes, "/", new ArrayList<Iterator<Event>>(PURGE_LIMIT), null, null, "");
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
            if (filterRef.get().include(Event.PROPERTY_ADDED, jcrPath(), associatedParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_ADDED, path, after);
                events.add(Iterators.singletonIterator(event));
            }
            return !stopping;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (filterRef.get().include(Event.PROPERTY_CHANGED, jcrPath(), associatedParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_CHANGED, path, after);
                events.add(Iterators.singletonIterator(event));
            }
            return !stopping;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (filterRef.get().include(Event.PROPERTY_REMOVED, jcrPath(), associatedParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_REMOVED, path, before);
                events.add(Iterators.singletonIterator(event));
            }
            return !stopping;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (filterRef.get().includeChildren(jcrPath())) {
                Iterator<Event> events = generateNodeEvents(Event.NODE_ADDED, path, name, after);
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
                Iterator<Event> events = generateNodeEvents(Event.NODE_REMOVED, path, name, before);
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
                        changes, PathUtils.concat(path, name), events, after, this, name);
                return VisibleDiff.wrap(diff);
            } else {
                return new RecursingNodeStateDiff();
            }
        }

        private EventImpl createEvent(int eventType, String jcrPath, String id) {
            // TODO support identifier, info
            return new EventImpl(ChangeProcessor.this, eventType, jcrPath, changes.getUserId(),
                    id, null, changes.getDate(), userDataRef.get(), changes.isExternal());
        }

        private String getId(String childName) {
            if (parent == null) {
                return '/' + namePathMapper.getJcrName(childName);
            }

            PropertyState uuid = associatedParentNode.getProperty(JcrConstants.JCR_UUID);
            if (uuid == null) {
                return parent.getId(name) + '/' + namePathMapper.getJcrName(childName);
            }

            return uuid.getValue(Type.STRING);
        }

        private Event generatePropertyEvent(int eventType, String parentPath, PropertyState property) {
            String jcrPath = namePathMapper.getJcrPath(PathUtils.concat(parentPath, property.getName()));
            return createEvent(eventType, jcrPath, parent.getId(name));
        }

        private Iterator<Event> generateNodeEvents(int eventType, String parentPath, String childName, NodeState node) {
            EventFilter filter = filterRef.get();
            final String path = PathUtils.concat(parentPath, childName);
            String jcrParentPath = namePathMapper.getJcrPath(parentPath);
            String jcrPath = namePathMapper.getJcrPath(path);

            Iterator<Event> nodeEvent;
            if (filter.include(eventType, jcrParentPath, associatedParentNode)) {
                Event event = createEvent(eventType, jcrPath, getId(childName));
                nodeEvent = Iterators.singletonIterator(event);
            } else {
                nodeEvent = Iterators.emptyIterator();
            }

            final int propertyEventType = eventType == Event.NODE_ADDED
                    ? Event.PROPERTY_ADDED
                    : Event.PROPERTY_REMOVED;

            Iterator<Event> propertyEvents;
            if (filter.include(propertyEventType, jcrPath, associatedParentNode)) {
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
                                return generatePropertyEvent(propertyEventType, path, property);
                            }
                        });
            } else {
                propertyEvents = Iterators.emptyIterator();
            }

            Iterator<Event> childNodeEvents = filter.includeChildren(jcrPath)
                    ? Iterators.concat(generateChildEvents(eventType, path, node))
                    : Iterators.<Event>emptyIterator();

            return Iterators.concat(nodeEvent, propertyEvents, childNodeEvents);
        }

        // FIXME this doesn't correctly track the associated parent node
        private Iterator<Iterator<Event>> generateChildEvents(final int eventType, final String parentPath, NodeState node) {
            return Iterators.transform(
                    Iterators.filter(node.getChildNodeEntries().iterator(),
                        new Predicate<ChildNodeEntry>() {
                            @Override
                            public boolean apply(ChildNodeEntry entry) {
                                return !NodeStateUtils.isHidden(entry.getName());
                            }
                    }),
                    new Function<ChildNodeEntry, Iterator<Event>>() {
                        @Override
                        public Iterator<Event> apply(ChildNodeEntry entry) {
                            return generateNodeEvents(eventType, parentPath, entry.getName(), entry.getNodeState());
                        }
                    });
        }

    }

}
