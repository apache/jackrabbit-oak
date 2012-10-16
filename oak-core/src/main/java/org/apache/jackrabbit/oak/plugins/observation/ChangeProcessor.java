/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.observation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.observation.Event;
import javax.jcr.observation.EventListener;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.oak.spi.observation.ChangeExtractor;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

class ChangeProcessor implements Runnable {
    private final ObservationManagerImpl observationManager;
    private final NamePathMapper namePathMapper;
    private final ChangeExtractor changeExtractor;
    private final EventListener listener;
    private final AtomicReference<ChangeFilter> filterRef;
    private volatile boolean running;
    private volatile boolean stopping;
    private ScheduledFuture<?> future;

    public ChangeProcessor(ObservationManagerImpl observationManager, EventListener listener, ChangeFilter filter) {
        this.observationManager = observationManager;
        this.namePathMapper = observationManager.getNamePathMapper();
        this.changeExtractor = observationManager.getChangeExtractor();
        this.listener = listener;
        filterRef = new AtomicReference<ChangeFilter>(filter);
    }

    public void setFilter(ChangeFilter filter) {
        filterRef.set(filter);
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
            future = null;
        }
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
        future = executor.scheduleWithFixedDelay(this, 100, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        running = true;
        try{
            EventGeneratingNodeStateDiff diff = new EventGeneratingNodeStateDiff();
            changeExtractor.getChanges(diff);
            if (!stopping) {
                diff.sendEvents();
            }
        } finally {
            synchronized (this) {
                running = false;
                notifyAll();
            }
        }
    }

    //------------------------------------------------------------< private >---

    private class EventGeneratingNodeStateDiff implements NodeStateDiff {
        public static final int PURGE_LIMIT = 8192;

        private final String path;
        private final NodeState associatedParentNode;

        private int childNodeCount;
        private List<Iterator<Event>> events;

        EventGeneratingNodeStateDiff(String path, List<Iterator<Event>> events, NodeState associatedParentNode) {
            this.path = path;
            this.associatedParentNode = associatedParentNode;
            this.events = events;
        }

        public EventGeneratingNodeStateDiff() {
            this("/", new ArrayList<Iterator<Event>>(PURGE_LIMIT), null);
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
        public void propertyAdded(PropertyState after) {
            if (!stopping && filterRef.get().include(Event.PROPERTY_ADDED, jcrPath(), associatedParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_ADDED, path, after);
                events.add(Iterators.singletonIterator(event));
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (!stopping && filterRef.get().include(Event.PROPERTY_CHANGED, jcrPath(), associatedParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_CHANGED, path, after);
                events.add(Iterators.singletonIterator(event));
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            if (!stopping && filterRef.get().include(Event.PROPERTY_REMOVED, jcrPath(), associatedParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_REMOVED, path, before);
                events.add(Iterators.singletonIterator(event));
            }
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                return;
            }
            if (!stopping && filterRef.get().includeChildren(jcrPath())) {
                Iterator<Event> events = generateNodeEvents(Event.NODE_ADDED, path, name, after);
                this.events.add(events);
                if (++childNodeCount > PURGE_LIMIT) {
                    sendEvents();
                }
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (NodeStateUtils.isHidden(name)) {
                return;
            }
            if (!stopping && filterRef.get().includeChildren(jcrPath())) {
                Iterator<Event> events = generateNodeEvents(Event.NODE_REMOVED, path, name, before);
                this.events.add(events);
            }
        }

        @Override
        public void childNodeChanged(String name, NodeState before, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                return;
            }
            if (!stopping && filterRef.get().includeChildren(jcrPath())) {
                EventGeneratingNodeStateDiff diff = new EventGeneratingNodeStateDiff(
                        PathUtils.concat(path, name), events, after);
                after.compareAgainstBaseState(before, diff);
                if (events.size() > PURGE_LIMIT) {
                    diff.sendEvents();
                }
            }
        }

        private Event generatePropertyEvent(int eventType, String parentPath, PropertyState property) {
            String jcrPath = namePathMapper.getJcrPath(PathUtils.concat(parentPath, property.getName()));

            // TODO support userId, identifier, info, date
            return new EventImpl(eventType, jcrPath, null, null, null, 0);
        }

        private Iterator<Event> generateNodeEvents(int eventType, String parentPath, String name, NodeState node) {
            ChangeFilter filter = filterRef.get();
            final String path = PathUtils.concat(parentPath, name);
            String jcrParentPath = namePathMapper.getJcrPath(parentPath);
            String jcrPath = namePathMapper.getJcrPath(path);

            Iterator<Event> nodeEvent;
            if (filter.include(eventType, jcrParentPath, associatedParentNode)) {
                // TODO support userId, identifier, info, date
                Event event = new EventImpl(eventType, jcrPath, null, null, null, 0);
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
                        node.getProperties().iterator(),
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

        private Iterator<Iterator<Event>> generateChildEvents(final int eventType, final String parentPath, NodeState node) {
            return Iterators.transform(
                    node.getChildNodeEntries().iterator(),
                    new Function<ChildNodeEntry, Iterator<Event>>() {
                        @Override
                        public Iterator<Event> apply(ChildNodeEntry entry) {
                            return generateNodeEvents(eventType, parentPath, entry.getName(), entry.getNodeState());
                        }
                    });
        }

    }

}
