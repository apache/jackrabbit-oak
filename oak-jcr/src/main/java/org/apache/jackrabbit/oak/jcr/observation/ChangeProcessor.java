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
package org.apache.jackrabbit.oak.jcr.observation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.observation.Event;
import javax.jcr.observation.EventListener;

import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.oak.api.ChangeExtractor;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;

import static org.apache.jackrabbit.commons.iterator.LazyIteratorChain.chain;
import static org.apache.jackrabbit.oak.util.Iterators.singleton;

class ChangeProcessor extends TimerTask {
    private final NamePathMapper namePathMapper;
    private final ChangeExtractor changeExtractor;
    private final EventListener listener;
    private final AtomicReference<ChangeFilter> filterRef;

    private volatile boolean stopped;

    public ChangeProcessor(NamePathMapper namePathMapper, ChangeExtractor changeExtractor, EventListener listener,
            ChangeFilter filter) {
        this.namePathMapper = namePathMapper;
        this.changeExtractor = changeExtractor;
        this.listener = listener;
        filterRef = new AtomicReference<ChangeFilter>(filter);
    }

    public void setFilter(ChangeFilter filter) {
        filterRef.set(filter);
    }

    public void stop() {
        cancel();
        stopped = true;
    }

    @Override
    public void run() {
        EventGeneratingNodeStateDiff diff = new EventGeneratingNodeStateDiff();
        changeExtractor.getChanges(diff);
        diff.sendEvents();
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
            Iterator<Event> eventIt = Iterators.flatten(events.iterator());
            if (eventIt.hasNext()) {
                listener.onEvent(new EventIteratorAdapter(eventIt));
                events = new ArrayList<Iterator<Event>>(PURGE_LIMIT);
            }
        }

        private String jcrPath() {
            return namePathMapper.getJcrPath(path);
        }

        @Override
        public void propertyAdded(PropertyState after) {
            if (!stopped && filterRef.get().include(Event.PROPERTY_ADDED, jcrPath(), associatedParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_ADDED, path, after);
                events.add(Iterators.singleton(event));
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (!stopped && filterRef.get().include(Event.PROPERTY_CHANGED, jcrPath(), associatedParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_CHANGED, path, after);
                events.add(Iterators.singleton(event));
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            if (!stopped && filterRef.get().include(Event.PROPERTY_REMOVED, jcrPath(), associatedParentNode)) {
                Event event = generatePropertyEvent(Event.PROPERTY_REMOVED, path, before);
                events.add(Iterators.singleton(event));
            }
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (!stopped && filterRef.get().includeChildren(jcrPath())) {
                Iterator<Event> events = generateNodeEvents(Event.NODE_ADDED, path, name, after);
                this.events.add(events);
                if (++childNodeCount > PURGE_LIMIT) {
                    sendEvents();
                }
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (!stopped && filterRef.get().includeChildren(jcrPath())) {
                Iterator<Event> events = generateNodeEvents(Event.NODE_REMOVED, path, name, before);
                this.events.add(events);
            }
        }

        @Override
        public void childNodeChanged(String name, NodeState before, NodeState after) {
            if (!stopped && filterRef.get().includeChildren(jcrPath())) {
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
                nodeEvent = singleton(event);
            }
            else {
                nodeEvent = Iterators.empty();
            }

            final int propertyEventType = eventType == Event.NODE_ADDED
                    ? Event.PROPERTY_ADDED
                    : Event.PROPERTY_REMOVED;

            Iterator<Event> propertyEvents;
            if (filter.include(propertyEventType, jcrPath, associatedParentNode)) {
                propertyEvents = Iterators.map(node.getProperties().iterator(),
                    new Function1<PropertyState, Event>() {
                        @Override
                        public Event apply(PropertyState property) {
                            return generatePropertyEvent(propertyEventType, path, property);
                        }
                    });
            }
            else {
                propertyEvents = Iterators.empty();
            }

            Iterator<Event> childNodeEvents = !stopped && filter.includeChildren(jcrPath)
                    ? chain(generateChildEvents(eventType, path, node))
                    : Iterators.<Event>empty();

            return chain(nodeEvent, propertyEvents, childNodeEvents);
        }

        private Iterator<Iterator<Event>> generateChildEvents(final int eventType, final String parentPath, NodeState node) {
            return Iterators.map(node.getChildNodeEntries().iterator(),
                new Function1<ChildNodeEntry, Iterator<Event>>() {
                    @Override
                    public Iterator<Event> apply(ChildNodeEntry entry) {
                        return generateNodeEvents(eventType, parentPath, entry.getName(), entry.getNodeState());
                    }
                });
        }

    }

}
