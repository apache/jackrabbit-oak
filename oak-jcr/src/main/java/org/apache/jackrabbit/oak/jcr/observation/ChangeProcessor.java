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

import java.util.Collections;
import java.util.Iterator;
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
        changeExtractor.getChanges(new EventGeneratingNodeStateDiff());
    }

    //------------------------------------------------------------< private >---

    private class EventGeneratingNodeStateDiff implements NodeStateDiff {
        private final String path;

        EventGeneratingNodeStateDiff(String path) {
            this.path = path;
        }

        public EventGeneratingNodeStateDiff() {
            this("/");
        }

        @Override
        public void propertyAdded(PropertyState after) {
            if (!stopped && filterRef.get().include(Event.PROPERTY_ADDED, path, after)) {
                Event event = generatePropertyEvent(Event.PROPERTY_ADDED, path, after);
                listener.onEvent(new EventIteratorAdapter(Collections.singleton(event)));
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (!stopped && filterRef.get().include(Event.PROPERTY_CHANGED, path, before)) {
                Event event = generatePropertyEvent(Event.PROPERTY_CHANGED, path, after);
                listener.onEvent(new EventIteratorAdapter(Collections.singleton(event)));
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            if (!stopped && filterRef.get().include(Event.PROPERTY_REMOVED, path, before)) {
                Event event = generatePropertyEvent(Event.PROPERTY_REMOVED, path, before);
                listener.onEvent(new EventIteratorAdapter(Collections.singleton(event)));
            }
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (!stopped && filterRef.get().include(Event.NODE_ADDED, path, after)) {
                Iterator<Event> events = generateNodeEvents(Event.NODE_ADDED, path, name, after);
                listener.onEvent(new EventIteratorAdapter(events));
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (!stopped && filterRef.get().include(Event.NODE_REMOVED, path, before)) {
                Iterator<Event> events = generateNodeEvents(Event.NODE_REMOVED, path, name, before);
                listener.onEvent(new EventIteratorAdapter(events));
            }
        }

        @Override
        public void childNodeChanged(String name, NodeState before, NodeState after) {
            if (!stopped && filterRef.get().includeChildren(path)) {
                changeExtractor.getChanges(before, after,
                        new EventGeneratingNodeStateDiff(PathUtils.concat(path, name)));
            }
        }
    }

    private Event generatePropertyEvent(int eventType, String path, PropertyState property) {
        String jcrPath = namePathMapper.getJcrPath(PathUtils.concat(path, property.getName()));

        // TODO support userId, identifier, info, date
        return new EventImpl(eventType, jcrPath, null, null, null, 0);
    }

    private Iterator<Event> generateNodeEvents(final int eventType, String path, String name, NodeState node) {
        final String jcrPath = namePathMapper.getJcrPath(PathUtils.concat(path, name));

        Iterator<Event> propertyEvents = Iterators.map(node.getProperties().iterator(),
                new Function1<PropertyState, Event>() {
                    int propertyEventType = eventType == Event.NODE_ADDED ? Event.PROPERTY_ADDED : Event.PROPERTY_REMOVED;

                    @Override
                    public Event apply(PropertyState property) {
                        return generatePropertyEvent(propertyEventType, jcrPath, property);
                    }
                });

        // TODO support userId, identifier, info, date
        final Event nodeEvent = new EventImpl(eventType, jcrPath, null, null, null, 0);
        Iterator<Event> events = Iterators.chain(singleton(nodeEvent), propertyEvents);
        return chain(events, chain(generateChildEvents(eventType, path, name, node)));
    }

    private Iterator<Iterator<Event>> generateChildEvents(final int eventType, final String path, final String name,
            NodeState node) {
        return Iterators.map(node.getChildNodeEntries().iterator(),
                new Function1<ChildNodeEntry, Iterator<Event>>() {
            @Override
            public Iterator<Event> apply(ChildNodeEntry entry) {
                return generateNodeEvents(eventType, path, name, entry.getNodeState());
            }
        });
    }

}
