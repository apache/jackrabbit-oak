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
package org.apache.jackrabbit.oak.jcr;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventJournal;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.EventListenerIteratorAdapter;
import org.apache.jackrabbit.oak.api.ChangeExtractor;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.util.LazyValue;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;

import static org.apache.jackrabbit.commons.iterator.LazyIteratorChain.chain;
import static org.apache.jackrabbit.oak.util.Iterators.singleton;

public class ObservationManagerImpl implements ObservationManager {
    private final SessionDelegate sessionDelegate;
    private final Map<EventListener, ChangeProcessor> processors =
            new HashMap<EventListener, ChangeProcessor>();

    private final LazyValue<Timer> timer;

    public ObservationManagerImpl(SessionDelegate sessionDelegate, LazyValue<Timer> timer) {
        this.sessionDelegate = sessionDelegate;
        this.timer = timer;
    }

    public void dispose() {
        for (ChangeProcessor processor : processors.values()) {
            processor.stop();
        }
        timer.get().cancel();
    }

    @Override
    public void addEventListener(EventListener listener, int eventTypes, String absPath,
            boolean isDeep, String[] uuid, String[] nodeTypeName, boolean noLocal)
            throws RepositoryException {

        ChangeProcessor processor = processors.get(listener);
        if (processor == null) {
            ChangeExtractor extractor = sessionDelegate.getChangeExtractor();
            ChangeFilter filter = new ChangeFilter(eventTypes, absPath, isDeep, uuid, nodeTypeName, noLocal);
            ChangeProcessor changeProcessor = new ChangeProcessor(sessionDelegate.getNamePathMapper(), extractor,
                    listener, filter);
            processors.put(listener, changeProcessor);
            timer.get().schedule(changeProcessor, 0, 1000);
        }
        else {
            ChangeFilter filter = new ChangeFilter(eventTypes, absPath, isDeep, uuid, nodeTypeName, noLocal);
            processor.setFilter(filter);
        }

        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void removeEventListener(EventListener listener) throws RepositoryException {
        ChangeProcessor processor = processors.remove(listener);
        if (processor != null) {
            processor.stop();
        }
    }

    @Override
    public EventListenerIterator getRegisteredEventListeners() throws RepositoryException {
        return new EventListenerIteratorAdapter(processors.keySet());
    }

    @Override
    public void setUserData(String userData) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("User data not supported");
    }

    @Override
    public EventJournal getEventJournal() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public EventJournal getEventJournal(int eventTypes, String absPath, boolean isDeep, String[] uuid, String[]
            nodeTypeName) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    //------------------------------------------------------------< private >---

    private static class ChangeProcessor extends TimerTask {
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

        private class EventGeneratingNodeStateDiff implements NodeStateDiff {
            private final String path;

            private EventGeneratingNodeStateDiff(String path) {
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

    private static class ChangeFilter {
        public ChangeFilter(int eventTypes, String absPath, boolean deep, String[] uuid, String[] nodeTypeName,
                boolean noLocal) {
            // todo implement ChangeFilter
        }

        public boolean include(int eventType, String path, PropertyState propertyState) {
            return true; // todo implement include
        }

        public boolean include(int eventType, String path, NodeState nodeState) {
            return true; // todo implement include
        }

        public boolean includeChildren(String path) {
            return true; // todo implement includeChildren
        }
    }

    private static class EventImpl implements Event {
        private final int type;
        private final String path;
        private final String userID;
        private final String identifier;
        private final Map<?, ?> info;
        private final long date;

        EventImpl(int type, String path, String userID, String identifier, Map<?, ?> info, long date) {
            this.type = type;
            this.path = path;
            this.userID = userID;
            this.identifier = identifier;
            this.info = info;
            this.date = date;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public String getPath() throws RepositoryException {
            return path;
        }

        @Override
        public String getUserID() {
            return userID;
        }

        @Override
        public String getIdentifier() throws RepositoryException {
            return identifier;
        }

        @Override
        public Map<?, ?> getInfo() throws RepositoryException {
            return info;
        }

        @Override
        public String getUserData() throws RepositoryException {
            throw new UnsupportedRepositoryOperationException("User data not supported");
        }

        @Override
        public long getDate() throws RepositoryException {
            return date;
        }

        @Override
        public final boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            EventImpl that = (EventImpl) other;
            return date == that.date && type == that.type &&
                    (identifier == null ? that.identifier == null : identifier.equals(that.identifier)) &&
                    (info == null ? that.info == null : info.equals(that.info)) &&
                    (path == null ? that.path == null : path.equals(that.path)) &&
                    (userID == null ? that.userID == null : userID.equals(that.userID));

        }

        @Override
        public final int hashCode() {
            int result = type;
            result = 31 * result + (path == null ? 0 : path.hashCode());
            result = 31 * result + (userID == null ? 0 : userID.hashCode());
            result = 31 * result + (identifier == null ? 0 : identifier.hashCode());
            result = 31 * result + (info == null ? 0 : info.hashCode());
            result = 31 * result + (int) (date ^ (date >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "EventImpl{" +
                    "type=" + type +
                    ", path='" + path + '\'' +
                    ", userID='" + userID + '\'' +
                    ", identifier='" + identifier + '\'' +
                    ", info=" + info +
                    ", date=" + date +
                    '}';
        }
    }
}
