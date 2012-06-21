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

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.RepositoryException;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventJournal;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.commons.iterator.EventListenerIteratorAdapter;
import org.apache.jackrabbit.oak.api.ChangeExtractor;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

public class ObservationManagerImpl implements ObservationManager {
    private final SessionDelegate sessionDelegate;
    private final Map<EventListener, ChangeProcessor> processors =
            new HashMap<EventListener, ChangeProcessor>();

    private final Timer timer = new Timer("ObservationManager", true);

    public ObservationManagerImpl(SessionDelegate sessionDelegate) {
        this.sessionDelegate = sessionDelegate;
    }

    public void dispose() {
        for (ChangeProcessor processor : processors.values()) {
            processor.stop();
        }
        timer.cancel();
    }

    @Override
    public void addEventListener(EventListener listener, int eventTypes, String absPath,
            boolean isDeep, String[] uuid, String[] nodeTypeName, boolean noLocal)
            throws RepositoryException {

        ChangeProcessor processor = processors.get(listener);
        if (processor == null) {
            ChangeExtractor extractor = sessionDelegate.getChangeExtractor();
            ChangeFilter filter = new ChangeFilter(eventTypes, absPath, isDeep, uuid, nodeTypeName, noLocal);
            ChangeProcessor changeProcessor = new ChangeProcessor(extractor, listener, filter);
            processors.put(listener, changeProcessor);
            timer.schedule(changeProcessor, 0, 1000);
        }
        else {
            ChangeFilter filter = new ChangeFilter(eventTypes, absPath, isDeep, uuid, nodeTypeName, noLocal);
            processor.setFilter(filter);
        }
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
        // todo implement setUserData
    }

    @Override
    public EventJournal getEventJournal() throws RepositoryException {
        return null; // todo implement getEventJournal
    }

    @Override
    public EventJournal getEventJournal(int eventTypes, String absPath, boolean isDeep, String[] uuid, String[]
            nodeTypeName) throws RepositoryException {
        return null; // todo implement getEventJournal
    }

    //------------------------------------------------------------< private >---

    private static class ChangeProcessor extends TimerTask {
        private final ChangeExtractor changeExtractor;
        private final EventListener listener;
        private final AtomicReference<ChangeFilter> filterRef;

        private volatile boolean stopped;

        public ChangeProcessor(ChangeExtractor changeExtractor, EventListener listener, ChangeFilter filter) {
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
                this("");
            }

            @Override
            public void propertyAdded(PropertyState after) {
                if (!stopped && filterRef.get().include(Event.PROPERTY_ADDED, path, after)) {
                    // todo implement propertyAdded
//                    listener.onEvent(null);
                }
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                if (!stopped && filterRef.get().include(Event.PROPERTY_CHANGED, path, before)) {
                    // todo implement propertyChanged
//                    listener.onEvent(null);
                }
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                if (!stopped && filterRef.get().include(Event.PROPERTY_REMOVED, path, before)) {
                    // todo implement propertyDeleted
//                    listener.onEvent(null);
                }
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                if (!stopped && filterRef.get().include(Event.NODE_ADDED, path, after)) {
                    // todo implement childNodeAdded
//                    listener.onEvent(null);
                }
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                if (!stopped && filterRef.get().include(Event.NODE_REMOVED, path, before)) {
                    // todo implement childNodeDeleted
//                    listener.onEvent(null);
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
}
