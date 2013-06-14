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

import static com.google.common.collect.Lists.newArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.observation.EventJournal;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;
import javax.jcr.observation.ObservationManager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.commons.iterator.EventListenerIteratorAdapter;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.Listener;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class ObservationManagerImpl implements ObservationManager {
    private static final Logger log = LoggerFactory.getLogger(ObservationManagerImpl.class);
    public static final Marker OBSERVATION = MarkerFactory.getMarker("observation");

    private final Map<EventListener, ChangeProcessor> processors = new HashMap<EventListener, ChangeProcessor>();
    private final AtomicBoolean hasEvents = new AtomicBoolean(false);
    private final ContentSession contentSession;
    private final ReadOnlyNodeTypeManager ntMgr;
    private final NamePathMapper namePathMapper;
    private final Whiteboard whiteboard;

    public ObservationManagerImpl(ContentSession contentSession, ReadOnlyNodeTypeManager nodeTypeManager,
            NamePathMapper namePathMapper, Whiteboard whiteboard) {

        Preconditions.checkArgument(contentSession instanceof Observable);

        this.contentSession = contentSession;
        this.ntMgr = nodeTypeManager;
        this.namePathMapper = namePathMapper;
        this.whiteboard = whiteboard;
    }

    public void dispose() {
        List<ChangeProcessor> toBeStopped;

        synchronized (this) {
            toBeStopped = newArrayList(processors.values());
            processors.clear();
        }

        for (ChangeProcessor processor : toBeStopped) {
            processor.stop();
        }
    }

    /**
     * Determine whether events have been generated since the time this method has been called.
     * @return  {@code true} if this {@code ObservationManager} instance has generated events
     *          since the last time this method has been called, {@code false} otherwise.
     */
    public boolean hasEvents() {
        return hasEvents.getAndSet(false);
    }

    @Override
    public synchronized void addEventListener(EventListener listener, int eventTypes, String absPath,
            boolean isDeep, String[] uuid, String[] nodeTypeName, boolean noLocal) throws RepositoryException {
        EventFilter filter = new EventFilter(ntMgr, namePathMapper, eventTypes,
                absPath, isDeep, uuid, nodeTypeName, noLocal);
        ChangeProcessor processor = processors.get(listener);
        if (processor == null) {
            log.error(OBSERVATION, "Registering event listener {} with filter {}", listener, filter);
            processor = new ChangeProcessor(this, listener, filter);
            processors.put(listener, processor);
            processor.start(whiteboard);
        } else {
            log.debug(OBSERVATION, "Changing event listener {} to filter {}", listener, filter);
            processor.setFilter(filter);
        }
    }

    @Override
    public void removeEventListener(EventListener listener) {
        ChangeProcessor processor;
        synchronized (this) {
            processor = processors.remove(listener);
        }
        if (processor != null) {
            processor.stop(); // needs to happen outside synchronization
        }
    }

    @Override
    public EventListenerIterator getRegisteredEventListeners() throws RepositoryException {
        return new EventListenerIteratorAdapter(processors.keySet());
    }

    @Override
    public void setUserData(String userData) throws RepositoryException {
        for (ChangeProcessor processor : processors.values()) {
            processor.setUserData(userData);
        }
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

    //------------------------------------------------------------< internal >---

    NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    void setHasEvents() {
        hasEvents.set(true);
    }

    Listener newChangeListener() {
        return ((Observable) contentSession).newListener();
    }

    public ContentSession getContentSession() {
        return contentSession;
    }
}
