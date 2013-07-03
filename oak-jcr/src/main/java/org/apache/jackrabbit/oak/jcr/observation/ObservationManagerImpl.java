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
package org.apache.jackrabbit.oak.jcr.observation;

import static com.google.common.collect.Lists.newArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.observation.EventJournal;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.commons.iterator.EventListenerIteratorAdapter;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.Observable;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class ObservationManagerImpl implements ObservationManager {

    private static final Logger log = LoggerFactory.getLogger(ObservationManagerImpl.class);

    public static final Marker OBSERVATION =
            MarkerFactory.getMarker("observation");

    private static final Marker DEPRECATED =
            MarkerFactory.getMarker("deprecated");

    private final Map<EventListener, ChangeProcessor> processors =
            new HashMap<EventListener, ChangeProcessor>();

    private final SessionDelegate sessionDelegate;
    private final ReadOnlyNodeTypeManager ntMgr;
    private final NamePathMapper namePathMapper;
    private final Whiteboard whiteboard;

    /**
     * Create a new instance based on a {@link ContentSession} that needs to implement
     * {@link Observable}.
     *
     * @param sessionDelegate  session delegate of the session in whose context this observation manager
     *                         operates.
     * @param nodeTypeManager  node type manager for the content session
     * @param namePathMapper   name path mapper for the content session
     * @param whiteboard
     * @throws IllegalArgumentException if {@code contentSession} doesn't implement {@code Observable}.
     */
    public ObservationManagerImpl(
            SessionDelegate sessionDelegate, ReadOnlyNodeTypeManager nodeTypeManager,
            NamePathMapper namePathMapper, Whiteboard whiteboard) {

        this.sessionDelegate = sessionDelegate;
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

    @Override
    public synchronized void addEventListener(EventListener listener, int eventTypes, String absPath,
            boolean isDeep, String[] uuid, String[] nodeTypeName, boolean noLocal) throws RepositoryException {
        EventFilter filter = new EventFilter(ntMgr, eventTypes, oakPath(absPath), isDeep,
                uuid, validateNodeTypeNames(nodeTypeName), noLocal);
        ChangeProcessor processor = processors.get(listener);
        if (processor == null) {
            log.info(OBSERVATION, "Registering event listener {} with filter {}", listener, filter);
            ListenerTracker tracker = new ListenerTracker(
                    listener, eventTypes, absPath, isDeep,
                    uuid, nodeTypeName, noLocal) {
                @Override
                protected void warn(String message) {
                    log.warn(DEPRECATED, message, initStackTrace);
                }
                @Override
                protected void beforeEventDelivery() {
                    sessionDelegate.refreshAtNextAccess();
                }
            };
            processor = new ChangeProcessor(
                    sessionDelegate.getContentSession(), namePathMapper, tracker, filter);
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

    //------------------------------------------------------------< private >---

    private String oakPath(String jcrPath) {
        return namePathMapper.getOakPath(jcrPath);
    }

    /**
     * Validates the given node type names.
     *
     * @param nodeTypeNames the node type names.
     * @return the node type names as oak names.
     * @throws javax.jcr.nodetype.NoSuchNodeTypeException if one of the node type names refers to
     *                                 an non-existing node type.
     * @throws javax.jcr.RepositoryException     if an error occurs while reading from the
     *                                 node type manager.
     */
    @CheckForNull
    private String[] validateNodeTypeNames(@Nullable String[] nodeTypeNames)
            throws NoSuchNodeTypeException, RepositoryException {
        if (nodeTypeNames == null) {
            return null;
        }
        String[] oakNames = new String[nodeTypeNames.length];
        for (int i = 0; i < nodeTypeNames.length; i++) {
            ntMgr.getNodeType(nodeTypeNames[i]);
            oakNames[i] = namePathMapper.getOakName(nodeTypeNames[i]);
        }
        return oakNames;
    }

}
