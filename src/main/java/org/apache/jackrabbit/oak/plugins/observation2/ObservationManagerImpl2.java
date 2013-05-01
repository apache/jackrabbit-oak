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

package org.apache.jackrabbit.oak.plugins.observation2;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.observation.EventJournal;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;
import javax.jcr.observation.ObservationManager;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.commons.iterator.EventListenerIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

/**
 * TODO document
 */
public class ObservationManagerImpl2 implements ObservationManager {
    private static final Logger log = LoggerFactory.getLogger(ObservationManagerImpl2.class);

    private final ContentSession contentSession;
    private final NamePathMapper namePathMapper;
    private final ScheduledExecutorService executor;
    private final ReadOnlyNodeTypeManager ntMgr;
    private final Map<EventListener, EventCollector> collectors = new HashMap<EventListener, EventCollector>();
    private final AtomicBoolean hasEvents = new AtomicBoolean(false);

    public ObservationManagerImpl2(Root root, NamePathMapper namePathMapper, ScheduledExecutorService executor) {
        this.contentSession = checkNotNull(root).getContentSession();
        this.namePathMapper = checkNotNull(namePathMapper);
        this.executor = checkNotNull(executor);
        this.ntMgr = ReadOnlyNodeTypeManager.getInstance(root, namePathMapper);
        clearEventQueueOnRestart();
    }

    // FIXME: we need a better way to communicate BUNDLE_ID across.
    // Preferably through persisting it to th repository
    private void clearEventQueueOnRestart() {
        if (EventQueueWriterProvider.BUNDLE_ID.get() == 0) {
            try {
                Root root = contentSession.getLatestRoot();
                Tree events = root.getTreeOrNull(ObservationConstants.EVENTS_PATH);
                if (events != null) {
                    events.remove();
                    root.commit();
                }
            }
            catch (CommitFailedException e) {
                log.warn("Error clearing event queue after restart", e);
            }
        }
    }

    private static void stop(EventCollector collector) {
        try {
            collector.stop();
        }
        catch (CommitFailedException e) {
            log.warn("Error while stopping event collector", e);
        }
    }

    public synchronized void dispose() {
        for (EventCollector collector : collectors.values()) {
            stop(collector);
        }
        collectors.clear();
    }

    /**
     * Determine whether events have been generated since the time this method has been called.
     * @return  {@code true} if this {@code ObservationManager} instance has generated events
     *          since the last time this method has been called, {@code false} otherwise.
     */
    public boolean hasEvents() {
        return hasEvents.getAndSet(false);
    }

    /**
     * Validates the given node type names.
     *
     * @param nodeTypeNames the node type names.
     * @return the node type names as oak names.
     * @throws javax.jcr.nodetype.NoSuchNodeTypeException if one of the node type
     *         names refers to an non-existing node type.
     * @throws javax.jcr.RepositoryException if an error occurs while reading from
     *         the node type manager.
     */
    @CheckForNull
    private String[] getOakTypes(@Nullable String[] nodeTypeNames) throws RepositoryException {
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

    @Override
    public synchronized void addEventListener(EventListener listener, int eventTypes, String absPath,
            boolean isDeep, String[] uuid, String[] nodeTypeName, boolean noLocal) throws RepositoryException {

        String oakPath = namePathMapper.getOakPath(absPath);
        if (oakPath == null) {
            throw new RepositoryException("Invalid path: " + absPath);
        }

        String[] oakTypes = getOakTypes(nodeTypeName);
        EventFilter filter = new EventFilter(eventTypes, oakPath, isDeep, uuid, oakTypes, noLocal);
        EventCollector collector = collectors.get(listener);
        try {
            if (collector == null) {
                log.error(MarkerFactory.getMarker("observation"),
                        "Registering event listener {} with filter {}", listener, filter);
                collector = new EventCollector(this, listener, filter);
                collectors.put(listener, collector);
                collector.start(executor);
            } else {
                log.debug(MarkerFactory.getMarker("observation"),
                        "Changing event listener {} to filter {}", listener, filter);
                collector.updateFilter(filter);
            }
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public synchronized void removeEventListener(EventListener listener) {
        EventCollector collector = collectors.remove(listener);

        if (collector != null) {
            stop(collector);
        }
    }

    @Override
    public synchronized EventListenerIterator getRegisteredEventListeners() throws RepositoryException {
        return new EventListenerIteratorAdapter(ImmutableSet.copyOf(collectors.keySet()));
    }

    @Override
    public synchronized void setUserData(String userData) throws RepositoryException {
        try {
            for (EventCollector collector : collectors.values()) {
                collector.setUserData(userData);
            }
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
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

    void setHasEvents() {
        hasEvents.set(true);
    }

    ContentSession getContentSession() {
        return contentSession;
    }

    NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }
}
