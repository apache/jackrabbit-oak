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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.observation.EventJournal;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;
import javax.jcr.observation.ObservationManager;

import com.google.common.base.Preconditions;
import org.apache.jackrabbit.commons.iterator.EventListenerIteratorAdapter;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.observation.ChangeExtractor;

/**
 * TODO document
 */
public class ObservationManagerImpl implements ObservationManager {
    private final RootImpl root;
    private final NamePathMapper namePathMapper;
    private final ScheduledExecutorService executor;
    private final Map<EventListener, ChangeProcessor> processors = new HashMap<EventListener, ChangeProcessor>();
    private final AtomicBoolean hasEvents = new AtomicBoolean(false);
    private final ReadOnlyNodeTypeManager ntMgr;

    public ObservationManagerImpl(Root root, NamePathMapper namePathMapper, ScheduledExecutorService executor) {
        Preconditions.checkArgument(root instanceof RootImpl, "root must be of actual type RootImpl");
        this.root = ((RootImpl) root);
        this.namePathMapper = namePathMapper;
        this.executor = executor;
        this.ntMgr = new NTMgr();
    }

    public synchronized void dispose() {
        for (ChangeProcessor processor : processors.values()) {
            processor.stop();
        }
        processors.clear();
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
        ChangeFilter filter = new ChangeFilter(ntMgr, namePathMapper, eventTypes,
                absPath, isDeep, uuid, nodeTypeName, noLocal);
        ChangeProcessor processor = processors.get(listener);
        if (processor == null) {
            processor = new ChangeProcessor(this, listener, filter);
            processors.put(listener, processor);
            processor.start(executor);
        } else {
            processor.setFilter(filter);
        }
    }

    @Override
    public synchronized void removeEventListener(EventListener listener) {
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

    //------------------------------------------------------------< internal >---

    NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    ChangeExtractor getChangeExtractor() {
        return root.getChangeExtractor();
    }

    void setHasEvents() {
        hasEvents.set(true);
    }

    private final class NTMgr extends ReadOnlyNodeTypeManager {

        @Override
        protected Tree getTypes() {
            return root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
        }

        @Nonnull
        @Override
        protected NamePathMapper getNamePathMapper() {
            return namePathMapper;
        }
    }
}
