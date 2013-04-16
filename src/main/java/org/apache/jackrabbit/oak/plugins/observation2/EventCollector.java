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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.LISTENERS;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.LISTENER_PATH;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.REP_OBSERVATION;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.USER_DATA;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.USER_ID;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.jcr.observation.Event;
import javax.jcr.observation.EventListener;

import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
class EventCollector implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(EventCollector.class);

    private final ObservationManagerImpl2 observationManager;
    private final EventQueueReader eventQueueReader;
    private final EventListener listener;
    private volatile boolean running;
    private ScheduledFuture<?> future;
    private String id;

    public EventCollector(ObservationManagerImpl2 observationManager, EventListener listener, EventFilter filter)
            throws CommitFailedException {
        this.observationManager = observationManager;
        this.listener = listener;
        this.eventQueueReader = new EventQueueReader(
                observationManager.getContentSession().getLatestRoot(),
                observationManager.getNamePathMapper());
        setFilterSpec(filter);
    }

    public void updateFilter(EventFilter filter) throws CommitFailedException {
        updateFilterSpec(filter);
    }

    public void setUserData(String userData) throws CommitFailedException {
        Root root = getLatestRoot();
        Tree listenerSpec = getOrCreateListenerSpec(root);
        if (userData == null) {
            listenerSpec.removeProperty(USER_DATA);
        } else {
            listenerSpec.setProperty(USER_DATA, userData);
        }
        root.commit();
    }

    /**
     * Stop this change processor if running. After returning from this methods no further
     * events will be delivered.
     * @throws IllegalStateException if not yet started or stopped already
     */
    public synchronized void stop() throws CommitFailedException {
        if (future == null) {
            throw new IllegalStateException("Change processor not started");
        }

        try {
            future.cancel(true);
            while (running) {
                wait();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            future = null;
            clearFilterSpec();
        }
    }

    /**
     * Start the change processor on the passed {@code executor}.
     * @param executor
     * @throws IllegalStateException if started already
     */
    public synchronized void start(ScheduledExecutorService executor) {
        checkArgument(future == null, "Change processor started already");
        future = executor.scheduleWithFixedDelay(this, 100, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        running = true;
        try {
            Iterator<Event> bundle = eventQueueReader.getEventBundle(getId());
            // FIXME filter by session specific access restrictions
            if (bundle != null) {
                observationManager.setHasEvents();
                listener.onEvent(new EventIteratorAdapter(bundle));
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

    //------------------------------------------------------------< private >---

    private String getId() {
        if (id == null) {
            id = UUID.randomUUID().toString();
        }
        return id;
    }

    private static Tree getOrCreate(Tree parent, String name) {
        Tree child = parent.getChild(name);
        if (child == null) {
            child = parent.addChild(name);
        }
        return child;
    }

    private Tree getOrCreateListenerSpec(Root root) {
        return getOrCreate(getOrCreate(getOrCreate(
                root.getTree('/' + JCR_SYSTEM), REP_OBSERVATION), LISTENERS), getId());
    }

    private Root getLatestRoot() {
        return observationManager.getContentSession().getLatestRoot();
    }

    private void setFilterSpec(EventFilter filter) throws CommitFailedException {
        Root root = getLatestRoot();
        Tree listenerSpec = getOrCreateListenerSpec(root);
        String userId = observationManager.getContentSession().getAuthInfo().getUserID();
        listenerSpec.setProperty(USER_ID, userId);
        filter.persist(listenerSpec);
        root.commit();
    }

    private void updateFilterSpec(EventFilter filter) throws CommitFailedException {
        Root root = getLatestRoot();
        Tree listenerSpec = getOrCreateListenerSpec(root);
        filter.persist(listenerSpec);
        root.commit();
    }

    private void clearFilterSpec() throws CommitFailedException {
        Root root = getLatestRoot();
        Tree listenerSpec = root.getTree(LISTENER_PATH + '/' + getId());
        if  (listenerSpec != null) {
            listenerSpec.remove();
            root.commit();
        }
    }

}
