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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.observation.EventListener;

import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.ChangeSet;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.Listener;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code ChangeProcessor} generates observation {@link javax.jcr.observation.Event}s
 * based on a {@link EventFilter} and delivers them to an {@link javax.jcr.observation.EventListener}.
 * <p>
 * After instantiation a {@code ChangeProcessor} must be started in order for its
 * {@link ListenerThread listener thread's} run methods to be regularly
 * executed and stopped in order to not execute its run method anymore.
 */
public class ChangeProcessor {
    private static final Logger log = LoggerFactory.getLogger(ChangeProcessor.class);

    private final ContentSession contentSession;
    private final NamePathMapper namePathMapper;
    private final AtomicReference<EventFilter> filterRef;

    private final ListenerTracker tracker;
    private final EventListener listener;

    /**
     * Background thread used to wait for and deliver events.
     */
    private ListenerThread thread = null;

    private volatile boolean stopping = false;

    public ChangeProcessor(
            ContentSession contentSession, NamePathMapper namePathMapper,
            ListenerTracker tracker, EventFilter filter) {
        checkArgument(contentSession instanceof Observable);
        this.contentSession = contentSession;
        this.namePathMapper = namePathMapper;
        this.tracker = tracker;
        this.listener = tracker.getTrackedListener();
        filterRef = new AtomicReference<EventFilter>(filter);
    }

    /**
     * Set the filter for the events this change processor will generate.
     * @param filter
     */
    public void setFilter(EventFilter filter) {
        filterRef.set(filter);
    }

    /**
     * Start this change processor
     * @param whiteboard  the whiteboard instance to used for scheduling individual
     *                    runs of this change processor.
     * @throws IllegalStateException if started already
     */
    public synchronized void start(Whiteboard whiteboard) {
        checkState(thread == null, "Change processor started already");

        thread = new ListenerThread(whiteboard);
        thread.setDaemon(true);
        thread.setPriority(Thread.MIN_PRIORITY);
        thread.start();
    }

    /**
     * Stop this change processor if running. After returning from this methods no further
     * events will be delivered.
     * @throws IllegalStateException if not yet started or stopped already
     */
    public synchronized void stop() {
        checkState(thread != null, "Change processor not started");
        checkState(!stopping, "Change processor already stopped");

        stopping = true;
        if (Thread.currentThread() != thread) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                log.warn("Interruption while waiting for the observation thread to terminate", e);
            }
        }
    }

    //------------------------------------------------------------< private >---

    private class ListenerThread extends Thread {

        private final Listener changeListener =
                ((Observable) contentSession).newListener();

        private final Registration mbean;

        ListenerThread(Whiteboard whiteboard) {
            mbean = WhiteboardUtils.registerMBean(
                    whiteboard, EventListenerMBean.class,
                    tracker.getListenerMBean(), "EventListener",
                    tracker.toString());
        }

        @Override
        public void run() {
            try {
                while (!stopping) {
                    ChangeSet changes = changeListener.getChanges(100);
                    EventFilter filter = filterRef.get();
                    // FIXME don't rely on toString for session id
                    if (changes != null &&
                            filter.includeSessionLocal(changes.isLocal(contentSession.toString())) &&
                            filter.includeClusterExternal(changes.getCommitInfo() == null)) {
                        String path = namePathMapper.getOakPath(filter.getPath());
                        ImmutableTree beforeTree = getTree(changes.getBeforeState(), path);
                        ImmutableTree afterTree = getTree(changes.getAfterState(), path);
                        EventGenerator events = new EventGenerator(changes.getCommitInfo(),
                                beforeTree, afterTree, filter, namePathMapper);
                        if (events.hasNext()) {
                            listener.onEvent(new EventIteratorAdapter(events));
                        }
                    }
                }
            } catch (Exception e) {
                log.debug("Error while dispatching observation events", e);
            } finally {
                mbean.unregister();
                changeListener.dispose();
            }
        }

        private ImmutableTree getTree(NodeState nodeState, String path) {
            return new ImmutableRoot(nodeState).getTree(path);
        }
    }

}
