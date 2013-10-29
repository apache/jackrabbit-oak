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
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyMap;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager.getIdentifier;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.observation.Event;
import javax.jcr.observation.EventListener;

import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.ChangeSet;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.Listener;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.state.MoveDetector;
import org.apache.jackrabbit.oak.spi.state.MoveValidator;
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

    private static final Logger log =
            LoggerFactory.getLogger(ChangeProcessor.class);

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
                ChangeSet changes = changeListener.getChanges(100);
                while (!stopping) {
                    EventFilter filter = filterRef.get();
                    // FIXME don't rely on toString for session id
                    if (changes != null &&
                            !(filter.excludeLocal() && changes.isLocal(contentSession.toString()))) {
                        String path = namePathMapper.getOakPath(filter.getPath());
                        ImmutableTree beforeTree = getTree(changes.getBeforeState(), path);
                        ImmutableTree afterTree = getTree(changes.getAfterState(), path);
                        EventGeneratingValidator events = new EventGeneratingValidator(
                                changes.getCommitInfo(), beforeTree, afterTree);
                        if (events.hasNext()) {
                            listener.onEvent(new EventIteratorAdapter(events));
                        }
                    }
                    changes = changeListener.getChanges(100);
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

    private class EventGeneratingValidator extends ForwardingIterator<Event> implements MoveValidator {
        private final String userId;
        private final String message;
        private final long timestamp;
        private final boolean external;

        private final ImmutableTree beforeTree;
        private final ImmutableTree afterTree;
        private final List<Event> events = newArrayList();
        private final List<Iterator<Event>> childEvents = newArrayList();

        private Iterator<Event> eventIterator;

        EventGeneratingValidator(CommitInfo info, ImmutableTree beforeTree, ImmutableTree afterTree) {
            if (info != null) {
                this.userId = info.getUserId();
                this.message = info.getMessage();
                this.timestamp = info.getDate();
                this.external = false;
            } else {
                this.userId = CommitInfo.OAK_UNKNOWN;
                this.message = null;
                // we can't tell exactly when external changes were committed,
                // so we just use a rough estimate like this
                this.timestamp = System.currentTimeMillis();
                this.external = true;
            }
            this.beforeTree = beforeTree;
            this.afterTree = afterTree;
        }

        private EventGeneratingValidator(EventGeneratingValidator parent, String name) {
            this.userId = parent.userId;
            this.message = parent.message;
            this.timestamp = parent.timestamp;
            this.external = parent.external;
            this.beforeTree = parent.beforeTree.getChild(name);
            this.afterTree = parent.afterTree.getChild(name);
        }

        //------------------------------------------------------------< ForwardingIterator >---

        @Override
        protected Iterator<Event> delegate() {
            try {
                if (eventIterator == null) {
                    SecureValidator.compare(beforeTree, afterTree,
                            new VisibleValidator(
                                    new MoveDetector(this, afterTree.getPath()), true, true));
                    eventIterator = concat(events.iterator(), concat(childEvents.iterator()));
                }
                return eventIterator;
            } catch (CommitFailedException e) {
                log.error("Error while extracting observation events", e);
                return Iterators.emptyIterator();
            }
        }

        //------------------------------------------------------------< Validator >---

        @Override
        public void enter(NodeState before, NodeState after) throws CommitFailedException {
        }

        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
        }

        @Override
        public void propertyAdded(PropertyState after) {
            if (filterRef.get().include(PROPERTY_ADDED, afterTree)) {
                events.add(createEvent(PROPERTY_ADDED, afterTree, after));
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (filterRef.get().include(Event.PROPERTY_CHANGED, afterTree)) {
                events.add(createEvent(Event.PROPERTY_CHANGED, afterTree, after));
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            if (filterRef.get().include(PROPERTY_REMOVED, afterTree)) {
                events.add(createEvent(PROPERTY_REMOVED, beforeTree, before));
            }
        }

        @Override
        public MoveValidator childNodeAdded(String name, NodeState after) {
            EventFilter eventFilter = filterRef.get();
            if (eventFilter.include(NODE_ADDED, afterTree)) {
                events.add(createEvent(NODE_ADDED, afterTree.getChild(name)));
            }
            if (eventFilter.includeChildren(afterTree.getPath())) {
                childEvents.add(new EventGeneratingValidator(this, name));
            }
            return null;
        }

        @Override
        public MoveValidator childNodeDeleted(String name, NodeState before) {
            EventFilter eventFilter = filterRef.get();
            if (eventFilter.include(NODE_REMOVED, beforeTree)) {
                events.add(createEvent(NODE_REMOVED, beforeTree.getChild(name)));
            }
            if (eventFilter.includeChildren(beforeTree.getPath())) {
                childEvents.add(new EventGeneratingValidator(this, name));
            }
            return null;
        }

        @Override
        public MoveValidator childNodeChanged(String name, NodeState before, NodeState after) {
            if (filterRef.get().includeChildren(afterTree.getPath())) {
                childEvents.add(new EventGeneratingValidator(this, name));
            }
            return null;
        }

        @Override
        public void move(String sourcePath, String destPath, NodeState moved)
                throws CommitFailedException {
            if (filterRef.get().include(NODE_MOVED, afterTree)) {
                events.add(createEvent(NODE_MOVED, afterTree.getChild(getName(destPath)),
                        ImmutableMap.of(
                            "srcAbsPath", namePathMapper.getJcrPath(sourcePath),
                            "destAbsPath", namePathMapper.getJcrPath(destPath))));
            }
        }

        //------------------------------------------------------------< internal >---

        private Event createEvent(int eventType, Tree tree) {
            return createEvent(eventType, tree.getPath(), getIdentifier(tree), emptyMap());
        }

        private Event createEvent(int eventType, Tree tree, Map<?, ?> info) {
            return createEvent(eventType, tree.getPath(), getIdentifier(tree), info);
        }

        private Event createEvent(int eventType, Tree parent, PropertyState property) {
            String path = PathUtils.concat(parent.getPath(), property.getName());
            return createEvent(eventType, path, getIdentifier(parent), emptyMap());
        }

        private Event createEvent(int eventType, String path, String id, Map<?, ?> info) {
            return new EventImpl(
                    eventType, namePathMapper.getJcrPath(path), userId, id,
                    info, timestamp, message, external);
        }

    }

}
