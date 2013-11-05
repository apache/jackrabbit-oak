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

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyMap;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.core.AbstractTree.OAK_CHILD_ORDER;
import static org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager.getIdentifier;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jcr.observation.Event;

import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.state.MoveDetector;
import org.apache.jackrabbit.oak.spi.state.MoveValidator;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
class EventGenerator extends ForwardingIterator<Event> implements MoveValidator {
    private static final Logger log = LoggerFactory.getLogger(EventGenerator.class);

    private final String userId;
    private final String message;
    private final long timestamp;
    private final boolean external;

    private final ImmutableTree beforeTree;
    private final ImmutableTree afterTree;
    private final EventFilter filter;
    private final NamePathMapper namePathMapper;

    private final List<Event> events = newArrayList();
    private final List<Iterator<Event>> childEvents = newArrayList();

    private Iterator<Event> eventIterator;

    EventGenerator(CommitInfo info, ImmutableTree beforeTree, ImmutableTree afterTree,
            EventFilter filter, NamePathMapper namePathMapper) {
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
        this.filter = filter;
        this.namePathMapper = namePathMapper;
    }

    EventGenerator(EventGenerator parent, String name) {
        this.userId = parent.userId;
        this.message = parent.message;
        this.timestamp = parent.timestamp;
        this.external = parent.external;
        this.beforeTree = parent.beforeTree.getChild(name);
        this.afterTree = parent.afterTree.getChild(name);
        this.filter = parent.filter;
        this.namePathMapper = parent.namePathMapper;
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
        if (filter.include(PROPERTY_ADDED, afterTree)) {
            events.add(createEvent(PROPERTY_ADDED, afterTree, after));
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        if (filter.include(Event.PROPERTY_CHANGED, afterTree)) {
            events.add(createEvent(Event.PROPERTY_CHANGED, afterTree, after));
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        if (filter.include(PROPERTY_REMOVED, afterTree)) {
            events.add(createEvent(PROPERTY_REMOVED, beforeTree, before));
        }
    }

    @Override
    public MoveValidator childNodeAdded(String name, NodeState after) {
        if (filter.include(NODE_ADDED, afterTree)) {
            events.add(createEvent(NODE_ADDED, afterTree.getChild(name)));
        }
        if (filter.includeChildren(afterTree.getPath())) {
            childEvents.add(new EventGenerator(this, name));
        }
        return null;
    }

    @Override
    public MoveValidator childNodeDeleted(String name, NodeState before) {
        if (filter.include(NODE_REMOVED, beforeTree)) {
            events.add(createEvent(NODE_REMOVED, beforeTree.getChild(name)));
        }
        if (filter.includeChildren(beforeTree.getPath())) {
            childEvents.add(new EventGenerator(this, name));
        }
        return null;
    }

    @Override
    public MoveValidator childNodeChanged(String name, NodeState before, NodeState after) {
        if (filter.include(NODE_MOVED, afterTree)) {
            detectReorder(name, before, after);
        }
        if (filter.includeChildren(afterTree.getPath())) {
            childEvents.add(new EventGenerator(this, name));
        }
        return null;
    }

    private void detectReorder(String name, NodeState before, NodeState after) {
        PropertyState afterOrder = after.getProperty(OAK_CHILD_ORDER);
        PropertyState beforeOrder = before.getProperty(OAK_CHILD_ORDER);
        if (afterOrder == null || beforeOrder == null) {
            return;
        }

        List<String> afterNames = getNames(afterOrder);
        List<String> beforeNames = getNames(beforeOrder);

        afterNames.retainAll(beforeNames);
        beforeNames.retainAll(afterNames);

        // Selection sort beforeNames into afterNames recording the swaps as we go
        for (int a = 0; a < afterNames.size(); a++) {
            String afterName = afterNames.get(a);
            for (int b = a; b < beforeNames.size(); b++) {
                String beforeName = beforeNames.get(b);
                if (a != b && beforeName.equals(afterName)) {
                    beforeNames.set(b, beforeNames.get(a));
                    beforeNames.set(a, beforeName);
                    events.add(createEvent(NODE_MOVED, afterTree.getChild(name).getChild(afterName),
                            ImmutableMap.of(
                                    "srcChildRelPath", beforeNames.get(a),
                                    "destChildRelPath", beforeNames.get(a + 1))));
                }
            }
        }
    }

    private static List<String> getNames(PropertyState propertyState) {
        List<String> names = Lists.newArrayList();
        for (int k = 0; k < propertyState.count(); k++) {
            names.add(propertyState.getValue(STRING, k));
        }
        return names;
    }

    @Override
    public void move(String sourcePath, String destPath, NodeState moved)
            throws CommitFailedException {
        if (filter.include(NODE_MOVED, afterTree)) {
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
