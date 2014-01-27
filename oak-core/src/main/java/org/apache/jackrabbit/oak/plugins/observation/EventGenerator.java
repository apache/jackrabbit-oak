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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.core.AbstractTree.OAK_CHILD_ORDER;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.MoveDetector.SOURCE_PATH;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.Filters;
import org.apache.jackrabbit.oak.plugins.observation.filter.VisibleFilter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.collect.ImmutableMap;

/**
 * Generator of a traversable view of events.
 */
public class EventGenerator implements EventIterator {

    private final EventContext context;

    private final LinkedList<Event> events = newLinkedList();
    private final LinkedList<Runnable> generators = newLinkedList();

    private long position = 0;

    /**
     * Create a new instance of a {@code EventGenerator} reporting events to the
     * passed {@code listener} after filtering with the passed {@code filter}.
     *
     * @param filter filter for filtering changes
     */
    public EventGenerator(
            @Nonnull NamePathMapper namePathMapper, CommitInfo info,
            @Nonnull NodeState before, @Nonnull NodeState after,
            @Nonnull String basePath, @Nonnull EventFilter filter) {
        this.context = new EventContext(namePathMapper, info);

        filter = Filters.all(new VisibleFilter(), checkNotNull(filter));

        new EventDiff(before, after, basePath, filter).run();
    }

    private class EventDiff implements NodeStateDiff, Runnable {

        /**
         * The diff handler of the parent node, or {@code null} for the root.
         */
        private final EventDiff parent;

        /**
         * The name of this node, or the empty string for the root.
         */
        private final String name;

        /**
         * Before state, or {@code MISSING_NODE} if this node was added.
         */
        private final NodeState before;

        /**
         * After state, or {@code MISSING_NODE} if this node was removed.
         */
        private final NodeState after;

        /**
         * Filter for selecting which events to produce.
         */
        private final EventFilter filter;

        private final ImmutableTree beforeTree;
        private final ImmutableTree afterTree;

        EventDiff(NodeState before, NodeState after, String path,
                EventFilter filter) {
            String name = null;
            ImmutableTree btree = new ImmutableTree(before);
            ImmutableTree atree = new ImmutableTree(after);
            for (String element : PathUtils.elements(path)) {
                name = element;
                before = before.getChildNode(name);
                after = after.getChildNode(name);
                btree = new ImmutableTree(btree, name, before);
                atree = new ImmutableTree(atree, name, after);
            }

            this.parent = null;
            this.name = name;
            this.before = before;
            this.after = after;
            this.filter = filter;
            this.beforeTree = btree;
            this.afterTree = atree;
        }

        private EventDiff(
                EventDiff parent, EventFilter filter,
                String name, NodeState before, NodeState after) {
            this.parent = parent;
            this.name = name;
            this.before = before;
            this.after = after;
            this.filter = filter;
            this.beforeTree = new ImmutableTree(parent.beforeTree, name, before);
            this.afterTree = new ImmutableTree(parent.afterTree, name, after);
        }

        //------------------------------------------------------< Runnable >--

        @Override
        public void run() {
            if (parent != null) {
                if (before == MISSING_NODE) {
                    parent.handleAddedNode(name, after); // postponed handling of added nodes
                } else if (after == MISSING_NODE) {
                    parent.handleDeletedNode(name, before); // postponed handling of removed nodes
                }
            }

            // process changes below this node
            after.compareAgainstBaseState(before, this);
        }

        //-------------------------------------------------< NodeStateDiff >--

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (filter.includeAdd(after)) {
                events.add(new EventImpl(
                        context, PROPERTY_ADDED, afterTree, after.getName()));
            }
            return true;
        }

        @Override
        public boolean propertyChanged(
                PropertyState before, PropertyState after) {
            // check for reordering of child nodes
            if (OAK_CHILD_ORDER.equals(before.getName() &&
                    filter.includeChange(this.name, this.before, this.after))) {
                handleReorderedNodes(
                        before.getValue(NAMES), after.getValue(NAMES));
            }
            if (filter.includeChange(before, after)) {
                events.add(new EventImpl(
                        context, PROPERTY_CHANGED, afterTree, after.getName()));
            }
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (filter.includeDelete(before)) {
                events.add(new EventImpl(
                        context, PROPERTY_REMOVED, beforeTree, before.getName()));
            }
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (!addChildEventGenerator(name, MISSING_NODE, after)) {
                handleAddedNode(name, after); // not postponed
            }
            return true;
        }

        @Override
        public boolean childNodeChanged(
                String name, NodeState before, NodeState after) {
            addChildEventGenerator(name, before, after);
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (!addChildEventGenerator(name, before, MISSING_NODE)) {
                handleDeletedNode(name, before); // not postponed
            }
            return true;
        }

        //------------------------------------------------------------< private >---

        private boolean addChildEventGenerator(
                String name, NodeState before, NodeState after) {
            EventFilter childFilter = filter.create(name, before, after);
            if (childFilter != null) {
                generators.add(new EventDiff(
                        this, childFilter, name, before, after));
                return true;
            } else {
                return false;
            }
        }

        private void handleAddedNode(String name, NodeState after) {
            PropertyState sourceProperty = after.getProperty(SOURCE_PATH);
            if (sourceProperty != null) {
                String sourcePath = sourceProperty.getValue(STRING);
                if (filter.includeMove(sourcePath, name, after)) {
                    ImmutableTree tree = new ImmutableTree(afterTree, name, after);
                    Map<String, String> info = ImmutableMap.of(
                            "srcAbsPath", context.getJcrPath(sourcePath),
                            "destAbsPath", context.getJcrPath(tree.getPath()));
                    events.add(new EventImpl(context, NODE_MOVED, tree, info));
                }
            }

            if (filter.includeAdd(name, after)) {
                ImmutableTree tree = new ImmutableTree(afterTree, name, after);
                events.add(new EventImpl(context, NODE_ADDED, tree));
            }
        }

        protected void handleDeletedNode(String name, NodeState before) {
            if (filter.includeDelete(name, before)) {
                ImmutableTree tree = new ImmutableTree(beforeTree, name, before);
                events.add(new EventImpl(context, NODE_REMOVED, tree));
            }
        }

        private void handleReorderedNodes(
                Iterable<String> before, Iterable<String> after) {
            List<String> afterNames = newArrayList(after);
            List<String> beforeNames = newArrayList(before);

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
                        Map<String, String> info = ImmutableMap.of(
                                "srcChildRelPath", context.getJcrName(beforeNames.get(a)),
                                "destChildRelPath", context.getJcrName(beforeNames.get(a + 1)));
                        ImmutableTree tree = afterTree.getChild(afterName);
                        events.add(new EventImpl(context, NODE_MOVED, tree, info));
                    }
                }
            }
        }

    }

    //-----------------------------------------------------< EventIterator >--

    @Override
    public long getSize() {
        if (generators.isEmpty()) {
            return position + events.size();
        } else {
            return -1;
        }
    }

    @Override
    public long getPosition() {
        return position;
    }

    @Override
    public boolean hasNext() {
        while (events.isEmpty()) {
            if (generators.isEmpty()) {
                return false;
            } else {
                generators.removeFirst().run();
            }
        }
        return true;
    }

    @Override
    public void skip(long skipNum) {
        while (skipNum > events.size()) {
            position += events.size();
            skipNum -= events.size();
            events.clear();
            // the remove below throws NoSuchElementException if there
            // are no more generators, which is correct as then we can't
            // skip over enough events
            generators.removeFirst().run();
        }
        position += skipNum;
        events.subList(0, (int) skipNum).clear();
    }

    @Override
    public Event nextEvent() {
        if (hasNext()) {
            position++;
            return events.removeFirst();
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public Event next() {
        return nextEvent();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
