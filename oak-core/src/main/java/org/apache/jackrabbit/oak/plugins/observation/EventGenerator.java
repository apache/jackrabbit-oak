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
            @Nonnull ImmutableTree before, @Nonnull ImmutableTree after,
            @Nonnull EventFilter filter) {
        this.context = new EventContext(namePathMapper, info);

        filter = Filters.all(new VisibleFilter(), checkNotNull(filter));
        new EventDiff(before, after, filter).run();
    }

    private class EventDiff implements NodeStateDiff, Runnable {

        private final EventFilter filter;
        private final NodeState before;
        private final NodeState after;
        private final ImmutableTree beforeTree;
        private final ImmutableTree afterTree;

        EventDiff(ImmutableTree before, ImmutableTree after,
                EventFilter filter) {
            this.before = before.getNodeState();
            this.after = after.getNodeState();
            this.filter = filter;
            this.beforeTree = before;
            this.afterTree = after;
        }

        public EventDiff(
                EventDiff parent, String name,
                NodeState before, NodeState after, EventFilter filter) {
            this.before = before;
            this.after = after;
            this.filter = filter;
            this.beforeTree = new ImmutableTree(parent.beforeTree, name, before);
            this.afterTree = new ImmutableTree(parent.afterTree, name, after);
        }

        //------------------------------------------------------< Runnable >--

        @Override
        public void run() {
            after.compareAgainstBaseState(before, this);
        }

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
            PropertyState sourceProperty = after.getProperty(SOURCE_PATH);
            if (sourceProperty != null) {
                String sourcePath = sourceProperty.getValue(STRING);
                if (filter.includeMove(sourcePath, name, after)) {
                    String destPath = PathUtils.concat(afterTree.getPath(), name);
                    Map<String, String> info = ImmutableMap.of(
                            "srcAbsPath", context.getJcrPath(sourcePath),
                            "destAbsPath", context.getJcrPath(destPath));
                    ImmutableTree tree = new ImmutableTree(afterTree, name, after);
                    events.add(new EventImpl(context, NODE_MOVED, tree, info));
                }
            }
            if (filter.includeAdd(name, after)) {
                ImmutableTree tree = new ImmutableTree(afterTree, name, after);
                events.add(new EventImpl(context, NODE_ADDED, tree));
            }
            addChildGenerator(name, MISSING_NODE, after);
            return true;
        }

        @Override
        public boolean childNodeChanged(
                String name, NodeState before, NodeState after) {
            if (filter.includeChange(name, before, after)) {
                detectReorder(name, before, after);
            }
            addChildGenerator(name, before, after);
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (filter.includeDelete(name, before)) {
                ImmutableTree tree = new ImmutableTree(beforeTree, name, before);
                events.add(new EventImpl(context, NODE_REMOVED, tree));
            }
            addChildGenerator(name, before, MISSING_NODE);
            return true;
        }

        //------------------------------------------------------------< private >---

        private void detectReorder(String name, NodeState before, NodeState after) {
            List<String> afterNames = newArrayList(after.getNames(OAK_CHILD_ORDER));
            List<String> beforeNames = newArrayList(before.getNames(OAK_CHILD_ORDER));

            afterNames.retainAll(beforeNames);
            beforeNames.retainAll(afterNames);

            // Selection sort beforeNames into afterNames recording the swaps as we go
            ImmutableTree parent = new ImmutableTree(afterTree, name, after);
            for (int a = 0; a < afterNames.size(); a++) {
                String afterName = afterNames.get(a);
                for (int b = a; b < beforeNames.size(); b++) {
                    String beforeName = beforeNames.get(b);
                    if (a != b && beforeName.equals(afterName)) {
                        beforeNames.set(b, beforeNames.get(a));
                        beforeNames.set(a, beforeName);
                        Map<String, String> info = ImmutableMap.of(
                                "srcChildRelPath", beforeNames.get(a),
                                "destChildRelPath", beforeNames.get(a + 1));
                        ImmutableTree tree = parent.getChild(afterName);
                        events.add(new EventImpl(context, NODE_MOVED, tree, info));
                    }
                }
            }
        }

        /**
         * Factory method for creating {@code EventGenerator} instances of child nodes.
         * @param name  name of the child node
         * @param before  before state of the child node
         * @param after  after state of the child node
         */
        private void addChildGenerator(
                String name, NodeState before, NodeState after) {
            EventFilter childFilter = filter.create(name, before, after);
            if (childFilter != null) {
                generators.add(
                        new EventDiff(this, name, before, after, childFilter));
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
