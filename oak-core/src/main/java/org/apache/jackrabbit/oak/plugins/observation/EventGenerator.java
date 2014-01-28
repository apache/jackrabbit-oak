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
import static com.google.common.collect.Lists.newLinkedList;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.core.AbstractTree.OAK_CHILD_ORDER;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.MoveDetector.SOURCE_PATH;

import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.observation.handler.ChangeHandler;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * Generator of a traversable view of events.
 */
public class EventGenerator {

    private final LinkedList<Runnable> continuations = newLinkedList();

    /**
     * Create a new instance of a {@code EventGenerator} reporting events to the
     * passed {@code listener} after filtering with the passed {@code filter}.
     */
    public EventGenerator(
            @Nonnull NodeState before, @Nonnull NodeState after,
            @Nonnull ChangeHandler handler) {
        continuations.add(new DiffContinuation(handler, before, after));
    }

    public boolean isComplete() {
        return continuations.isEmpty();
    }

    public void generate() {
        if (!continuations.isEmpty()) {
            continuations.removeFirst().run();
        }
    }

    private class DiffContinuation implements NodeStateDiff, Runnable {

        /**
         * The diff handler of the parent node, or {@code null} for the root.
         */
        private final DiffContinuation parent;

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
         * Filtered handler of detected content changes.
         */
        private final ChangeHandler handler;

        DiffContinuation(ChangeHandler handler, NodeState before, NodeState after) {
            this.parent = null;
            this.name = null;
            this.before = before;
            this.after = after;
            this.handler = handler;
        }

        private DiffContinuation(
                DiffContinuation parent, ChangeHandler handler,
                String name, NodeState before, NodeState after) {
            this.parent = parent;
            this.name = name;
            this.before = before;
            this.after = after;
            this.handler = handler;
        }

        //------------------------------------------------------< Runnable >--

        @Override
        public void run() {
            if (parent != null) {
                if (before == MISSING_NODE) {
                    // postponed handling of added nodes
                    parent.handleAddedNode(name, after);
                } else if (after == MISSING_NODE) {
                    // postponed handling of removed nodes
                    parent.handleDeletedNode(name, before);
                }
            }

            // process changes below this node
            after.compareAgainstBaseState(before, this);
        }

        //-------------------------------------------------< NodeStateDiff >--

        @Override
        public boolean propertyAdded(PropertyState after) {
            handler.propertyAdded(after);
            return true;
        }

        @Override
        public boolean propertyChanged(
                PropertyState before, PropertyState after) {
            // check for reordering of child nodes
            if (OAK_CHILD_ORDER.equals(before.getName())) {
                handleReorderedNodes(
                        before.getValue(NAMES), after.getValue(NAMES));
            }
            handler.propertyChanged(before, after);
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            handler.propertyDeleted(before);
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
            ChangeHandler h = handler.getChildHandler(name, before, after);
            if (h != null) {
                continuations.add(new DiffContinuation(this, h, name, before, after));
                return true;
            } else {
                return false;
            }
        }

        private void handleAddedNode(String name, NodeState after) {
            PropertyState sourceProperty = after.getProperty(SOURCE_PATH);
            if (sourceProperty != null) {
                String sourcePath = sourceProperty.getValue(STRING);
                handler.nodeMoved(sourcePath, name, after);
            }

            handler.nodeAdded(name, after);
        }

        protected void handleDeletedNode(String name, NodeState before) {
            handler.nodeDeleted(name, before);
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
                        String destName = beforeNames.get(a + 1);
                        NodeState afterChild = this.after.getChildNode(afterName);
                        handler.nodeReordered(destName, afterName, afterChild);
                    }
                }
            }
        }

    }

}
