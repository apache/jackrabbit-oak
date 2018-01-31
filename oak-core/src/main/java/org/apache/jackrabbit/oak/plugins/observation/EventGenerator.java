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
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.MoveDetector.SOURCE_PATH;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.slf4j.LoggerFactory;

/**
 * Continuation-based content diff implementation that generates
 * {@link EventHandler} callbacks by recursing down a content diff
 * in a way that guarantees that only a finite number of callbacks
 * will be made during a {@link #generate()} method call, regardless
 * of how large or complex the content diff is.
 * <p>
 * A simple usage pattern would look like this:
 * <pre>
 * EventGenerator generator = new EventGenerator(before, after, handler);
 * while (!generator.isDone()) {
 *     generator.generate();
 * }
 * </pre>
 */
public class EventGenerator {

    private static final PerfLogger perfLogger = new PerfLogger(
            LoggerFactory.getLogger(EventGenerator.class.getName()
                    + ".perf"));

    /**
     * Maximum number of content changes to process during the
     * execution of a single diff continuation.
     */
    private static final int MAX_CHANGES_PER_CONTINUATION = 10000;

    /**
     * Maximum number of continuations queued for future processing.
     * Once this limit has been reached, we'll start pushing for the
     * processing of property-only diffs, which will automatically
     * help reduce the backlog.
     */
    private static final int MAX_QUEUED_CONTINUATIONS = 1000;

    private final LinkedList<Continuation> continuations = newLinkedList();

    /**
     * Creates a new generator instance. Changes to process need to be added
     * through {@link #addHandler(NodeState, NodeState, EventHandler)}
     */
    public EventGenerator() {}

    /**
     * Creates a new generator instance for processing the given changes.
     */
    public EventGenerator(
            @Nonnull NodeState before, @Nonnull NodeState after,
            @Nonnull EventHandler handler) {
        continuations.addFirst(new Continuation(handler, before, after, 0));
    }

    public void addHandler(NodeState before, NodeState after, EventHandler handler) {
        continuations.addFirst(new Continuation(handler, before, after, 0));
    }

    /**
     * Checks whether there are no more content changes to be processed.
     */
    public boolean isDone() {
        return continuations.isEmpty();
    }

    /**
     * Generates a finite number of {@link EventHandler} callbacks based
     * on the content changes that have yet to be processed. Further processing
     * (even if no callbacks were made) may be postponed to a future
     * {@link #generate()} call, until the {@link #isDone()} method finally
     * return {@code true}.
     */
    public void generate() {
        if (!continuations.isEmpty()) {
            final Continuation c = continuations.removeFirst();
            final long start = perfLogger
                    .start("generate: Starting event generation");
            c.run();
            perfLogger.end(start, 1, "generate: Generated {} events",
                    c.counter);
        }
    }

    private class Continuation implements NodeStateDiff, Runnable {

        /**
         * Filtered handler of detected content changes.
         */
        private final EventHandler handler;

        /**
         * Before state, possibly non-existent.
         */
        private final NodeState before;

        /**
         * After state, possibly non-existent.
         */
        private final NodeState after;

        /**
         * Number of initial changes to skip.
         */
        private final int skip;

        /**
         * Number of changes seen so far.
         */
        private int counter = 0;

        private Continuation(
                EventHandler handler, NodeState before, NodeState after,
                int skip) {
            this.handler = handler;
            this.before = before;
            this.after = after;
            this.skip = skip;
        }

        //------------------------------------------------------< Runnable >--

        /**
         * Continues the content diff from the point where this
         * continuation was created.
         */
        @Override
        public void run() {
            if (skip == 0) {
                // Only call enter if this is not a continuation that hit
                // the MAX_CHANGES_PER_CONTINUATION limit before
                handler.enter(before, after);
            }
            if (after.compareAgainstBaseState(before, this)) {
                // Only call leave if this continuation exists normally and not
                // as a result of hitting the MAX_CHANGES_PER_CONTINUATION limit
                handler.leave(before, after);
            }
        }

        //-------------------------------------------------< NodeStateDiff >--

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (beforeEvent()) {
                handler.propertyAdded(after);
                return afterEvent();
            } else {
                return true;
            }
        }

        @Override
        public boolean propertyChanged(
                PropertyState before, PropertyState after) {
            if (beforeEvent()) {
                // check for reordering of child nodes
                if (OAK_CHILD_ORDER.equals(before.getName())) {
                    // list the child node names before and after the change
                    List<String> beforeNames =
                            newArrayList(before.getValue(NAMES));
                    List<String> afterNames =
                            newArrayList(after.getValue(NAMES));

                    // check only those names that weren't added or removed
                    beforeNames.retainAll(newHashSet(afterNames));
                    afterNames.retainAll(newHashSet(beforeNames));

                    // Selection sort beforeNames into afterNames,
                    // recording the swaps as we go
                    for (int a = 0; a < afterNames.size() - 1; a++) {
                        String beforeName = beforeNames.get(a);
                        String afterName = afterNames.get(a);
                        if (!afterName.equals(beforeName)) {
                            // Find afterName in the beforeNames list.
                            // This loop is guaranteed to stop because both
                            // lists contain the same names and we've already
                            // processed all previous names.
                            int b = a + 1;
                            while (!afterName.equals(beforeNames.get(b))) {
                                b++;
                            }

                            // Swap the non-matching before name forward.
                            // No need to beforeNames.set(a, afterName),
                            // as we won't look back there anymore.
                            beforeNames.set(b, beforeName);

                            // find the destName of the orderBefore operation
                            String destName = null;
                            Iterator<String> iterator =
                                    after.getValue(NAMES).iterator();
                            while (destName == null && iterator.hasNext()) {
                                if (afterName.equals(iterator.next())) {
                                    if (iterator.hasNext()) {
                                        destName = iterator.next();
                                    }
                                }
                            }

                            // deliver the reordering event
                            handler.nodeReordered(
                                    destName, afterName,
                                    this.after.getChildNode(afterName));
                        }
                    }
                }

                handler.propertyChanged(before, after);
                return afterEvent();
            } else {
                return true;
            }
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (beforeEvent()) {
                handler.propertyDeleted(before);
                return afterEvent();
            } else {
                return true;
            }
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (fullQueue()) {
                return false;
            } else if (beforeEvent()) {
                PropertyState sourceProperty = after.getProperty(SOURCE_PATH);
                if (sourceProperty != null) {
                    String sourcePath = sourceProperty.getValue(STRING);
                    handler.nodeMoved(sourcePath, name, after);
                }

                handler.nodeAdded(name, after);
                addChildDiff(name, MISSING_NODE, after);
                return afterEvent();
            } else {
                return true;
            }
        }

        @Override
        public boolean childNodeChanged(
                String name, NodeState before, NodeState after) {
            if (fullQueue()) {
                return false;
            } else if (beforeEvent()) {
                addChildDiff(name, before, after);
                return afterEvent();
            } else {
                return true;
            }
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (fullQueue()) {
                return false;
            } else if (beforeEvent()) {
                handler.nodeDeleted(name, before);
                addChildDiff(name, before, MISSING_NODE);
                return afterEvent();
            } else {
                return true;
            }
        }

        //-------------------------------------------------------< private >--

        /**
         * Schedules a continuation for processing changes within the given
         * child node, if changes within that subtree should be processed.
         */
        private void addChildDiff(
                String name, NodeState before, NodeState after) {
            EventHandler h = handler.getChildHandler(name, before, after);
            if (h != null) {
                continuations.addFirst(new Continuation(h, before, after, 0));
            }
        }

        /**
         * Increases the event counter and checks whether the event should
         * be processed, i.e. whether the initial skip count has been reached.
         */
        private boolean beforeEvent() {
            return ++counter > skip;
        }

        /**
         * Checks whether the diff queue has reached the maximum size limit,
         * and postpones further processing of the current diff to later.
         * Even though this postponement increases the size of the queue
         * beyond the limit, doing so ultimately forces property-only
         * diffs to the beginning of the queue, and thus helps to
         * automatically clean up the backlog.
         */
        private boolean fullQueue() {
            if (counter > skip // must have processed at least one event
                    && continuations.size() >= MAX_QUEUED_CONTINUATIONS) {
                continuations.add(new Continuation(
                        handler, this.before, this.after, counter));
                return true;
            } else {
                return false;
            }
        }

        /**
         * Checks whether enough events have already been processed in this
         * continuation. If that is the case, we postpone further processing
         * to a new continuation that will first skip all the initial events
         * we've already seen. Otherwise we let the current diff continue.
         */
        private boolean afterEvent() {
            if (counter >= skip + MAX_CHANGES_PER_CONTINUATION) {
                continuations.addFirst(
                        new Continuation(handler, before, after, counter));
                return false;
            } else {
                return true;
            }
        }

    }

}
