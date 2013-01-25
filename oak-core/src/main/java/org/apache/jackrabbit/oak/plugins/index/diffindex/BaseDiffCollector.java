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
package org.apache.jackrabbit.oak.plugins.index.diffindex;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.EmptyNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

public abstract class BaseDiffCollector implements DiffCollector {

    private final NodeState before;
    private final NodeState after;

    private Set<String> results;
    protected boolean init = false;

    /**
     * @param before
     *            initial state
     * @param after
     *            after state
     * @param filter
     *            filter that verifies of a NodeState qualifies or not
     */
    public BaseDiffCollector(NodeState before, NodeState after) {
        this.before = before;
        this.after = after;
        results = new HashSet<String>();
    }

    public Set<String> getResults(Filter filter) {
        if (!init) {
            collect(filter);
        }
        return results;
    }

    public double getCost(Filter filter) {
        if (!init) {
            collect(filter);
        }
        if (results.isEmpty()) {
            return Double.POSITIVE_INFINITY;
        }

        // TODO probably the number of read nodes during the diff
        return 0;
    }

    public void collect(final Filter filter) {
        DiffCollectorNodeStateDiff diff = new DiffCollectorNodeStateDiff(this,
                filter);
        after.compareAgainstBaseState(before, diff);
        this.results = new HashSet<String>(diff.getResults());
        this.init = true;
    }

    abstract boolean match(NodeState state, Filter filter);

    protected boolean isUnique() {
        return false;
    }

    private static class DiffCollectorNodeStateDiff extends EmptyNodeStateDiff {

        private final BaseDiffCollector collector;
        private final Filter filter;
        private final Set<String> results;

        private final DiffCollectorNodeStateDiff parent;
        private final String path;
        private boolean done;

        DiffCollectorNodeStateDiff(BaseDiffCollector collector, Filter filter) {
            this(collector, filter, null, "", new HashSet<String>());
        }

        private DiffCollectorNodeStateDiff(BaseDiffCollector collector,
                Filter filter, DiffCollectorNodeStateDiff parent, String path,
                Set<String> results) {
            this.collector = collector;
            this.filter = filter;
            this.parent = parent;
            this.path = path;
            this.results = results;
        }

        private boolean isDone() {
            if (parent != null) {
                return parent.isDone();
            }
            return done;
        }

        private void setDone() {
            if (parent != null) {
                parent.setDone();
                return;
            }
            done = true;
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (NodeStateUtils.isHidden(name) || isDone()) {
                return;
            }
            testNodeState(after, name);
        }

        @Override
        public void childNodeChanged(String name, NodeState before,
                NodeState after) {
            if (isDone()) {
                return;
            }
            after.compareAgainstBaseState(before,
                    new DiffCollectorNodeStateDiff(collector, filter, this,
                            concat(path, name), results));
        }

        private void testNodeState(NodeState nodeState, String currentPath) {
            if (isDone()) {
                return;
            }
            boolean match = collector.match(nodeState, filter);
            if (match) {
                results.add(concat(path, currentPath));
                if (collector.isUnique()) {
                    setDone();
                    return;
                }
            }
        }

        Set<String> getResults() {
            return results;
        }
    }

}
