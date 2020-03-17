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
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

public abstract class BaseDiffCollector implements DiffCollector {

    protected boolean init;

    private final NodeState before;
    private final NodeState after;

    private Set<String> results;
    
    /**
     * The filter that was used to generate the result.
     */
    private Filter resultFilter;

    /**
     * @param before initial state
     * @param after after state
     */
    public BaseDiffCollector(NodeState before, NodeState after) {
        this.before = before;
        this.after = after;
        results = new HashSet<String>();
    }

    @Override
    public Set<String> getResults(Filter filter) {
        if (!init) {
            collect(filter);
        }
        return results;
    }

    @Override
    public double getCost(Filter filter) {
        if (!init || filter != resultFilter) {
            collect(filter);
        }
        if (results.isEmpty()) {
            return Double.POSITIVE_INFINITY;
        }
        // no read operations needed
        return 0;
    }

    public void collect(final Filter filter) {
        DiffCollectorNodeStateDiff diff = new DiffCollectorNodeStateDiff(this,
                filter);
        after.compareAgainstBaseState(before, diff);
        this.results = new HashSet<String>(diff.getResults());
        this.resultFilter = filter;
        this.init = true;
    }

    abstract boolean match(NodeState state, Filter filter);

    protected boolean isUnique() {
        return false;
    }

    private static class DiffCollectorNodeStateDiff extends DefaultNodeStateDiff {

        private final BaseDiffCollector collector;
        private final Filter filter;
        private final Set<String> results;

        private final String path;

        DiffCollectorNodeStateDiff(BaseDiffCollector collector, Filter filter) {
            this(collector, filter, "", new HashSet<String>());
        }

        private DiffCollectorNodeStateDiff(
                BaseDiffCollector collector, Filter filter, String path,
                Set<String> results) {
            this.collector = collector;
            this.filter = filter;
            this.path = path;
            this.results = results;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            return childNodeChanged(name, EMPTY_NODE, after);
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before,
                NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                return true;
            }
            return testNodeState(after, name)
                    && after.compareAgainstBaseState(
                            before,
                            new DiffCollectorNodeStateDiff(
                                    collector, filter,
                                    concat(path, name), results));
        }

        private boolean testNodeState(NodeState nodeState, String currentPath) {
            if (collector.match(nodeState, filter)) {
                results.add(concat(path, currentPath));
                if (collector.isUnique()) {
                    return false;
                }
            }
            return true;
        }

        Set<String> getResults() {
            return results;
        }
    }

}
