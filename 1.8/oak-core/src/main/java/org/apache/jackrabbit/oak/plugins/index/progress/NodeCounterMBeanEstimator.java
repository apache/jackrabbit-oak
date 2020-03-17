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

package org.apache.jackrabbit.oak.plugins.index.progress;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_INCLUDED_PATHS;

public class NodeCounterMBeanEstimator implements NodeCountEstimator {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeCounter counter;
    private final NodeStore nodeStore;

    public NodeCounterMBeanEstimator(NodeStore nodeStore) {
        this(nodeStore, new NodeCounter(nodeStore));
    }

    NodeCounterMBeanEstimator(NodeStore nodeStore, NodeCounter nodeCounter) {
        this.nodeStore = nodeStore;
        this.counter = nodeCounter;
    }

    @Override
    public long getEstimatedNodeCount(String basePath, Set<String> indexPaths) {
        PathPerimeter pp = new PathPerimeter();
        pp.compute(basePath, indexPaths);

        if (pp.includeAll) {
            return counter.getEstimatedNodeCount(basePath);
        } else {
            long totalCount = 0;
            for (String path : pp.includes) {
                long estimate = counter.getEstimatedNodeCount(path);
                if (estimate > 0) {
                    totalCount += estimate;
                }
            }
            for (String path : pp.excludes) {
                long estimate = counter.getEstimatedNodeCount(path);
                if (estimate > 0) {
                    totalCount -= estimate;
                }
            }

            log.info("Paths to be traversed {}", pp);
            return totalCount;
        }
    }

    private class PathPerimeter {
        final Set<String> includes = new HashSet<>();
        final Set<String> excludes = new HashSet<>();
        boolean includeAll;

        public void compute(String basePath, Set<String> indexPaths) {
            NodeState root = nodeStore.getRoot();
            for (String indexPath : indexPaths) {
                NodeState idxState = NodeStateUtils.getNode(root, indexPath);

                //No include exclude specified so include all
                if (!idxState.hasProperty(PROP_INCLUDED_PATHS)
                        && !idxState.hasProperty(PROP_EXCLUDED_PATHS)) {
                    includeAll = true;
                    return;
                }

                Iterables.addAll(includes, idxState.getStrings(PROP_INCLUDED_PATHS));
                Iterables.addAll(excludes, idxState.getStrings(PROP_EXCLUDED_PATHS));
            }

            if (includes.isEmpty()) {
                includes.add(basePath);
            }

            PathUtils.unifyInExcludes(includes, excludes);
        }

        @Override
        public String toString() {
            return String.format("includedPath : %s, excludedPaths : %s", includes, excludes);
        }
    }
}
