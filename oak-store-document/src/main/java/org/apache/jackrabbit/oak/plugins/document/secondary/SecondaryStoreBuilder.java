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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import static java.util.Collections.singletonList;

public class SecondaryStoreBuilder {
    private final NodeStore store;
    private PathFilter pathFilter = new PathFilter(singletonList("/"), Collections.<String>emptyList());
    private NodeStateDiffer differ = NodeStateDiffer.DEFAULT_DIFFER;
    private StatisticsProvider statsProvider = StatisticsProvider.NOOP;
    private List<String> metaPropNames = Collections.emptyList();

    public SecondaryStoreBuilder(NodeStore nodeStore) {
        this.store = nodeStore;
    }

    public SecondaryStoreBuilder pathFilter(PathFilter filter) {
        this.pathFilter = filter;
        return this;
    }

    public SecondaryStoreBuilder differ(NodeStateDiffer differ) {
        this.differ = differ;
        return this;
    }

    public SecondaryStoreBuilder statisticsProvider(StatisticsProvider statisticsProvider) {
        this.statsProvider = statisticsProvider;
        return this;
    }

    public SecondaryStoreBuilder metaPropNames(List<String> metaPropNames) {
        this.metaPropNames = ImmutableList.copyOf(metaPropNames);
        return this;
    }

    public SecondaryStoreCache buildCache() {
        return new SecondaryStoreCache(store, differ, pathFilter, statsProvider);
    }

    public SecondaryStoreObserver buildObserver(){
        return buildObserver(SecondaryStoreRootObserver.NOOP);
    }

    public SecondaryStoreObserver buildObserver(SecondaryStoreRootObserver secondaryStoreRootObserver) {
        return new SecondaryStoreObserver(store, metaPropNames, differ, pathFilter, statsProvider, secondaryStoreRootObserver);
    }
}
