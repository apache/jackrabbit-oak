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
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import java.util.Set;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Predicate;

/**
 * A delegating IndexStoreStrategy that filters out updates that are not
 * accepted by the given predicate
 */
public class FilteringIndexStoreStrategy implements IndexStoreStrategy {

    private final IndexStoreStrategy strategy;
    private final Predicate<String> filter;
    private final boolean readOnly;

    public FilteringIndexStoreStrategy(IndexStoreStrategy strategy,
            Predicate<String> filter) {
        this(strategy, filter, false);
    }

    public FilteringIndexStoreStrategy(IndexStoreStrategy strategy,
            Predicate<String> filter, boolean readOnly) {
        this.strategy = strategy;
        this.filter = filter;
        this.readOnly = readOnly;
    }

    @Override
    public void update(Supplier<NodeBuilder> index, String path, String indexName,
                       NodeBuilder indexMeta, Set<String> beforeKeys, Set<String> afterKeys)
            throws CommitFailedException {
        if (filter.apply(path)) {
            if (readOnly) {
                throw new CommitFailedException(
                        CommitFailedException.UNSUPPORTED, 0,
                        "Unsupported commit to a read-only store!",
                        new Throwable("Commit path: " + path));
            }
            strategy.update(index, path, indexName, indexMeta, beforeKeys,
                    afterKeys);
        }
    }

    @Override
    public boolean exists(Supplier<NodeBuilder> index, String key) {
        return strategy.exists(index, key);
    }

    @Override
    public Iterable<String> query(Filter filter, String indexName,
            NodeState indexMeta, Iterable<String> values) {
        return strategy.query(filter, indexName, indexMeta, values);
    }

    @Override
    public long count(NodeState root, NodeState indexMeta, Set<String> values,
            int max) {
        return strategy.count(root, indexMeta, values, max);
    }

    @Override
    public long count(Filter filter, NodeState root, NodeState indexMeta,
            Set<String> values, int max) {
        return strategy.count(filter, root, indexMeta, values, max);
    }

    @Override
    public String getIndexNodeName() {
        return strategy.getIndexNodeName();
    }

}
