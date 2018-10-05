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
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import java.util.Set;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Strategy that defines how the index content will be actually stored under the
 * index node
 */
public interface IndexStoreStrategy {
    
    /**
     * Updates the index for the given path.
     * 
     * @param index the index node supplier
     * @param path path stored in the index
     * @param indexName the name of the index. May be null.
     * @param indexMeta the definition of the index. May be null.
     * @param beforeKeys keys that no longer apply to the path
     * @param afterKeys keys that now do apply to the path
     */
    void update(
            Supplier<NodeBuilder> index, String path,
            String indexName, NodeBuilder indexMeta,
            Set<String> beforeKeys, Set<String> afterKeys) throws CommitFailedException;

    /**
     * Check whether an entry for the given key exists.
     * 
     * @param index the index node supplier
     * @param key the key
     * @return true if at least one entry exists
     */
    boolean exists(Supplier<NodeBuilder> index, String key);

    /**
     * Search for a given set of values.
     * 
     * @param filter the filter (can optionally be used for optimized query execution)
     * @param indexName the name of the index (for logging)
     * @param indexMeta the index metadata node (may not be null)
     * @param values values to look for (null to check for property existence)
     * @return an iterator of paths
     */
    Iterable<String> query(Filter filter, String indexName, NodeState indexMeta, Iterable<String> values);

    /**
     * Count the occurrence of a given set of values. Used in calculating the
     * cost of an index.
     *
     * @param root the root node (may not be null)
     * @param indexMeta the index metadata node (may not be null)
     * @param values values to look for (null to check for property existence)
     * @param max the maximum value to return
     * @return the aggregated count of occurrences for each provided value
     */
    long count(NodeState root, NodeState indexMeta, Set<String> values, int max);

    /**
     * Count the occurrence of a given set of values. Used in calculating the
     * cost of an index.
     *
     * @param filter the filter which can be used to estimate better cost
     * @param root the root node (may not be null)
     * @param indexMeta the index metadata node (may not be null)
     * @param values values to look for (null to check for property existence)
     * @param max the maximum value to return
     * @return the aggregated count of occurrences for each provided value
     */
    long count(Filter filter, NodeState root, NodeState indexMeta, Set<String> values, int max);

    String getIndexNodeName();

}
