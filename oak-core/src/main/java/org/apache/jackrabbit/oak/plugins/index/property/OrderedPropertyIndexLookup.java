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

package org.apache.jackrabbit.oak.plugins.index.property;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 *
 */
public class OrderedPropertyIndexLookup extends PropertyIndexLookup {
    private NodeState root;
    
    /**
     * the standard Ascending ordered index
     */
    private static final IndexStoreStrategy STORE = new OrderedContentMirrorStoreStrategy();

    /**
     * the descending ordered index
     */
    private static final IndexStoreStrategy REVERSED_STORE = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
    
    /**
     * we're slightly more expensive than the standard PropertyIndex.
     */
    private static final int COST_OVERHEAD = 3;
    
    public OrderedPropertyIndexLookup(NodeState root) {
        super(root);
        this.root = root;
    }

    @Override
    IndexStoreStrategy getStrategy(NodeState indexMeta) {
        if (OrderDirection.isAscending(indexMeta)) {
            return STORE;
        } else {
            return REVERSED_STORE;
        }
    }
    
    public boolean isAscending(NodeState root, String propertyName, Filter filter){
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        return OrderDirection.isAscending(indexMeta);
    }

    @Override
    String getType() {
        return OrderedIndex.TYPE;
    }
    
    @Override
    public double getCost(Filter filter, String propertyName, PropertyValue value) {
        double cost = Double.POSITIVE_INFINITY;
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta != null) {
            // we relay then on the standard property index for the cost
            cost = COST_OVERHEAD
                   + getStrategy(indexMeta).count(indexMeta, PropertyIndex.encode(value), MAX_COST);
        }
        return cost;
    }

    /**
     * query the strategy for the provided constrains
     * 
     * @param filter
     * @param propertyName
     * @param pr
     * @return the resultset
     */
    public Iterable<String> query(Filter filter, String propertyName, PropertyRestriction pr) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta == null) {
            throw new IllegalArgumentException("No index for " + propertyName);
        }
        return ((OrderedContentMirrorStoreStrategy) getStrategy(indexMeta)).query(filter,
            propertyName, indexMeta, pr);
    }

    /**
     * return an estimated count to be used in IndexPlans.
     * 
     * @param propertyName
     * @param value
     * @param filter
     * @param pr
     * @return
     */
    public long getEstimatedEntryCount(String propertyName, PropertyValue value, Filter filter,
                                       PropertyRestriction pr) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        OrderedContentMirrorStoreStrategy strategy = (OrderedContentMirrorStoreStrategy) getStrategy(indexMeta);
        return strategy.count(indexMeta, pr, MAX_COST);
    }
}
