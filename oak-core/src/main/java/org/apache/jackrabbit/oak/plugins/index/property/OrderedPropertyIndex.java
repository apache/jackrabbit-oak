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

import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.TYPE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * A property index that supports ordering keys.
 */
public class OrderedPropertyIndex extends PropertyIndex implements AdvancedQueryIndex {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndex.class);
    
    @Override
    public String getIndexName() {
        return TYPE;
    }

    @Override
    PropertyIndexLookup getLookup(NodeState root) {
        return new OrderedPropertyIndexLookup(root);
    }

    /**
     * retrieve the cost for the query.
     * 
     * !!! for now we want to skip the use-case of NON range-queries !!!
     */
    @Override
    public double getCost(Filter filter, NodeState root) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }
    
    /**
     * @return an builder with some initial common settings
     */
    private static IndexPlan.Builder getIndexPlanBuilder(final Filter filter) {
        IndexPlan.Builder b = new IndexPlan.Builder();
        b.setCostPerExecution(1); // we're local. Low-cost
        // we're local but slightly more expensive than a standard PropertyIndex
        b.setCostPerEntry(1.3);
        b.setFulltextIndex(false); // we're never full-text
        b.setIncludesNodeData(false); // we should not include node data
        b.setFilter(filter);
        // TODO it's synch for now but we should investigate the indexMeta
        b.setDelayed(false);
        return b;
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState root) {
        LOG.debug("getPlans(Filter, List<OrderEntry>, NodeState)");
        LOG.debug("getPlans() - filter: {} - ", filter);
        LOG.debug("getPlans() - sortOrder: {} - ", sortOrder);
        LOG.debug("getPlans() - rootState: {} - ", root);
        List<IndexPlan> plans = new ArrayList<IndexPlan>();

        PropertyIndexLookup pil = getLookup(root);
        if (pil instanceof OrderedPropertyIndexLookup) {
            OrderedPropertyIndexLookup lookup = (OrderedPropertyIndexLookup) pil;
            Collection<PropertyRestriction> restrictions = filter.getPropertyRestrictions();

            // first we process the sole orders as we could be in a situation where we don't have
            // a where condition indexed but we do for order. In that case we will return always the
            // whole index
            if (sortOrder != null) {
                for (OrderEntry oe : sortOrder) {
                    String propertyName = PathUtils.getName(oe.getPropertyName());
                    if (lookup.isIndexed(propertyName, "/", filter)) {
                        IndexPlan.Builder b = getIndexPlanBuilder(filter);
                        b.setSortOrder(ImmutableList.of(new OrderEntry(
                            propertyName,
                                Type.UNDEFINED,
                                lookup.isAscending(root, propertyName, filter) ? OrderEntry.Order.ASCENDING
                                        : OrderEntry.Order.DESCENDING)));
                        b.setEstimatedEntryCount(lookup.getEstimatedEntryCount(propertyName, null,
                            filter, null));
                        IndexPlan plan = b.build();
                        LOG.debug("plan: {}", plan);
                        plans.add(plan);
                    }
                }
            }

            // then we add plans for each restriction that could apply to us
            for (Filter.PropertyRestriction pr : restrictions) {
                String propertyName = PathUtils.getName(pr.propertyName);
                if (lookup.isIndexed(propertyName, "/", filter)) {
                    PropertyValue value = null;
                    boolean createPlan = true;
                    if (pr.first == null && pr.last == null) {
                        // open query: [property] is not null
                        value = null;
                    } else if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                               && pr.lastIncluding) {
                        // [property]=[value]
                        value = pr.first;
// ----------- DISABLING RANGE QUERIES FOR NOW. EASYING THE INTEGRATION WITH OAK-622 [BEGIN]
//                    } else if (pr.first != null && !pr.first.equals(pr.last)) {
//                        // '>' & '>=' use cases
//                        if (lookup.isAscending(root, propertyName, filter)) {
//                            value = pr.first;
//                        } else {
//                            createPlan = false;
//                        }
//                    } else if (pr.last != null && !pr.last.equals(pr.first)) {
//                        // '<' & '<='
//                        if (!lookup.isAscending(root, propertyName, filter)) {
//                            value = pr.last;
//                        } else {
//                            createPlan = false;
//                        }
// ----------- DISABLING RANGE QUERIES FOR NOW. EASYING THE INTEGRATION WITH OAK-622 [ END ]
                    }
                    if (createPlan) {
                        // we always return a sorted set
                        IndexPlan.Builder b = getIndexPlanBuilder(filter);
                        b.setSortOrder(ImmutableList.of(new OrderEntry(
                            propertyName,
                            Type.UNDEFINED,
                            (lookup.isAscending(root, propertyName, filter) ? OrderEntry.Order.ASCENDING
                                                                           : OrderEntry.Order.DESCENDING))));
                        long count = lookup.getEstimatedEntryCount(propertyName, value, filter, pr);
                        b.setEstimatedEntryCount(count);
                        LOG.debug("estimatedCount: {}", count);

                        IndexPlan plan = b.build();
                        LOG.debug("plan: {}", plan);
                        plans.add(plan);
                    }
                }
            }
        } else {
            LOG.error("Without an OrderedPropertyIndexLookup you should not end here");
        }

        return plans;
    }

    @Override
    public String getPlanDescription(IndexPlan plan) {
        LOG.debug("getPlanDescription() - plan: {}", plan);
        LOG.error("Not implemented yet");
        throw new UnsupportedOperationException("Not implemented yet.");
//        return null;
    }

    @Override
    public Cursor query(IndexPlan plan, NodeState root) {
        LOG.debug("query(IndexPlan, NodeState)");
        LOG.debug("query() - plan: {}", plan);
        LOG.debug("query() - rootState: {}", root);

        Filter filter = plan.getFilter();
        List<OrderEntry> sortOrder = plan.getSortOrder();
        Iterable<String> paths = null;
        Cursor cursor = null;
        PropertyIndexLookup pil = getLookup(root);
        if (pil instanceof OrderedPropertyIndexLookup) {
            OrderedPropertyIndexLookup lookup = (OrderedPropertyIndexLookup) pil;
            Collection<PropertyRestriction> prs = filter.getPropertyRestrictions();
            int depth = 1;
            for (PropertyRestriction pr : prs) {
                String propertyName = PathUtils.getName(pr.propertyName);
                depth = PathUtils.getDepth(pr.propertyName);
                if (lookup.isIndexed(propertyName, "/", filter)) {
                    paths = lookup.query(filter, propertyName, pr);
                } 
            }
            if (paths == null && sortOrder != null && !sortOrder.isEmpty()) {
                // we could be here if we have a query where the ORDER BY makes us play it.
                for (OrderEntry oe : sortOrder) {
                    String propertyName = PathUtils.getName(oe.getPropertyName());
                    if (lookup.isIndexed(propertyName, "/", null)) {
                        paths = lookup.query(null, propertyName, new PropertyRestriction());
                    }
                }
            }
            if (paths == null) {
                // if still here then something went wrong.
                throw new IllegalStateException(
                    "OrderedPropertyIndex index is used even when no index is available for filter "
                        + filter);
            }
            cursor = Cursors.newPathCursor(paths);
            if (depth > 1) {
                cursor = Cursors.newAncestorCursor(cursor, depth - 1);
            }
        } else {
            // if for some reasons it's not an Ordered Lookup we delegate up the chain
            cursor = super.query(filter, root);
        }
        return cursor;
    }
}
