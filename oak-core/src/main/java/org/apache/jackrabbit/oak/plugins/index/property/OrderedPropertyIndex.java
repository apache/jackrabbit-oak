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

import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.TYPE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A property index that supports ordering keys.
 */
public class OrderedPropertyIndex implements QueryIndex, AdvancedQueryIndex {

    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndex.class);

    @Override
    public String getIndexName() {
        return TYPE;
    }

    OrderedPropertyIndexLookup getLookup(NodeState root) {
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
    static IndexPlan.Builder getIndexPlanBuilder(final Filter filter) {
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("getPlans(Filter, List<OrderEntry>, NodeState)");
            LOG.debug("getPlans() - filter: {} - ", filter);
            LOG.debug("getPlans() - sortOrder: {} - ", sortOrder);
            LOG.debug("getPlans() - rootState: {} - ", root);
        }
        List<IndexPlan> plans = new ArrayList<IndexPlan>();
        if (filter.getFullTextConstraint() != null) {
            // not an appropriate index for full-text search
            return plans;
        }
        if (filter.containsNativeConstraint()) {
            // not an appropriate index for native search
            return plans;
        }

        OrderedPropertyIndexLookup lookup = getLookup(root);
        Collection<PropertyRestriction> restrictions = filter.getPropertyRestrictions();
        String filterPath = filter.getPath();

        // first we process the sole orders as we could be in a situation where we don't have
        // a where condition indexed but we do for order. In that case we will return always the
        // whole index
        if (sortOrder != null) {
            for (OrderEntry oe : sortOrder) {
                lookup.collectPlans(filter, filterPath, oe, plans);
            }
        }

        // then we add plans for each restriction that could apply to us
        for (Filter.PropertyRestriction pr : restrictions) {
            lookup.collectPlans(filter, filterPath, pr, plans);
        }

        return plans;
    }

    @Override
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        LOG.debug("getPlanDescription({}, {})", plan, root);
        StringBuilder buff = new StringBuilder("ordered");
        NodeState definition = plan.getDefinition();
        int depth = 1;
        boolean found = false;
        if (plan.getPropertyRestriction() != null) {
            PropertyRestriction pr = plan.getPropertyRestriction();
            String propertyName = PathUtils.getName(pr.propertyName);
            String operation = null;
            PropertyValue value = null;       
            // TODO support pr.list
            if (pr.first == null && pr.last == null) {
                // open query: [property] is not null
                operation = "is not null";
            } else if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                       && pr.lastIncluding) {
                // [property]=[value]
                operation = "=";
                value = pr.first;
            } else if (pr.first != null && !pr.first.equals(pr.last)) {
                // '>' & '>=' use cases
                if (OrderDirection.isAscending(definition)) {
                    value = pr.first;
                    operation = pr.firstIncluding ? ">=" : ">";
                }
            } else if (pr.last != null && !pr.last.equals(pr.first)) {
                // '<' & '<='
                if (!OrderDirection.isAscending(definition)) {
                    value = pr.last;
                    operation = pr.lastIncluding ? "<=" : "<";
                }
            }
            if (operation != null) {
                buff.append(' ').append(propertyName).append(' ').
                        append(operation).append(' ').append(value);
                found = true;
            }
        }
        List<OrderEntry> sortOrder = plan.getSortOrder();
        if (!found && sortOrder != null && !sortOrder.isEmpty()) {
            // we could be here if we have a query where the ORDER BY makes us play it.
            for (OrderEntry oe : sortOrder) {
                String propertyName = PathUtils.getName(oe.getPropertyName());
                depth = PathUtils.getDepth(oe.getPropertyName());
                buff.append(" order by ").append(propertyName);
                // stop with the first property that is indexed
                break;
            }
        }        
        if (depth > 1) {
            buff.append(" ancestor ").append(depth - 1);
        }       
        return buff.toString();
    }

    @Override
    public Cursor query(IndexPlan plan, NodeState root) {
        LOG.debug("query(IndexPlan, NodeState)");
        LOG.debug("query() - plan: {}", plan);
        LOG.debug("query() - rootState: {}", root);

        Filter filter = plan.getFilter();
        List<OrderEntry> sortOrder = plan.getSortOrder();
        String pathPrefix = plan.getPathPrefix();
        Iterable<String> paths = null;
        OrderedContentMirrorStoreStrategy strategy
                = OrderedPropertyIndexLookup.getStrategy(plan.getDefinition());
        int depth = 1;
        PropertyRestriction pr = plan.getPropertyRestriction();
        if (pr != null) {
            String propertyName = PathUtils.getName(pr.propertyName);
            depth = PathUtils.getDepth(pr.propertyName);
            paths = strategy.query(plan.getFilter(), propertyName,
                    plan.getDefinition(), pr, pathPrefix);
        }
        if (paths == null && sortOrder != null && !sortOrder.isEmpty()) {
            // we could be here if we have a query where the ORDER BY makes us play it.
            for (OrderEntry oe : sortOrder) {
                String propertyName = PathUtils.getName(oe.getPropertyName());
                depth = PathUtils.getDepth(oe.getPropertyName());
                paths = strategy.query(plan.getFilter(), propertyName,
                        plan.getDefinition(), new PropertyRestriction(), pathPrefix);
            }
        }

        if (paths == null) {
            // if still here then something went wrong.
            throw new IllegalStateException(
                    "OrderedPropertyIndex index is used even when no index is available for filter "
                            + filter);
        }
        Cursor cursor = Cursors.newPathCursor(paths, filter.getQueryEngineSettings());
        if (depth > 1) {
            cursor = Cursors.newAncestorCursor(cursor, depth - 1, filter.getQueryEngineSettings());
        }
        return cursor;
    }

    //--------------------------------------------------------< QueryIndex >--

    @Override
    public String getPlan(Filter filter, NodeState root) {
        return getPlanDescription(getIndexPlanBuilder(filter).build(), root);
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        return query(getIndexPlanBuilder(filter).build(), root);
    }

}
