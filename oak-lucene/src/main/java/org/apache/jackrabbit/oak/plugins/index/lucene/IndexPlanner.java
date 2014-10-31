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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.lucene.index.IndexReader;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

public class IndexPlanner {
    private final IndexDefinition defn;
    private final Filter filter;
    private final String indexPath;
    private final List<OrderEntry> sortOrder;
    private IndexNode indexNode;

    public IndexPlanner(IndexNode indexNode,
                        String indexPath,
                        Filter filter, List<OrderEntry> sortOrder) {
        this.indexNode = indexNode;
        this.indexPath = indexPath;
        this.defn = indexNode.getDefinition();
        this.filter = filter;
        this.sortOrder = sortOrder;
    }

    IndexPlan getPlan() {
        IndexPlan.Builder builder = getPlanBuilder();
        return builder != null ? builder.build() : null;
    }

    private IndexPlan.Builder getPlanBuilder() {
        FullTextExpression ft = filter.getFullTextConstraint();

        //IndexPlanner is currently for property indexes only and does not
        //support full text indexes
        if (ft != null) {
            return null;
        }

        if (defn.hasFunctionDefined()
                && filter.getPropertyRestriction(defn.getFunctionName()) != null) {
            //If native function is handled by this index then ensure
            // that lowest cost if returned
            return defaultPlan().setEstimatedEntryCount(1);
        }

        List<String> indexedProps = newArrayListWithCapacity(filter.getPropertyRestrictions().size());
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            //Only those properties which are included and not tokenized
            //can be managed by lucene for property restrictions
            if (defn.includeProperty(pr.propertyName)
                    && defn.skipTokenization(pr.propertyName)) {
                indexedProps.add(pr.propertyName);
            }
        }

        if (!indexedProps.isEmpty()) {
            //TODO Need a way to have better cost estimate to indicate that
            //this index can evaluate more propertyRestrictions natively (if more props are indexed)
            //For now we reduce cost per entry
            int costPerEntryFactor = indexedProps.size();
            
            // Restrict matching index when declaringNodeTypes declared
            if (defn.hasDeclaredNodeTypes()) {
                if (supportsNodeTypes(filter)) {
                    // Reduce cost per entry by a small factor because number of nodes would be less
                    costPerEntryFactor += 1;
                } else {
                    return null;
                }
            }
            //this index can evaluate more propertyRestrictions natively (if more props are indexed)
            //For now we reduce cost per entry
            IndexPlan.Builder plan = defaultPlan();
            if (plan != null) {
                return plan.setCostPerEntry(1.0 / costPerEntryFactor);
            }
        }

        //TODO Support for property existence queries
        //TODO support for nodeName queries
        return null;
    }

    /**
     * Determines if NodeTypes as defined in Filter are supported by current index
     */
    private boolean supportsNodeTypes(Filter filter) {
        return !Sets
            .intersection(defn.getDeclaringNodeTypes(), getSuperTypes(filter)).isEmpty();
    }

    private IndexPlan.Builder defaultPlan() {
        return new IndexPlan.Builder()
                .setCostPerExecution(1) // we're local. Low-cost
                .setCostPerEntry(1)
                .setFulltextIndex(defn.isFullTextEnabled())
                .setIncludesNodeData(false) // we should not include node data
                .setFilter(filter)
                .setPathPrefix(getPathPrefix())
                .setSortOrder(createSortOrder())
                .setDelayed(true) //Lucene is always async
                .setAttribute(LuceneIndex.ATTR_INDEX_PATH, indexPath)
                .setEstimatedEntryCount(Math.min(defn.getEntryCount(), getReader().numDocs()));
    }

    private String getPathPrefix() {
        String parentPath = PathUtils.getAncestorPath(indexPath, 2);
        return PathUtils.denotesRoot(parentPath) ? "" : parentPath;
    }

    private IndexReader getReader() {
        return indexNode.getSearcher().getIndexReader();
    }

    @CheckForNull
    private List<OrderEntry> createSortOrder() {
        //TODO Refine later once we make mixed indexes having both
        //full text  and property index
        if (defn.isFullTextEnabled()) {
            return Collections.emptyList();
        }

        if (sortOrder == null) {
            return null;
        }

        List<OrderEntry> orderEntries = newArrayListWithCapacity(sortOrder.size());
        for (OrderEntry o : sortOrder) {
            //sorting can only be done for known/configured properties
            // and whose types are known
            if (defn.includeProperty(o.getPropertyName()) || defn.isOrdered(o.getPropertyName())
                    && o.getPropertyType() != null
                    && !o.getPropertyType().isArray()) {
                orderEntries.add(o); //Lucene can manage any order desc/asc
            }
        }
        return orderEntries;
    }

    private static Set<String> getSuperTypes(Filter filter) {
        return filter.matchesAllTypes() ? Collections.<String>emptySet() : filter.getSupertypes();
    }
}
