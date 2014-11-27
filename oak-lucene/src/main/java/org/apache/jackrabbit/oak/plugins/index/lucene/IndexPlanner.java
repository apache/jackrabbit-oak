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
import java.util.Map;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.lucene.index.IndexReader;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

class IndexPlanner {
    private final IndexDefinition defn;
    private final Filter filter;
    private final String indexPath;
    private final List<OrderEntry> sortOrder;
    private IndexNode indexNode;
    private PlanResult result;

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

        if (defn.isTestMode()
                && builder == null){
            if (notSupportedFeature()) {
                return null;
            }
            String msg = String.format("No plan found for filter [%s] " +
                    "while using definition [%s] and testMode is found to be enabled", filter, defn);
            throw new IllegalStateException(msg);
        }

        return builder != null ? builder.build() : null;
    }

    @Override
    public String toString() {
        return "IndexPlanner{" +
                "indexPath='" + indexPath + '\'' +
                ", filter=" + filter +
                ", sortOrder=" + sortOrder +
                '}';
    }

    private IndexPlan.Builder getPlanBuilder() {
        FullTextExpression ft = filter.getFullTextConstraint();

        if (!defn.getVersion().isAtLeast(IndexFormatVersion.V2)){
            return null;
        }

        //Query Fulltext and Index does not support fulltext
        if (ft != null && !defn.isFullTextEnabled()) {
            return null;
        }

        IndexingRule indexingRule = getApplicableRule();
        if (indexingRule == null){
            return null;
        }

        //Query Fulltext and indexing rule does not support fulltext
        if (ft != null && !indexingRule.isFulltextEnabled()){
            return null;
        }

        result = new PlanResult(indexPath, defn, indexingRule);

        if (defn.hasFunctionDefined()
                && filter.getPropertyRestriction(defn.getFunctionName()) != null) {
            //If native function is handled by this index then ensure
            // that lowest cost if returned
            return defaultPlan().setEstimatedEntryCount(1);
        }

        List<String> indexedProps = newArrayListWithCapacity(filter.getPropertyRestrictions().size());

        //Optimization - Go further only if any of the property is configured
        //for property index
        if (indexingRule.propertyIndexEnabled) {
            for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
                PropertyDefinition pd = indexingRule.getConfig(pr.propertyName);
                if (pd != null && pd.propertyIndexEnabled()) {
                    indexedProps.add(pr.propertyName);
                    result.propDefns.put(pr.propertyName, pd);
                }
            }
        }

        boolean evalPathRestrictions = canEvalPathRestrictions();
        //TODO For the full text case need to determine if all field names
        //used in fulltext expression are fulltext indexed or not

        //Fulltext expression can also be like jcr:contains(jcr:content/metadata/@format, 'image')

        List<OrderEntry> sortOrder = createSortOrder(indexingRule);
        if (!indexedProps.isEmpty() || !sortOrder.isEmpty() || ft != null || evalPathRestrictions) {
            //TODO Need a way to have better cost estimate to indicate that
            //this index can evaluate more propertyRestrictions natively (if more props are indexed)
            //For now we reduce cost per entry
            int costPerEntryFactor = indexedProps.size();
            costPerEntryFactor += sortOrder.size();

            //this index can evaluate more propertyRestrictions natively (if more props are indexed)
            //For now we reduce cost per entry
            IndexPlan.Builder plan = defaultPlan();
            if (!sortOrder.isEmpty()) {
                plan.setSortOrder(sortOrder);
            }

            if (costPerEntryFactor == 0){
                costPerEntryFactor = 1;
            }

            return plan.setCostPerEntry(1.0 / costPerEntryFactor);
        }

        //TODO Support for property existence queries
        //TODO support for nodeName queries
        return null;
    }

    private boolean canEvalPathRestrictions() {
        if (filter.getPathRestriction() == Filter.PathRestriction.NO_RESTRICTION){
            return false;
        }
        //TODO If no other restrictions is provided and query is pure
        //path restriction based then need to be sure that index definition at least
        //allows indexing all the path for given nodeType
        return defn.evaluatePathRestrictions();
    }

    private IndexPlan.Builder defaultPlan() {
        return new IndexPlan.Builder()
                .setCostPerExecution(1) // we're local. Low-cost
                .setCostPerEntry(1)
                .setFulltextIndex(defn.isFullTextEnabled())
                .setIncludesNodeData(false) // we should not include node data
                .setFilter(filter)
                .setPathPrefix(getPathPrefix())
                .setDelayed(true) //Lucene is always async
                .setAttribute(LucenePropertyIndex.ATTR_PLAN_RESULT, result)
                .setEstimatedEntryCount(estimatedEntryCount());
    }

    private long estimatedEntryCount() {
        //Other index only compete in case of property indexes. For fulltext
        //index return true count so as to allow multiple property indexes
        //to be compared fairly
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft != null && defn.isFullTextEnabled()){
            return getReader().numDocs();
        }
        return Math.min(defn.getEntryCount(), getReader().numDocs());
    }

    private String getPathPrefix() {
        String parentPath = PathUtils.getAncestorPath(indexPath, 2);
        return PathUtils.denotesRoot(parentPath) ? "" : parentPath;
    }

    private IndexReader getReader() {
        return indexNode.getSearcher().getIndexReader();
    }

    private List<OrderEntry> createSortOrder(IndexingRule rule) {
        if (sortOrder == null) {
            return Collections.emptyList();
        }

        List<OrderEntry> orderEntries = newArrayListWithCapacity(sortOrder.size());
        for (OrderEntry o : sortOrder) {
            PropertyDefinition pd = rule.getConfig(o.getPropertyName());
            if (pd != null
                    && pd.ordered
                    && o.getPropertyType() != null
                    && !o.getPropertyType().isArray()) {
                orderEntries.add(o); //Lucene can manage any order desc/asc
                result.sortedProperties.add(pd);
            }
        }
        //TODO Should we return order entries only when all order clauses are satisfied
        return orderEntries;
    }

    @CheckForNull
    private IndexingRule getApplicableRule() {
        if (filter.matchesAllTypes()){
            return defn.getApplicableIndexingRule(JcrConstants.NT_BASE);
        } else {
            //TODO May be better if filter.getSuperTypes returned a list which maintains
            //inheritance order and then we iterate over that
            for (IndexingRule rule : defn.getDefinedRules()){
                if (filter.getSupertypes().contains(rule.getNodeTypeName())){
                    //Theoretically there may be multiple rules for same nodeType with
                    //some condition defined. So again find a rule which applies
                    IndexingRule matchingRule = defn.getApplicableIndexingRule(rule.getNodeTypeName());
                    if (matchingRule != null){
                        return rule;
                    }
                }
            }
        }
        return null;
    }

    private boolean notSupportedFeature() {
        if(filter.getPathRestriction() == Filter.PathRestriction.NO_RESTRICTION
                && filter.matchesAllTypes()
                && filter.getPropertyRestrictions().isEmpty()){
            //This mode includes name(), localname() queries
            //OrImpl [a/name] = 'Hello' or [b/name] = 'World'
            return true;
        }
        return false;
    }

    //~--------------------------------------------------------< PlanResult >

    public static class PlanResult {
        final String indexPath;
        final IndexDefinition indexDefinition;
        final IndexingRule indexingRule;
        private List<PropertyDefinition> sortedProperties = newArrayList();
        private Map<String, PropertyDefinition> propDefns = newHashMap();

        public PlanResult(String indexPath, IndexDefinition defn, IndexingRule indexingRule) {
            this.indexPath = indexPath;
            this.indexDefinition = defn;
            this.indexingRule = indexingRule;
        }

        public PropertyDefinition getPropDefn(PropertyRestriction pr){
            return propDefns.get(pr.propertyName);
        }

        public PropertyDefinition getOrderedProperty(int index){
            return sortedProperties.get(index);
        }
    }
}
