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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetHelper;
import org.apache.jackrabbit.oak.plugins.index.property.ValuePatternUtil;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.lucene.index.IndexReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.JcrConstants.JCR_SCORE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.commons.PathUtils.getAncestorPath;
import static org.apache.jackrabbit.oak.commons.PathUtils.getDepth;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

class IndexPlanner {
    private static final String FLAG_ENTRY_COUNT = "oak.lucene.useActualEntryCount";
    private static final Logger log = LoggerFactory.getLogger(IndexPlanner.class);
    private final IndexDefinition definition;
    private final Filter filter;
    private final String indexPath;
    private final List<OrderEntry> sortOrder;
    private IndexNode indexNode;
    private PlanResult result;
    private static boolean useActualEntryCount = false;

    static {
        useActualEntryCount = Boolean.parseBoolean(System.getProperty(FLAG_ENTRY_COUNT, "true"));
        if (!useActualEntryCount) {
            log.info("System property {} found to be false. IndexPlanner would use a default entryCount of 1000 instead" +
                    " of using the actual entry count", FLAG_ENTRY_COUNT);
        }
    }

    public IndexPlanner(IndexNode indexNode,
                        String indexPath,
                        Filter filter, List<OrderEntry> sortOrder) {
        this.indexNode = indexNode;
        this.indexPath = indexPath;
        this.definition = indexNode.getDefinition();
        this.filter = filter;
        this.sortOrder = sortOrder;
    }

    IndexPlan getPlan() {
        IndexPlan.Builder builder = getPlanBuilder();

        if (definition.isTestMode()){
            if ( builder == null) {
                if (notSupportedFeature()) {
                    return null;
                }
                String msg = String.format("No plan found for filter [%s] " +
                        "while using definition [%s] and testMode is found to be enabled", filter, definition);
                throw new IllegalStateException(msg);
            } else {
                builder.setEstimatedEntryCount(1)
                        .setCostPerExecution(1e-3)
                        .setCostPerEntry(1e-3);
            }
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

    //For tests
    static void setUseActualEntryCount(boolean useActualEntryCount) {
        IndexPlanner.useActualEntryCount = useActualEntryCount;
    }

    private IndexPlan.Builder getPlanBuilder() {
        log.trace("Evaluating plan with index definition {}", definition);

        if (wrongIndex()) {
            return null;
        }

        FullTextExpression ft = filter.getFullTextConstraint();

        if (!definition.getVersion().isAtLeast(IndexFormatVersion.V2)){
            log.trace("Index is old format. Not supported");
            return null;
        }

        //Query Fulltext and Index does not support fulltext
        if (ft != null && !definition.isFullTextEnabled()) {
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

        if (!checkForQueryPaths()) {
            log.trace("Opting out due mismatch between path restriction {} and query paths {}",
                    filter.getPath(), definition.getQueryPaths());
            return null;
        }

        result = new PlanResult(indexPath, definition, indexingRule);

        if (definition.hasFunctionDefined()
                && filter.getPropertyRestriction(definition.getFunctionName()) != null) {
            return getNativeFunctionPlanBuilder(indexingRule.getBaseNodeType());
        }

        List<String> indexedProps = newArrayListWithCapacity(filter.getPropertyRestrictions().size());

        for (PropertyDefinition functionIndex : indexingRule.getFunctionRestrictions()) {
            for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
                String f = functionIndex.function;
                if (pr.propertyName.equals(f)) {
                    indexedProps.add(f);
                    result.propDefns.put(f, functionIndex);
                }
            }
        }
        //Optimization - Go further only if any of the property is configured
        //for property index
        List<String> facetFields = new LinkedList<String>();
        if (indexingRule.propertyIndexEnabled) {
            for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
                String name = pr.propertyName;
                if (QueryConstants.RESTRICTION_LOCAL_NAME.equals(name)) {
                    continue;
                }
                if (name.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX)) {
                    // function-based indexes were handled before
                    continue;
                }
                if (QueryConstants.REP_FACET.equals(pr.propertyName)) {
                    String value = pr.first.getValue(Type.STRING);
                    facetFields.add(FacetHelper.parseFacetField(value));
                }

                PropertyDefinition pd = indexingRule.getConfig(pr.propertyName);
                if (pd != null && pd.propertyIndexEnabled()) {
                    if (pr.isNullRestriction() && !pd.nullCheckEnabled){
                        continue;
                    }

                    if (!pd.valuePattern.matchesAll()){
                        //So we have a valuePattern defined. So determine if
                        //this index can return a plan based on values
                        Set<String> values = ValuePatternUtil.getAllValues(pr);
                        if (values == null) {
                            // "is not null" condition, but we have a value pattern
                            // that doesn't match everything
                            // case of like search
                            String prefix = ValuePatternUtil.getLongestPrefix(filter, name);
                            if (!pd.valuePattern.matchesPrefix(prefix)) {
                                // region match which is not fully in the pattern
                                continue;
                            }
                        } else {
                            // we have a value pattern, for example (a|b),
                            // but we search (also) for 'c': can't match
                            if (!pd.valuePattern.matchesAll(values)) {
                                continue;
                            }
                        }
                    }

                    //A property definition with weight == 0 is only meant to be used
                    //with some other definitions
                    if (pd.weight != 0) {
                        indexedProps.add(name);
                    }
                    result.propDefns.put(name, pd);
                }
            }
        }

        boolean evalNodeTypeRestrictions = canEvalNodeTypeRestrictions(indexingRule);
        boolean evalPathRestrictions = canEvalPathRestrictions(indexingRule);
        boolean canEvalAlFullText = canEvalAllFullText(indexingRule, ft);
        boolean canEvalNodeNameRestriction = canEvalNodeNameRestriction(indexingRule);

        if (ft != null && !canEvalAlFullText){
            return null;
        }

        //Fulltext expression can also be like jcr:contains(jcr:content/metadata/@format, 'image')

        List<OrderEntry> sortOrder = createSortOrder(indexingRule);
        boolean canSort = canSortByProperty(sortOrder);
        if (!indexedProps.isEmpty() || canSort || ft != null
                || evalPathRestrictions || evalNodeTypeRestrictions || canEvalNodeNameRestriction) {
            //TODO Need a way to have better cost estimate to indicate that
            //this index can evaluate more propertyRestrictions natively (if more props are indexed)
            //For now we reduce cost per entry

            //Use propDefns instead of indexedProps as it determines true count of property restrictions
            //which are evaluated by this index
            int costPerEntryFactor = result.propDefns.size();
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

            if (facetFields.size() > 0) {
                plan.setAttribute(FacetHelper.ATTR_FACET_FIELDS, facetFields);
            }

            if (ft == null){
                result.enableNonFullTextConstraints();
            }

            if (evalNodeTypeRestrictions){
                result.enableNodeTypeEvaluation();
            }

            if (canEvalNodeNameRestriction){
                result.enableNodeNameRestriction();
            }

            return plan.setCostPerEntry(definition.getCostPerEntry() / costPerEntryFactor);
        }

        //TODO Support for property existence queries

        return null;
    }

    private boolean wrongIndex() {
        // REMARK: similar code is used in oak-core, PropertyIndex
        // skip index if "option(index ...)" doesn't match
        PropertyRestriction indexName = filter.getPropertyRestriction(IndexConstants.INDEX_NAME_OPTION);
        boolean wrong = false;
        if (indexName != null && indexName.first != null) {
            String name = indexName.first.getValue(Type.STRING);
            String thisName = definition.getIndexName();
            if (thisName != null) {
                thisName = PathUtils.getName(thisName);
                if (thisName.equals(name)) {
                    // index name specified, and matches
                    return false;
                }
            }
            wrong = true;
        }
        PropertyRestriction indexTag = filter.getPropertyRestriction(IndexConstants.INDEX_TAG_OPTION);
        if (indexTag != null && indexTag.first != null) {
            // index tag specified
            String[] tags = definition.getIndexTags();
            if (tags == null) {
                // no tag
                return true;
            }
            String tag = indexTag.first.getValue(Type.STRING);
            for(String t : tags) {
                if (t.equals(tag)) {
                    // tag matches
                    return false;
                }
            }
            // no tag matches
            return true;
        }
        // no tag specified
        return wrong;
    }

    private IndexPlan.Builder getNativeFunctionPlanBuilder(String indexingRuleBaseNodeType) {
        boolean canHandleNativeFunction = true;

        PropertyValue pv = filter.getPropertyRestriction(definition.getFunctionName()).first;
        String query = pv.getValue(Type.STRING);

        if (query.startsWith("suggest?term=")) {
            if (definition.isSuggestEnabled()) {
                canHandleNativeFunction = indexingRuleBaseNodeType.equals(filter.getNodeType());
            } else {
                canHandleNativeFunction = false;
            }
        } else if (query.startsWith("spellcheck?term=")) {
            if (definition.isSpellcheckEnabled()) {
                canHandleNativeFunction = indexingRuleBaseNodeType.equals(filter.getNodeType());
            } else {
                canHandleNativeFunction = false;
            }
        }

        //Suggestion and SpellCheck use virtual paths which is same for all results
        if (canHandleNativeFunction) {
            result.disableUniquePaths();
        }

        //If native function can be handled by this index then ensure
        // that lowest cost if returned
        return canHandleNativeFunction ? defaultPlan().setEstimatedEntryCount(1) : null;
    }

    /**
     * Check if there is a mismatch between QueryPaths associated with index
     * and path restriction specified in query

     * @return true if QueryPaths and path restrictions do not have any conflict
     */
    private boolean checkForQueryPaths() {
        String[] queryPaths = definition.getQueryPaths();
        if (queryPaths == null){
            //No explicit value specified. Assume '/' which results in true
            return true;
        }

        String pathRestriction = filter.getPath();
        for (String queryPath : queryPaths){
            if (queryPath.equals(pathRestriction) || PathUtils.isAncestor(queryPath, pathRestriction)){
                return true;
            }
        }

        return false;
    }

    private boolean canEvalNodeNameRestriction(IndexingRule indexingRule) {
        PropertyRestriction pr = filter.getPropertyRestriction(QueryConstants.RESTRICTION_LOCAL_NAME);
        if (pr == null){
            return false;
        }
        return indexingRule.isNodeNameIndexed();
    }

    private static boolean canSortByProperty(List<OrderEntry> sortOrder) {
        if (sortOrder.isEmpty()) {
            return false;
        }

        // If jcr:score is the only sort order then opt out
        if (sortOrder.size() == 1 &&
                JCR_SCORE.equals(sortOrder.get(0).getPropertyName())) {
            return false;
        }

        return true;
    }

    private boolean canEvalAllFullText(final IndexingRule indexingRule, FullTextExpression ft) {
        if (ft == null){
            return false;
        }

        final HashSet<String> relPaths = new HashSet<String>();
        final HashSet<String> nonIndexedPaths = new HashSet<String>();
        final AtomicBoolean relativeParentsFound = new AtomicBoolean();
        final AtomicBoolean nodeScopedCondition = new AtomicBoolean();
        ft.accept(new FullTextVisitor.FullTextVisitorBase() {
            @Override
            public boolean visit(FullTextContains contains) {
                visitTerm(contains.getPropertyName());
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                visitTerm(term.getPropertyName());
                return true;
            }

            private void visitTerm(String propertyName) {
                String p = propertyName;
                String propertyPath = null;
                String nodePath = null;
                if (p == null) {
                    relPaths.add("");
                } else if (p.startsWith("../") || p.startsWith("./")) {
                    relPaths.add(p);
                    relativeParentsFound.set(true);
                } else if (getDepth(p) > 1) {
                    String parent = getParentPath(p);
                    if (LucenePropertyIndex.isNodePath(p)){
                        nodePath = parent;
                    } else {
                        propertyPath = p;
                    }
                    relPaths.add(parent);
                } else {
                    propertyPath = p;
                    relPaths.add("");
                }

                if (nodePath != null
                        && !indexingRule.isAggregated(nodePath)){
                    nonIndexedPaths.add(p);
                } else if (propertyPath != null) {
                    PropertyDefinition pd = indexingRule.getConfig(propertyPath);
                    //If given prop is not analyzed then its
                    //not indexed
                    if (pd == null){
                        nonIndexedPaths.add(p);
                    } else if (!pd.analyzed){
                        nonIndexedPaths.add(p);
                    }
                }

                if (nodeScopedTerm(propertyName)){
                    nodeScopedCondition.set(true);
                }
            }
        });

        if (nodeScopedCondition.get() && !indexingRule.isNodeFullTextIndexed()){
            return false;
        }

        if (relativeParentsFound.get()){
            log.debug("Relative parents found {} which are not supported", relPaths);
            return false;
        }

        //where contains('jcr:content/bar', 'mountain OR valley') and contains('jcr:content/foo', 'mountain OR valley')
        //above query can be evaluated by index which indexes foo and bar with restriction that both belong to same node
        //by displacing the query path to evaluate on contains('bar', ...) and filter out those parents which do not
        //have jcr:content as parent. So ensure that relPaths size is 1 or 0
        if (!nonIndexedPaths.isEmpty()){
            if (relPaths.size() > 1){
                log.debug("Following relative  property paths are not index", relPaths);
                return false;
            }
            result.setParentPath(Iterables.getOnlyElement(relPaths, ""));

            //Such non indexed path can possibly be evaluated via any rule on nt:base
            //which can possibly index everything
            IndexingRule rule = definition.getApplicableIndexingRule(NT_BASE);
            if (rule == null){
                return false;
            }

            for (String p : nonIndexedPaths){
                //Index can only evaluate a node search jcr:content/*
                //if it indexes node scope indexing is enabled
                if (LucenePropertyIndex.isNodePath(p)){
                    if (!rule.isNodeFullTextIndexed()) {
                        return false;
                    }
                } else {
                    //Index can only evaluate a property like jcr:content/type
                    //if it indexes 'type' and that too analyzed
                    String propertyName = PathUtils.getName(p);
                    PropertyDefinition pd = rule.getConfig(propertyName);
                    if (pd == null){
                        return false;
                    }
                    if (!pd.analyzed){
                        return false;
                    }
                }
            }
        } else {
            result.setParentPath("");
        }

        return true;
    }

    private boolean canEvalPathRestrictions(IndexingRule rule) {
        //Opt out if one is looking for all children for '/' as its equivalent to
        //NO_RESTRICTION
        if (filter.getPathRestriction() == Filter.PathRestriction.NO_RESTRICTION
                || (filter.getPathRestriction() == Filter.PathRestriction.ALL_CHILDREN
                        && PathUtils.denotesRoot(filter.getPath()))
                ){
            return false;
        }
        //If no other restrictions is provided and query is pure
        //path restriction based then need to be sure that index definition at least
        //allows indexing all the path for given nodeType
        return definition.evaluatePathRestrictions() && rule.indexesAllNodesOfMatchingType();
    }


    private boolean canEvalNodeTypeRestrictions(IndexingRule rule) {
        //No need to handle nt:base
        if (filter.matchesAllTypes()){
            return false;
        }

        //Only opt in if rule is not derived from nt:base otherwise it would
        //get used when there a full text index on all nodes
        return rule.indexesAllNodesOfMatchingType() && !rule.isBasedOnNtBase();
    }

    private IndexPlan.Builder defaultPlan() {
        return new IndexPlan.Builder()
                .setCostPerExecution(definition.getCostPerExecution())
                .setCostPerEntry(definition.getCostPerEntry())
                .setFulltextIndex(definition.isFullTextEnabled())
                .setIncludesNodeData(false) // we should not include node data
                .setFilter(filter)
                .setPathPrefix(getPathPrefix())
                .setDelayed(true) //Lucene is always async
                .setAttribute(LucenePropertyIndex.ATTR_PLAN_RESULT, result)
                .setEstimatedEntryCount(estimatedEntryCount())
                .setPlanName(indexPath);
    }

    private long estimatedEntryCount() {
        int numOfDocs = getReader().numDocs();
        if (useActualEntryCount) {
            return definition.isEntryCountDefined() ? definition.getEntryCount() : numOfDocs;
        } else {
            return estimatedEntryCount_Compat(numOfDocs);
        }
    }

    private long estimatedEntryCount_Compat(int numOfDocs) {
        //Other index only compete in case of property indexes. For fulltext
        //index return true count so as to allow multiple property indexes
        //to be compared fairly
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft != null && definition.isFullTextEnabled()){
            return definition.getFulltextEntryCount(numOfDocs);
        }
        return Math.min(definition.getEntryCount(), numOfDocs);
    }

    private String getPathPrefix() {
        // 2 = /oak:index/<index name>
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
            } else if (o.getPropertyName().equals(IndexDefinition.NATIVE_SORT_ORDER.getPropertyName())) {
                // Supports jcr:score descending natively
                orderEntries.add(IndexDefinition.NATIVE_SORT_ORDER);
            }
            for (PropertyDefinition functionIndex : rule.getFunctionRestrictions()) {
                if (o.getPropertyName().equals(functionIndex.function)) {
                    // Lucene can manage any order desc/asc
                    orderEntries.add(o);
                    result.sortedProperties.add(functionIndex);
                }
            }
        }

        //TODO Should we return order entries only when all order clauses are satisfied
        return orderEntries;
    }

    @CheckForNull
    private IndexingRule getApplicableRule() {
        if (filter.matchesAllTypes()){
            return definition.getApplicableIndexingRule(JcrConstants.NT_BASE);
        } else {
            //TODO May be better if filter.getSuperTypes returned a list which maintains
            //inheritance order and then we iterate over that
            for (IndexingRule rule : definition.getDefinedRules()){
                if (filter.getSupertypes().contains(rule.getNodeTypeName())){
                    //Theoretically there may be multiple rules for same nodeType with
                    //some condition defined. So again find a rule which applies
                    IndexingRule matchingRule = definition.getApplicableIndexingRule(rule.getNodeTypeName());

                    if (matchingRule == null && rule.getNodeTypeName().equals(filter.getNodeType())){
                        //In case nodetype registry in IndexDefinition is stale then it would not populate
                        //rules for new nodetype even though at indexing time it was able to index (due to
                        //use of latest nodetype reg nodestate)
                        //In such a case if the rule name and nodetype name for query matches then it is
                        //considered a match.
                        //This would though not work for the case where rule is related to nodetype as used
                        //in query matched via some inheritance chain
                        //TODO Need a way to check if nodetype reg as seen by IndexDefinition is old then
                        //IndexNode is reopened
                        matchingRule = rule;
                    }
                    if (matchingRule != null){
                        log.debug("Applicable IndexingRule found {}", matchingRule);
                        return rule;
                    }
                }
                //nt:base is applicable for all. This specific condition is
                //required to support mixin case as filter.getSupertypes() for mixin based
                //query only includes the mixin type and not nt:base
                if (rule.getNodeTypeName().equals(JcrConstants.NT_BASE)){
                    return rule;
                }
            }
            log.trace("No applicable IndexingRule found for any of the superTypes {}",
                filter.getSupertypes());
        }
        return null;
    }

    private boolean notSupportedFeature() {
        if(filter.getPathRestriction() == Filter.PathRestriction.NO_RESTRICTION
                && filter.matchesAllTypes()
                && filter.getPropertyRestrictions().isEmpty()) {
            //This mode includes name(), localname() queries
            //OrImpl [a/name] = 'Hello' or [b/name] = 'World'
            //Relative parent properties where [../foo1] is not null
            return true;
        }
        boolean failTestOnMissingFunctionIndex = true;
        if (failTestOnMissingFunctionIndex) {
            // this means even just function restrictions fail the test
            // (for example "where upper(name) = 'X'",
            // if a matching function-based index is missing
            return false;
        }
        // the following would ensure the test doesn't fail in that case:
        for (PropertyRestriction r : filter.getPropertyRestrictions()) {
            if (!r.propertyName.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX)) {
                // not a function restriction
                return false;
            }
        }
        return true;
    }

    /**
     * Determine if the propertyName of a fulltext term indicates current node
     * @param propertyName property name in the full text term clause
     */
    private static boolean nodeScopedTerm(String propertyName) {
        return propertyName == null || ".".equals(propertyName) || "*".equals(propertyName);
    }

    //~--------------------------------------------------------< PlanResult >

    public static class PlanResult {
        final String indexPath;
        final IndexDefinition indexDefinition;
        final IndexingRule indexingRule;
        private List<PropertyDefinition> sortedProperties = newArrayList();
        private Map<String, PropertyDefinition> propDefns = newHashMap();

        private boolean nonFullTextConstraints;
        private int parentDepth;
        private String parentPathSegment;
        private boolean relativize;
        private boolean nodeTypeRestrictions;
        private boolean nodeNameRestriction;
        private boolean uniquePathsRequired = true;

        public PlanResult(String indexPath, IndexDefinition defn, IndexingRule indexingRule) {
            this.indexPath = indexPath;
            this.indexDefinition = defn;
            this.indexingRule = indexingRule;
        }

        public PropertyDefinition getPropDefn(PropertyRestriction pr){
            return propDefns.get(pr.propertyName);
        }

        public boolean hasProperty(String propName){
            return propDefns.containsKey(propName);
        }

        public PropertyDefinition getOrderedProperty(int index){
            return sortedProperties.get(index);
        }

        public boolean isPathTransformed(){
            return relativize;
        }

        public boolean isUniquePathsRequired() {
            return uniquePathsRequired;
        }

        /**
         * Transforms the given path if the query involved relative properties and index
         * is not making use of aggregated properties. If the path
         *
         * @param path path to transform
         * @return transformed path. Returns null if the path does not confirm to relative
         * path requirements
         */
        @CheckForNull
        public String transformPath(String path){
            if (isPathTransformed()){
                // get the base path
                // ensure the path ends with the given
                // relative path
                if (!path.endsWith(parentPathSegment)) {
                    return null;
                }
                return getAncestorPath(path, parentDepth);
            }
            return path;
        }

        public boolean evaluateNonFullTextConstraints(){
            return nonFullTextConstraints;
        }

        public boolean evaluateNodeTypeRestriction() {
            return nodeTypeRestrictions;
        }

        public boolean evaluateNodeNameRestriction() {return nodeNameRestriction;}

        private void setParentPath(String relativePath){
            parentPathSegment = "/" + relativePath;
            if (relativePath.isEmpty()){
                // we only restrict non-full-text conditions if there is
                // no relative property in the full-text constraint
                enableNonFullTextConstraints();
            } else {
                relativize = true;
                parentDepth = getDepth(relativePath);
            }
        }

        private void enableNonFullTextConstraints(){
            nonFullTextConstraints = true;
        }

        private void enableNodeTypeEvaluation() {
            nodeTypeRestrictions = true;
        }

        private void enableNodeNameRestriction(){
            nodeNameRestriction = true;
        }

        private void disableUniquePaths(){
            uniquePathsRequired = false;
        }
    }
}
