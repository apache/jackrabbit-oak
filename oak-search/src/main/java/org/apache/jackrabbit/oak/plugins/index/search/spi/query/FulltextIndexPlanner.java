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
package org.apache.jackrabbit.oak.plugins.index.search.spi.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.jackrabbit.guava.common.collect.ArrayListMultimap;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Multimap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.StrictPathRestriction;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexSelectionPolicy;
import org.apache.jackrabbit.oak.plugins.index.property.ValuePatternUtil;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_SCORE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.commons.PathUtils.getAncestorPath;
import static org.apache.jackrabbit.oak.commons.PathUtils.getDepth;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

public class FulltextIndexPlanner {

    public static final int DEFAULT_PROPERTY_WEIGHT = Integer.getInteger("oak.fulltext.defaultPropertyWeight", 5);

    /**
     * IndexPlan Attribute name which refers to the name of the fields that should be used for facets.
     */
    public static final String ATTR_FACET_FIELDS = "oak.facet.fields";

    private static final String FLAG_ENTRY_COUNT = "oak.fulltext.useActualEntryCount";
    private static final Logger log = LoggerFactory.getLogger(FulltextIndexPlanner.class);
    private final IndexDefinition definition;
    private final Filter filter;
    private final String indexPath;
    protected final List<OrderEntry> sortOrder;
    private final IndexNode indexNode;
    protected PlanResult result;
    protected static boolean useActualEntryCount;
    private final boolean improvedIsNullCost;

    static {
        useActualEntryCount = Boolean.parseBoolean(System.getProperty(FLAG_ENTRY_COUNT, "true"));
        if (!useActualEntryCount) {
            log.info("System property {} found to be false. IndexPlanner would use a default entryCount of 1000 instead" +
                    " of using the actual entry count", FLAG_ENTRY_COUNT);
        }
    }

    public FulltextIndexPlanner(IndexNode indexNode,
                                String indexPath,
                                Filter filter, List<OrderEntry> sortOrder) {
        this.indexNode = indexNode;
        this.indexPath = indexPath;
        this.definition = indexNode.getDefinition();
        this.filter = filter;
        this.sortOrder = sortOrder;
        this.improvedIsNullCost = filter.getQueryLimits().getImprovedIsNullCost();
    }

    public IndexPlan getPlan() {
        if (definition == null) {
            log.debug("Index {} not loaded", indexPath);
            return null;
        }

        IndexPlan.Builder builder = getPlanBuilder();

        if (definition.isTestMode()){
            if (builder == null) {
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

    //For tests and since the property is anyway controllable by JVM param, so
    //public isn't very bad. Though, the real need to use 'public' is because
    //tests not in this package (happy to hear about other options)
    public static void setUseActualEntryCount(boolean useActualEntryCount) {
        FulltextIndexPlanner.useActualEntryCount = useActualEntryCount;
    }

    private IndexPlan.Builder getPlanBuilder() {
        log.trace("Evaluating plan with index definition {}", definition);

        if (wrongIndex()) {
            return null;
        }
        if (filter.getQueryLimits().getStrictPathRestriction().equals(StrictPathRestriction.ENABLE.name()) && !isPlanWithValidPathFilter()) {
            return null;
        }

        if (!definition.getVersion().isAtLeast(IndexFormatVersion.V2)){
            log.trace("Index is old format. Not supported");
            return null;
        }

        FullTextExpression ft = filter.getFullTextConstraint();
        //Query Fulltext and Index does not support fulltext
        if (ft != null && !definition.isFullTextEnabled()) {
            return null;
        }

        IndexDefinition.IndexingRule indexingRule = getApplicableRule();
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

        List<String> indexedProps = new ArrayList<>(filter.getPropertyRestrictions().size());

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
        boolean ntBaseRule = NT_BASE.equals(indexingRule.getNodeTypeName());
        Map<String, PropertyDefinition> relativePropDefns = new HashMap<>();
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

                    String facetProp = FulltextIndex.parseFacetField(value);

                    PropertyDefinition facetPropDef = indexingRule.getConfig(facetProp);
                    if (facetPropDef == null || !facetPropDef.facet) {
                        log.debug("{} not backed by index. Opting out", value);
                        return null;
                    }

                    facetFields.add(facetProp);
                }

                PropertyDefinition pd = indexingRule.getConfig(pr.propertyName);

                boolean relativeProps = false;
                if (pd == null && ntBaseRule) {
                    //Direct match not possible. Check for relative property definition
                    //i.e. if no match found for jcr:content/@keyword then check if
                    //property definition exists for 'keyword'
                    pd = getSimpleProperty(indexingRule, pr.propertyName);
                    relativeProps = pd != null;
                }

                if (pd != null && pd.propertyIndexEnabled()) {
                    if (pr.isNullRestriction() && !pd.nullCheckEnabled){
                        continue;
                    }

                    if (!matchesValuePattern(pr, pd)) {
                        continue;
                    }

                    //A property definition with weight == 0 is only meant to be used
                    //with some other definitions
                    if (pd.weight != 0 && !relativeProps) {
                        indexedProps.add(name);
                    }

                    if (relativeProps) {
                        relativePropDefns.put(name, pd);
                    } else {
                        result.propDefns.put(name, pd);
                    }
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

        if (indexedProps.isEmpty() && !relativePropDefns.isEmpty() && !canEvalAlFullText) {
            indexedProps = planForRelativeProperties(relativePropDefns);
        }

        //Fulltext expression can also be like jcr:contains(jcr:content/metadata/@format, 'image')

        List<OrderEntry> sortOrder = createSortOrder(indexingRule);
        boolean canSort = canSortByProperty(sortOrder);
        if (!indexedProps.isEmpty() || canSort || ft != null
                || evalPathRestrictions || evalNodeTypeRestrictions || canEvalNodeNameRestriction) {
            int costPerEntryFactor = 1;
            costPerEntryFactor += sortOrder.size();

            IndexPlan.Builder plan = defaultPlan();

            if (plan == null) {
                return null;
            }

            if (filter.getQueryLimits().getStrictPathRestriction().equals(StrictPathRestriction.WARN.name()) && !isPlanWithValidPathFilter()) {
                plan.setLogWarningForPathFilterMismatch(true);
            }

            Pattern queryFilterPattern = definition.getQueryFilterRegex() != null ? definition.getQueryFilterRegex() :
                    definition.getPropertyRegex();

            if (queryFilterPattern != null) {
                if (ft != null && !queryFilterPattern.matcher(ft.toString()).find()) {
                    plan.addAdditionalMessage("WARN", "Potentially improper use of index " + definition.getIndexPath() + " with queryFilterRegex "
                            + queryFilterPattern + " to search for value '" + ft + "'");
                }
                 for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
                	// Ignore properties beginning with ";" like :indexTag / :indexName etx
                    if (!pr.propertyName.startsWith(":") && !queryFilterPattern.matcher(pr.toString()).find()) {
                        plan.addAdditionalMessage("WARN", "Potentially improper use of index " + definition.getIndexPath() + " with queryFilterRegex "
                                + queryFilterPattern + " to search for value '" + pr + "'");
                    }
                }
            }


            if (!sortOrder.isEmpty()) {
                plan.setSortOrder(sortOrder);
            }

            if (facetFields.size() > 0) {
                plan.setAttribute(ATTR_FACET_FIELDS, facetFields);
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

            // Set a index based guess here. Unique would set its own value below
            if (useActualEntryCount && !definition.isEntryCountDefined()) {
                int maxPossibleNumDocs = getMaxPossibleNumDocs(result.propDefns, filter);
                if (maxPossibleNumDocs >= 0) {
                    plan.setEstimatedEntryCount(maxPossibleNumDocs);
                }
            }

            if (sortOrder.isEmpty() && ft == null) {
                boolean uniqueIndexFound = planForSyncIndexes(indexingRule);
                if (uniqueIndexFound) {
                    //For unique index there would be at max 1 entry
                    plan.setEstimatedEntryCount(1);
                }
            }

            return plan.setCostPerEntry(definition.getCostPerEntry() / costPerEntryFactor);
        }

        //TODO Support for property existence queries

        return null;
    }

    private boolean isPlanWithValidPathFilter() {
        String pathFilter = filter.getPath();
        PathFilter definitionPathFilter = definition.getPathFilter();
        return definitionPathFilter.areAllDescendantsIncluded(pathFilter);
    }

    private boolean matchesValuePattern(PropertyRestriction pr, PropertyDefinition pd) {
        if (!pd.valuePattern.matchesAll()){
            //So we have a valuePattern defined. So determine if
            //this index can return a plan based on values
            Set<String> values = ValuePatternUtil.getAllValues(pr);
            if (values == null) {
                // "is not null" condition, but we have a value pattern
                // that doesn't match everything
                // case of like search
                String prefix = ValuePatternUtil.getLongestPrefix(filter, pr.propertyName);
                return pd.valuePattern.matchesPrefix(prefix);
            } else {
                // we have a value pattern, for example (a|b),
                // but we search (also) for 'c': can't match
                return pd.valuePattern.matchesAll(values);
            }
        }
        return true;
    }

    private boolean wrongIndex() {
        // REMARK: similar code is used in oak-core, PropertyIndex
        // skip index if "option(index ...)" doesn't match
        if (!definition.isEnabled()) {
            return true;
        }

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
        } else if (IndexSelectionPolicy.TAG.equals(definition.getIndexSelectionPolicy())) {
            // index tags are not specified in query, but required by the "tag" index selection policy
            return true;
        }
        // no tag specified
        return wrong;
    }

    private IndexPlan.Builder getNativeFunctionPlanBuilder(String indexingRuleBaseNodeType) {
        boolean canHandleNativeFunction = true;
        PropertyValue pv = filter.getPropertyRestriction(definition.getFunctionName()).first;
        String query = pv.getValue(Type.STRING);
        // Suggestion and SpellCheck use virtual paths which are same for all results
        // so we must disable later filtering of unique paths
        if (query.startsWith("suggest?term=")) {
            if (definition.isSuggestEnabled()) {
                canHandleNativeFunction = indexingRuleBaseNodeType.equals(filter.getNodeType());
                result.disableUniquePaths();
            } else {
                canHandleNativeFunction = false;
            }
        } else if (query.startsWith("spellcheck?term=")) {
            if (definition.isSpellcheckEnabled()) {
                canHandleNativeFunction = indexingRuleBaseNodeType.equals(filter.getNodeType());
                result.disableUniquePaths();
            } else {
                canHandleNativeFunction = false;
            }
        }

        if (!canHandleNativeFunction) {
            return null;
        }
        IndexPlan.Builder b = defaultPlan();
        if (b == null) {
            return null;
        }
        // If native function can be handled by this index, then ensure
        // that lowest cost is returned
        return b.setEstimatedEntryCount(1);
    }

    /*
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
        return sortOrder.size() != 1 ||
                !JCR_SCORE.equals(sortOrder.get(0).getPropertyName());
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
                    if (FulltextIndex.isNodePath(p)){
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
                log.debug("Following relative property paths are not index: {}", relPaths);
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
                if (FulltextIndex.isNodePath(p)){
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

    /*
     * Computes the indexedProps which can be part of query by virtue of relativizing i.e.
     * if query is on jcr:content/keyword then perform search on keyword and change parent
     * path to jcr:content
     * @param relativePropDefns property definitions for such relative properties. The key
     *                          would be actual property name as in query i.e. jcr:content/keyword
     *                          while property definition would be for 'keyword'
     * @return list of properties which are included in query issued to Lucene
     */
    private List<String> planForRelativeProperties(Map<String, PropertyDefinition> relativePropDefns) {
        Multimap<String, Map.Entry<String, PropertyDefinition>> relpaths = ArrayListMultimap.create();
        int maxSize = 0;
        String maxCountedParent = null;

        //Collect the relative properties grouped by parent path
        //and track the parent having maximum properties
        for (Map.Entry<String, PropertyDefinition> e : relativePropDefns.entrySet()) {
            String relativePropertyPath = e.getKey();
            String parent = getParentPath(relativePropertyPath);

            relpaths.put(parent, e);
            int count = relpaths.get(parent).size();
            if (count > maxSize) {
                maxSize = count;
                maxCountedParent = parent;
            }
        }

        //Set the parent path to one which is present in most prop. In case of tie any one
        //such path would be picked
        result.setParentPath(maxCountedParent);

        //Now add only those properties to plan which have the maxCountedParent
        List<String> indexedProps = new ArrayList<>(maxSize);
        for (Map.Entry<String, PropertyDefinition> e : relpaths.get(maxCountedParent)) {
            String relativePropertyPath = e.getKey();
            result.propDefns.put(relativePropertyPath, e.getValue());
            result.relPropMapping.put(relativePropertyPath, PathUtils.getName(relativePropertyPath));
            if (e.getValue().weight != 0) {
                indexedProps.add(relativePropertyPath);
            }
        }

        return indexedProps;
    }

    @Nullable
    private static PropertyDefinition getSimpleProperty(IndexingRule indexingRule, String relativePropertyName) {
        String name = PathUtils.getName(relativePropertyName);
        if (name.equals(relativePropertyName)){
            //Not a relative property
            return null;
        }

        //Properties using ../ or ./ notation not support. The relative property path
        //must be fixed
        if (relativePropertyName.startsWith("../") || relativePropertyName.startsWith("./")) {
            return null;
        }
        return indexingRule.getConfig(name);
    }

    private boolean planForSyncIndexes(IndexDefinition.IndexingRule indexingRule) {
        //If no sync index involved then return right away
        if (!definition.hasSyncPropertyDefinitions()) {
            return false;
        }

        if (result.propDefns.isEmpty() && !result.evaluateNodeTypeRestriction()) {
            return false;
        }

        List<PropertyIndexResult> unique = new ArrayList<>();
        List<PropertyIndexResult> nonUnique = new ArrayList<>();

        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String propertyName = result.getPropertyName(pr);
            PropertyDefinition pd = result.propDefns.get(pr.propertyName);

            if (pd != null) {
                PropertyIndexResult e = new PropertyIndexResult(propertyName, pr);
                if (pd.unique) {
                    unique.add(e);
                } else {
                    nonUnique.add(e);
                }
            }
        }

        //Pick the first index (if multiple). For unique its fine
        //For non unique we can probably later add support for cost
        //based selection
        boolean uniqueIndexFound = false;
        if (!unique.isEmpty()) {
            result.propertyIndexResult = unique.get(0);
            uniqueIndexFound = true;
        } else if (!nonUnique.isEmpty()) {
            result.propertyIndexResult = nonUnique.get(0);
        }

        if (result.propertyIndexResult == null && result.evaluateNodeTypeRestriction()) {
            PropertyDefinition pd = indexingRule.getConfig(JcrConstants.JCR_PRIMARYTYPE);
            if (pd != null && pd.sync) {
                result.syncNodeTypeRestrictions = true;
            }
        }

        return uniqueIndexFound;
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

    @Nullable
    private IndexPlan.Builder defaultPlan() {
        // With OAK-7947 lucene indexes return a non-null index node to delay reading index files
        // While IndexNode could have a status check method but for now we are using this work-around
        // to check null on {@code getIndexStatistics()} as proxy indicator
        // (this could be avoided by returning lazy statistics)
        if (indexNode.getIndexStatistics() == null) {
            return null;
        }

        return new IndexPlan.Builder()
                .setCostPerExecution(definition.getCostPerExecution())
                .setCostPerEntry(definition.getCostPerEntry())
                .setFulltextIndex(definition.isFullTextEnabled())
                .setIncludesNodeData(false) // we should not include node data
                .setFilter(filter)
                .setPathPrefix(getPathPrefix())
                .setSupportsPathRestriction(definition.evaluatePathRestrictions())
                .setDelayed(true) //Lucene is always async
                .setDeprecated(definition.isDeprecated())
                .setAttribute(FulltextIndex.ATTR_PLAN_RESULT, result)
                .setEstimatedEntryCount(estimatedEntryCount())
                .setPlanName(indexPath);
    }

    private long estimatedEntryCount() {
        if (useActualEntryCount) {
            if (definition.isEntryCountDefined()) {
                return definition.getEntryCount();
            }
            return  getNumDocs();
        } else {
            return estimatedEntryCount_Compat(getNumDocs());
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

    private int getNumDocs() {
        IndexStatistics indexStatistics = indexNode.getIndexStatistics();
        if (indexStatistics == null) {
            log.warn("Statistics not available - possibly index is corrupt? Returning high doc count");
            return Integer.MAX_VALUE;
        }
        return indexStatistics.numDocs();
    }

    private int getMaxPossibleNumDocs(Map<String, PropertyDefinition> propDefns, Filter filter) {
        IndexStatistics indexStatistics = indexNode.getIndexStatistics();
        if (indexStatistics == null) {
            log.warn("Statistics not available - possibly index is corrupt? Returning high doc count");
            return Integer.MAX_VALUE;
        }
        int minNumDocs = indexStatistics.numDocs();
        for (Map.Entry<String, PropertyDefinition> propDef : propDefns.entrySet()) {
            String key = propDef.getKey();
            if (result.relPropMapping.containsKey(key)) {
                key = getName(key);
            }
            PropertyRestriction pr = filter.getPropertyRestriction(key);
            String fieldName = key;
            // for "is not null" we can use an asterisk query
            if (improvedIsNullCost) {
                if (pr != null && pr.isNullRestriction()) {
                    fieldName = FieldNames.NULL_PROPS;
                }
            }
            int docCntForField = indexStatistics.getDocCountFor(fieldName);
            if (docCntForField == -1) {
                continue;
            }

            int weight = propDef.getValue().weight;

            if (pr != null) {
                if (pr.isNotNullRestriction()) {
                    // don't use weight for "is not null" restrictions
                    // as all documents with this field can match;
                    weight = 1;
                } else if (improvedIsNullCost && pr.isNullRestriction()) {
                    // don't use the weight for "is null" restrictions
                    // as all documents with ":nullProps" can match
                    weight = 1;
                } else {
                    if (weight > 1) {
                        // for non-equality conditions such as
                        // where x > 1, x < 2, x like y,...:
                        // use a maximum weight of 3,
                        // so assume we read at least 30%
                        if (!isEqualityRestriction(pr)) {
                            weight = Math.min(3, weight);
                        }
                    }
                }
            }

            if (weight > 1) {
                // use it to scale down the doc count - in broad strokes, we can think of weight
                // as number of terms for the field with all terms getting equal share of
                // the documents in this field
                double scaledDocCnt = Math.ceil((double) docCntForField / weight);
                if (minNumDocs < scaledDocCnt) {
                    continue;
                }
                // since, we've already taken care that scaled cost is lower than minCost,
                // we can safely cast without risking overflow
                minNumDocs = (int)scaledDocCnt;
            } else if (docCntForField < minNumDocs) {
                minNumDocs = docCntForField;
            }
        }

        // Reduce the estimation if the index supports path restrictions,
        // and we have one that we can use.
        // We don't need to be exact here, because we don't know the
        // exact number of nodes in the subtree - this is taken care of
        // in the query engine.
        // But we need to adjust the cost a bit, so that
        // the cost is lower if there is a usable path condition
        // (see below).
        if (definition.evaluatePathRestrictions()) {
            PathRestriction pathRestriction = filter.getPathRestriction();
            if (pathRestriction == PathRestriction.EXACT || pathRestriction == PathRestriction.PARENT) {
                // We have an exact path restriction and we have an index that indexes the path:
                // then the result size is at most 1.
                minNumDocs = 1;
            } else if (pathRestriction == PathRestriction.DIRECT_CHILDREN) {
                // We restrict to direct children: assume at most 50% are there.
                minNumDocs /= 2;
            } else if (pathRestriction != PathRestriction.NO_RESTRICTION) {
                // Other restriction: assume at most 90%.
                // This is important if we have
                // "descendantnode(/content/abc) or issamenode(/content/abc)":
                // We could either convert it to a union, or not use the path restriction.
                // In this case, we want to convert it to a union!
                // A path restriction will reduce the number of entries.
                // So the cost of having a usable path condition is
                // lower than the cost of not having a path condition at all.
                minNumDocs = (int) (minNumDocs * 0.9);
            }
        }
        return minNumDocs;
    }

    private static boolean isEqualityRestriction(PropertyRestriction pr) {
        return pr.first != null && pr.first == pr.last;
    }

    protected List<OrderEntry> createSortOrder(IndexDefinition.IndexingRule rule) {
        if (sortOrder == null) {
            return Collections.emptyList();
        }

        List<OrderEntry> orderEntries = new ArrayList<>(sortOrder.size());
        for (OrderEntry o : sortOrder) {
            PropertyDefinition pd = rule.getConfig(o.getPropertyName());
            if (pd != null
                    && pd.ordered
                    && o.getPropertyType() != null
                    && !o.getPropertyType().isArray()) {
                orderEntries.add(o); // can manage any order desc/asc
                result.sortedProperties.add(pd);
            } else if (o.getPropertyName().equals(IndexDefinition.NATIVE_SORT_ORDER.getPropertyName())) {
                // Supports jcr:score descending natively
                orderEntries.add(IndexDefinition.NATIVE_SORT_ORDER);
            }
            for (PropertyDefinition functionIndex : rule.getFunctionRestrictions()) {
                if (functionIndex.ordered && o.getPropertyName().equals(functionIndex.function)) {
                    // can manage any order desc/asc
                    orderEntries.add(o);
                    result.sortedProperties.add(functionIndex);
                }
            }
        }

        //TODO Should we return order entries only when all order clauses are satisfied
        return orderEntries;
    }

    @Nullable
    private IndexDefinition.IndexingRule getApplicableRule() {
        if (filter.matchesAllTypes()){
            return definition.getApplicableIndexingRule(JcrConstants.NT_BASE);
        } else {
            //TODO May be better if filter.getSuperTypes returned a list which maintains
            //inheritance order and then we iterate over that
            for (IndexDefinition.IndexingRule rule : definition.getDefinedRules()){
                if (filter.getSupertypes().contains(rule.getNodeTypeName())){
                    //Theoretically there may be multiple rules for same nodeType with
                    //some condition defined. So again find a rule which applies
                    IndexDefinition.IndexingRule matchingRule = definition.getApplicableIndexingRule(rule.getNodeTypeName());

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
                        log.trace("Applicable IndexingRule found {}", matchingRule);
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
            if (log.isTraceEnabled()) {
                log.trace("No applicable IndexingRule found for any of the superTypes {}",
                        filter.getSupertypes());
            }
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

    /*
     * Determine if the propertyName of a fulltext term indicates current node
     * @param propertyName property name in the full text term clause
     */
    private static boolean nodeScopedTerm(String propertyName) {
        return propertyName == null || ".".equals(propertyName) || "*".equals(propertyName);
    }

    //~--------------------------------------------------------< PlanResult >

    public static class PlanResult {
        public final String indexPath;
        public final IndexDefinition indexDefinition;
        public final IndexDefinition.IndexingRule indexingRule;
        private final List<PropertyDefinition> sortedProperties = new ArrayList<>();

        //Map of actual property name as present in our property definitions
        private final Map<String, PropertyDefinition> propDefns = new HashMap<>();

        //Map of property restriction name -> property definition name
        //like 'jcr:content/status' -> 'status'
        private final Map<String, String> relPropMapping = new HashMap<>();

        private boolean nonFullTextConstraints;
        private int parentDepth;
        private String parentPathSegment;
        private boolean relativize;
        private boolean nodeTypeRestrictions;
        private boolean nodeNameRestriction;
        private boolean uniquePathsRequired = true;
        private PropertyIndexResult propertyIndexResult;
        private boolean syncNodeTypeRestrictions;

        public PlanResult(String indexPath, IndexDefinition defn, IndexDefinition.IndexingRule indexingRule) {
            this.indexPath = indexPath;
            this.indexDefinition = defn;
            this.indexingRule = indexingRule;
        }

        public PropertyDefinition getPropDefn(PropertyRestriction pr){
            return propDefns.get(pr.propertyName);
        }

        /**
         * Returns the property name to be used for query for given PropertyRestriction
         * The name can be same as one for property restriction or it can be a mapped one
         */
        public String getPropertyName(PropertyRestriction pr) {
            return relPropMapping.getOrDefault(pr.propertyName, pr.propertyName);
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

        public int getParentDepth() {
            return parentDepth;
        }

        public String getParentPathSegment() {
            return parentPathSegment;
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
        @Nullable
        public String transformPath(String path){
            if (isPathTransformed()){
                // get the base path ensure the path ends with the given relative path
                // for fulltext constraint.
                // For non-fulltext constraint where query engine can evaluate the relative path
                // condition, we shall take leeway and allow other features of query engine
                // (like wildcard as a path element) get supported
                if ( (!nonFullTextConstraints && !path.endsWith(parentPathSegment))
                        || (nonFullTextConstraints && getDepth(path) < parentDepth) ) {
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

        public boolean evaluateSyncNodeTypeRestriction() {
            return syncNodeTypeRestrictions;
        }

        public boolean evaluateNodeNameRestriction() {return nodeNameRestriction;}

        @Nullable
        public PropertyIndexResult getPropertyIndexResult() {
            return propertyIndexResult;
        }

        public boolean hasPropertyIndexResult(){
            return propertyIndexResult != null;
        }

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

        public void disableUniquePaths() {
            uniquePathsRequired = false;
        }
    }

    public static class PropertyIndexResult {
        public final String propertyName;
        public final PropertyRestriction pr;

        public PropertyIndexResult(String propertyName, PropertyRestriction pr) {
            this.propertyName = propertyName;
            this.pr = pr;
        }
    }
}
