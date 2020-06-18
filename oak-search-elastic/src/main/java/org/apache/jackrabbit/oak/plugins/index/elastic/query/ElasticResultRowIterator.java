/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.facets.ElasticAggregationData;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.facets.ElasticFacetHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.facets.ElasticFacets;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticQueryUtil;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.search.util.LMSEstimator;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newMixinTypeQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newNodeTypeQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newNotNullPropQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newNullPropQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPrefixPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPrefixQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPropertyRestrictionQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newWildcardPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newWildcardQuery;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_PATH;
import static org.apache.jackrabbit.util.ISO8601.parse;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

class ElasticResultRowIterator implements Iterator<FulltextIndex.FulltextResultRow> {
    private static final Logger LOG = LoggerFactory
            .getLogger(ElasticResultRowIterator.class);

    private final static String SPELLCHECK_PREFIX = "spellcheck?term=";
    // TODO: oak-lucene gets this via WildcardQuery class. See if ES also exposes these consts
    private static final char WILDCARD_STRING = '*';
    private static final char WILDCARD_CHAR = '?';
    private final ElasticRowIteratorState rowIteratorState;

    ElasticResultRowIterator(@NotNull Filter filter,
                             @NotNull FulltextIndexPlanner.PlanResult planResult,
                             @NotNull QueryIndex.IndexPlan plan,
                             ElasticIndexNode indexNode,
                             RowInclusionPredicate rowInclusionPredicate,
                             LMSEstimator estimator) {
        this.rowIteratorState = new ElasticRowIteratorState(filter, planResult,
                plan, indexNode, rowInclusionPredicate, estimator);
    }

    @Override
    public boolean hasNext() {
        return !rowIteratorState.queue.isEmpty() || loadDocs();
    }

    @Override
    public FulltextIndex.FulltextResultRow next() {
        return rowIteratorState.queue.remove();
    }

    /**
     * Loads the lucene documents in batches
     *
     * @return true if any document is loaded
     */
    private boolean loadDocs() {

        if (rowIteratorState.isLastDoc) {
            return false;
        }

        if (rowIteratorState.indexNode == null) {
            throw new IllegalStateException("indexNode cannot be null");
        }

        SearchHit lastDocToRecord = null;
        try {
            ElasticProcess elasticProcess = getElasticProcess(rowIteratorState.plan, rowIteratorState.planResult);

            lastDocToRecord = elasticProcess.process();

            // TODO: suggest } else if (luceneRequestFacade.getLuceneRequest() instanceof SuggestHelper.SuggestQuery) {
        } catch (Exception e) {
            LOG.warn("query via {} failed.", this, e);
        } finally {
            rowIteratorState.indexNode.release();
        }

        if (lastDocToRecord != null) {
            this.rowIteratorState.lastIteratedDoc = lastDocToRecord;
        }

        return !rowIteratorState.queue.isEmpty();
    }

    public interface RowInclusionPredicate {
        boolean shouldInclude(@NotNull String path, @NotNull QueryIndex.IndexPlan plan);

        RowInclusionPredicate NOOP = (@NotNull String path, @NotNull QueryIndex.IndexPlan plan) -> true;
    }

    /**
     * Get the Elasticsearch query for the given filter.
     *
     * @param plan       index plan containing filter details
     * @param planResult
     * @return the Lucene query
     */
    private ElasticProcess getElasticProcess(IndexPlan plan, PlanResult planResult) {
        List<QueryBuilder> qs = new ArrayList<>();
        Filter filter = plan.getFilter();
        FullTextExpression ft = filter.getFullTextConstraint();
        ElasticIndexDefinition defn = (ElasticIndexDefinition) planResult.indexDefinition;

        if (ft != null) {
            qs.add(getFullTextQuery(ft, planResult));
        } else {
            // there might be no full-text constraint
            // when using the LowCostLuceneIndexProvider
            // which is used for testing
        }


        //Check if native function is supported
        Filter.PropertyRestriction pr = null;
        if (defn.hasFunctionDefined()) {
            pr = filter.getPropertyRestriction(defn.getFunctionName());
        }

        if (pr != null) {
            String query = String.valueOf(pr.first.getValue(pr.first.getType()));
            // TODO: more like this

            // TODO: spellcheck
            if (query.startsWith(SPELLCHECK_PREFIX)) {
                return new ElasticSpellcheckProcess(query, rowIteratorState);
            }

            // TODO: suggest

            qs.add(QueryBuilders.queryStringQuery(query));
        } else if (planResult.evaluateNonFullTextConstraints()) {
            addNonFullTextConstraints(qs, plan, planResult);
        }

        // TODO: sort with no other restriction

        if (qs.size() == 0) {

            // TODO: what happens here in planning mode (specially, apparently for things like rep:similar)

            //For purely nodeType based queries all the documents would have to
            //be returned (if the index definition has a single rule)
            if (planResult.evaluateNodeTypeRestriction()) {
                return new ElasticQueryProcess(matchAllQuery(), rowIteratorState, new ElasticFacetProvider());
            }

            throw new IllegalStateException("No query created for filter " + filter);
        }
        ElasticProcess elasticProcess = new ElasticQueryProcess(ElasticQueryUtil.performAdditionalWraps(qs),
                rowIteratorState, new ElasticFacetProvider());
        return elasticProcess;
    }

    private static QueryBuilder getFullTextQuery(FullTextExpression ft, final PlanResult pr) {
        // a reference to the query, so it can be set in the visitor
        // (a "non-local return")
        final AtomicReference<QueryBuilder> result = new AtomicReference<>();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextContains contains) {
                visitTerm(contains.getPropertyName(), contains.getRawText(), null, contains.isNot());
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                BoolQueryBuilder q = boolQuery();
                for (FullTextExpression e : or.list) {
                    QueryBuilder x = getFullTextQuery(e, pr);
                    q.should(x);
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BoolQueryBuilder q = boolQuery();
                for (FullTextExpression e : and.list) {
                    QueryBuilder x = getFullTextQuery(e, pr);
                    // TODO: see OAK-2434 and see if ES also can't work without unwrapping
                    /* Only unwrap the clause if MUST_NOT(x) */
                    boolean hasMustNot = false;
                    if (x instanceof BoolQueryBuilder) {
                        BoolQueryBuilder bq = (BoolQueryBuilder) x;
                        if (bq.mustNot().size() == 1
                                // no other clauses
                                && bq.should().isEmpty() && bq.must().isEmpty() && bq.filter().isEmpty()) {
                            hasMustNot = true;
                            q.mustNot(bq.mustNot().get(0));
                        }
                    }

                    if (!hasMustNot) {
                        q.must(x);
                    }
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                return visitTerm(term.getPropertyName(), term.getText(), term.getBoost(), term.isNot());
            }

            private boolean visitTerm(String propertyName, String text, String boost, boolean not) {
                String p = getLuceneFieldName(propertyName, pr);
                QueryBuilder q = tokenToQuery(text, p, pr);
                if (boost != null) {
                    q.boost(Float.parseFloat(boost));
                }
                if (not) {
                    BoolQueryBuilder bq = boolQuery();
                    bq.mustNot(q);
                    result.set(bq);
                } else {
                    result.set(q);
                }
                return true;
            }
        });
        return result.get();
    }

    private static QueryBuilder tokenToQuery(String text, String fieldName, PlanResult pr) {
        QueryBuilder ret;
        IndexDefinition.IndexingRule indexingRule = pr.indexingRule;
        //Expand the query on fulltext field
        if (FieldNames.FULLTEXT.equals(fieldName) &&
                !indexingRule.getNodeScopeAnalyzedProps().isEmpty()) {
            BoolQueryBuilder in = boolQuery();
            for (PropertyDefinition pd : indexingRule.getNodeScopeAnalyzedProps()) {
                QueryBuilder q = matchQuery(FieldNames.createAnalyzedFieldName(pd.name), text);
                q.boost(pd.boost);
                in.should(q);
            }

            //Add the query for actual fulltext field also. That query would
            //not be boosted
            in.should(matchQuery(fieldName, text));
            ret = in;
        } else {
            ret = matchQuery(fieldName, text);
        }

        return ret;
    }

    private static String getLuceneFieldName(@Nullable String p, PlanResult pr) {
        if (p == null) {
            return FieldNames.FULLTEXT;
        }

        if (pr.isPathTransformed()) {
            p = PathUtils.getName(p);
        }

        if ("*".equals(p)) {
            p = FieldNames.FULLTEXT;
        }
        return p;
    }

    private void addNonFullTextConstraints(List<QueryBuilder> qs,
                                           IndexPlan plan, PlanResult planResult) {
        Filter filter = plan.getFilter();
        if (!filter.matchesAllTypes()) {
            addNodeTypeConstraints(planResult.indexingRule, qs, filter);
        }

        qs.addAll(ElasticQueryUtil.getPathRestrictionQuery(plan, planResult, filter));
        for (Filter.PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String name = pr.propertyName;

            if (QueryConstants.REP_EXCERPT.equals(name) || QueryConstants.OAK_SCORE_EXPLANATION.equals(name)
                    || QueryConstants.REP_FACET.equals(name)) {
                continue;
            }

            if (QueryConstants.RESTRICTION_LOCAL_NAME.equals(name)) {
                if (planResult.evaluateNodeNameRestriction()) {
                    QueryBuilder q = createNodeNameQuery(pr);
                    if (q != null) {
                        qs.add(q);
                    }
                }
                continue;
            }

            if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                    && pr.lastIncluding) {
                String first = pr.first.getValue(Type.STRING);
                first = first.replace("\\", "");
                if (JCR_PATH.equals(name)) {
                    qs.add(newPathQuery(first));
                    continue;
                } else if ("*".equals(name)) {
                    //TODO Revisit reference constraint. For performant impl
                    //references need to be indexed in a different manner
                    addReferenceConstraint(first, qs);
                    continue;
                }
            }

            PropertyDefinition pd = planResult.getPropDefn(pr);
            if (pd == null) {
                continue;
            }

            QueryBuilder q = createQuery(planResult.getPropertyName(pr), pr, pd);
            if (q != null) {
                qs.add(q);
            }
        }
    }


    private static void addNodeTypeConstraints(IndexDefinition.IndexingRule defn, List<QueryBuilder> qs, Filter filter) {
        BoolQueryBuilder bq = boolQuery();
        PropertyDefinition primaryType = defn.getConfig(JCR_PRIMARYTYPE);
        //TODO OAK-2198 Add proper nodeType query support

        if (primaryType != null && primaryType.propertyIndex) {
            for (String type : filter.getPrimaryTypes()) {
                bq.should(newNodeTypeQuery(type));
            }
        }

        PropertyDefinition mixinType = defn.getConfig(JCR_MIXINTYPES);
        if (mixinType != null && mixinType.propertyIndex) {
            for (String type : filter.getMixinTypes()) {
                bq.should(newMixinTypeQuery(type));
            }
        }

        if (bq.hasClauses()) {
            qs.add(bq);
        }
    }

    private static QueryBuilder createNodeNameQuery(Filter.PropertyRestriction pr) {
        String first = pr.first != null ? pr.first.getValue(Type.STRING) : null;
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                && pr.lastIncluding) {
            // [property]=[value]
            return termQuery(FieldNames.NODE_NAME, first);
        }

        if (pr.isLike) {
            return createLikeQuery(FieldNames.NODE_NAME, first);
        }

        throw new IllegalStateException("For nodeName queries only EQUALS and LIKE are supported " + pr);
    }

    private static QueryBuilder createLikeQuery(String name, String first) {
        first = first.replace('%', WILDCARD_STRING);
        first = first.replace('_', WILDCARD_CHAR);

        QueryBuilders.wildcardQuery(name, first);

        int indexOfWS = first.indexOf(WILDCARD_STRING);
        int indexOfWC = first.indexOf(WILDCARD_CHAR);
        int len = first.length();

        if (indexOfWS == len || indexOfWC == len) {
            // remove trailing "*" for prefixquery
            first = first.substring(0, first.length() - 1);
            if (JCR_PATH.equals(name)) {
                return newPrefixPathQuery(first);
            } else {
                return newPrefixQuery(name, first);
            }
        } else {
            if (JCR_PATH.equals(name)) {
                return newWildcardPathQuery(first);
            } else {
                return newWildcardQuery(name, first);
            }
        }
    }

    private static void addReferenceConstraint(String uuid, List<QueryBuilder> qs) {
        // TODO: this seems very bad as a query - do we really want to support it. In fact, is it even used?
        // reference query
        qs.add(QueryBuilders.multiMatchQuery(uuid));
    }

    @Nullable
    private QueryBuilder createQuery(String propertyName, Filter.PropertyRestriction pr,
                                     PropertyDefinition defn) {
        int propType = FulltextIndex.determinePropertyType(defn, pr);

        if (pr.isNullRestriction()) {
            return newNullPropQuery(defn.name);
        }

        //If notNullCheckEnabled explicitly enabled use the simple TermQuery
        //otherwise later fallback to range query
        if (pr.isNotNullRestriction() && defn.notNullCheckEnabled) {
            return newNotNullPropQuery(defn.name);
        }

        final String field = rowIteratorState.indexNode.getDefinition().getElasticKeyword(propertyName);

        QueryBuilder in;
        switch (propType) {
            case PropertyType.DATE: {
                in = newPropertyRestrictionQuery(field, pr, value -> parse(value.getValue(Type.DATE)).getTime());
                break;
            }
            case PropertyType.DOUBLE: {
                in = newPropertyRestrictionQuery(field, pr, value -> value.getValue(Type.DOUBLE));
                break;
            }
            case PropertyType.LONG: {
                in = newPropertyRestrictionQuery(field, pr, value -> value.getValue(Type.LONG));
                break;
            }
            default: {
                if (pr.isLike) {
                    return createLikeQuery(propertyName, pr.first.getValue(Type.STRING));
                }

                //TODO Confirm that all other types can be treated as string
                in = newPropertyRestrictionQuery(field, pr, value -> value.getValue(Type.STRING));
            }
        }

        if (in != null) {
            return in;
        }

        throw new IllegalStateException("PropertyRestriction not handled " + pr + " for index " + defn);
    }

    class ElasticFacetProvider implements FulltextIndex.FacetProvider {
        private ElasticFacets elasticFacets;
        private Map<String, List<FulltextIndex.Facet>> cachedResults = new HashMap<>();
        private AtomicBoolean isInitialized = new AtomicBoolean();

        @Override
        public List<FulltextIndex.Facet> getFacets(int numberOfFacets, String columnName) throws IOException {
            if (isInitiliazed()) {
                String facetProp = FulltextIndex.parseFacetField(columnName);
                if (cachedResults.get(facetProp) == null) {
                    cachedResults = elasticFacets.getFacets(rowIteratorState.indexNode.getDefinition(), numberOfFacets);
                }
                return cachedResults.get(facetProp);
            } else {
                LOG.error("FacetProvider not initialized");
            }
            return Collections.emptyList();
        }

        public boolean isInitiliazed() {
            return isInitialized.get();
        }

        public void initialize(ElasticFacets elasticFacets) {
            isInitialized.set(true);
            this.elasticFacets = elasticFacets;
        }
    }

    static class ElasticRowIteratorState {

        private final Deque<FulltextIndex.FulltextResultRow> queue = new ArrayDeque<>();
        // TODO : find if ES can return dup docs - if so how to avoid
        SearchHit lastIteratedDoc;
        private boolean isLastDoc = false;
        private final Filter filter;
        private final FulltextIndexPlanner.PlanResult planResult;
        private final QueryIndex.IndexPlan plan;
        private final ElasticIndexNode indexNode;
        private final ElasticResultRowIterator.RowInclusionPredicate rowInclusionPredicate;
        private final LMSEstimator estimator;

        private ElasticRowIteratorState(Filter filter, FulltextIndexPlanner.PlanResult planResult,
                                        QueryIndex.IndexPlan plan, ElasticIndexNode indexNode,
                                        ElasticResultRowIterator.RowInclusionPredicate rowInclusionPredicate,
                                        LMSEstimator estimator) {
            this.filter = filter;
            this.planResult = planResult;
            this.plan = plan;
            this.indexNode = indexNode;
            this.rowInclusionPredicate = rowInclusionPredicate;
            this.estimator = estimator;
        }

        void updateEstimator(long value) {
            estimator.update(filter, value);
        }

        void addResultRow(FulltextIndex.FulltextResultRow row) {
            queue.add(row);
        }

        void setLastDoc(boolean lastDoc) {
            this.isLastDoc = lastDoc;
        }

        IndexPlan getPlan() {
            return plan;
        }


        ElasticIndexNode getIndexNode() {
            return indexNode;
        }


        RowInclusionPredicate getRowInclusionPredicate() {
            return rowInclusionPredicate;
        }

        Filter getFilter() {
            return filter;
        }

        PlanResult getPlanResult() {
            return planResult;
        }

        boolean isEmpty(){
            return queue.isEmpty();
        }

    }

}
