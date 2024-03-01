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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.query;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.search.util.LMSEstimator;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.elasticsearch.action.search.SearchResponse;
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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newAncestorQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newDepthQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newMixinTypeQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newNodeTypeQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newNotNullPropQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newNullPropQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newPrefixPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newPrefixQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newPropertyRestrictionQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newWildcardPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.TermQueryBuilderFactory.newWildcardQuery;
import static org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.isNodePath;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_PATH;
import static org.apache.jackrabbit.util.ISO8601.parse;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class ElasticsearchResultRowIterator implements Iterator<FulltextIndex.FulltextResultRow> {
    private static final Logger LOG = LoggerFactory
            .getLogger(ElasticsearchResultRowIterator.class);
    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(ElasticsearchResultRowIterator.class.getName() + ".perf"));

    // TODO: oak-lucene gets this via WildcardQuery class. See if ES also exposes these consts
    private static final char WILDCARD_STRING = '*';
    private static final char WILDCARD_CHAR = '?';

    /**
     * Batch size for fetching results from queries.
     */
    private static final int ELASTICSEARCH_QUERY_BATCH_SIZE = 1000;

    private static final int ELASTICSEARCH_QUERY_MAX_BATCH_SIZE = 10000;

    private final Deque<FulltextIndex.FulltextResultRow> queue = new ArrayDeque<>();
    // TODO : find if ES can return dup docs - if so how to avoid
//    private final Set<String> seenPaths = Sets.newHashSet();
    private SearchHit lastDoc;
    private int nextBatchSize = ELASTICSEARCH_QUERY_BATCH_SIZE;
    private boolean noDocs = false;

    private final Filter filter;
    private final PlanResult pr;
    private final IndexPlan plan;
    private final ElasticsearchIndexNode indexNode;
    private final RowInclusionPredicate rowInclusionPredicate;
    private final LMSEstimator estimator;

    ElasticsearchResultRowIterator(@NotNull Filter filter,
                                   @NotNull PlanResult pr,
                                   @NotNull IndexPlan plan,
                                   ElasticsearchIndexNode indexNode,
                                   RowInclusionPredicate rowInclusionPredicate,
                                   LMSEstimator estimator) {
        this.filter = filter;
        this.pr = pr;
        this.plan = plan;
        this.indexNode = indexNode;
        this.rowInclusionPredicate = rowInclusionPredicate != null ? rowInclusionPredicate : RowInclusionPredicate.NOOP;
        this.estimator = estimator;
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty() || loadDocs();
    }

    @Override
    public FulltextIndex.FulltextResultRow next() {
        return queue.remove();
    }

    /**
     * Loads the lucene documents in batches
     *
     * @return true if any document is loaded
     */
    private boolean loadDocs() {

        if (noDocs) {
            return false;
        }

        if (indexNode == null) {
            throw new IllegalStateException("indexNode cannot be null");
        }

        SearchHit lastDocToRecord = null;
        try {
            ElasticsearchSearcher searcher = getCurrentSearcher(indexNode);
            QueryBuilder query = getESRequest(plan, pr);
            // TODO: custom scoring

            SearchResponse docs;
            long start = PERF_LOGGER.start();
            while (true) {
                LOG.debug("loading {} entries for query {}", nextBatchSize, query);
                docs = searcher.search(query, nextBatchSize);

                SearchHit[] searchHits = docs.getHits().getHits();
                PERF_LOGGER.end(start, -1, "{} ...", searchHits.length);

                estimator.update(filter, docs.getHits().getTotalHits().value);

                if (searchHits.length < nextBatchSize) {
                    noDocs = true;
                }

                nextBatchSize = (int) Math.min(nextBatchSize * 2L, ELASTICSEARCH_QUERY_MAX_BATCH_SIZE);

                // TODO: faceting

                // TODO: excerpt

                // TODO: explanation

                // TODO: sim search

                for (SearchHit doc : searchHits) {
                    // TODO : excerpts

                    FulltextIndex.FulltextResultRow row = convertToRow(doc);
                    if (row != null) {
                        queue.add(row);
                    }
                    lastDocToRecord = doc;
                }

                if (queue.isEmpty() && searchHits.length > 0) {
                    //queue is still empty but more results can be fetched
                    //from Lucene so still continue
                    lastDoc = lastDocToRecord;
                } else {
                    break;
                }
            }

            // TODO: spellcheck else if (luceneRequestFacade.getLuceneRequest() instanceof SpellcheckHelper.SpellcheckQuery) {
            // TODO: suggest } else if (luceneRequestFacade.getLuceneRequest() instanceof SuggestHelper.SuggestQuery) {
        } catch (Exception e) {
            LOG.warn("query via {} failed.", this, e);
        } finally {
            indexNode.release();
        }

        if (lastDocToRecord != null) {
            this.lastDoc = lastDocToRecord;
        }

        return !queue.isEmpty();
    }

    private ElasticsearchSearcher getCurrentSearcher(ElasticsearchIndexNode indexNode) {
        return new ElasticsearchSearcher(indexNode);
    }

    private FulltextIndex.FulltextResultRow convertToRow(SearchHit hit) throws IOException {
        String id = hit.getId();
        String path = idToPath(id);
        if (path != null) {
            if ("".equals(path)) {
                path = "/";
            }
            if (pr.isPathTransformed()) {
                String originalPath = path;
                path = pr.transformPath(path);

                if (path == null) {
                    LOG.trace("Ignoring path {} : Transformation returned null", originalPath);
                    return null;
                }
            }

            boolean shouldIncludeForHierarchy = rowInclusionPredicate.shouldInclude(path, plan);
            LOG.trace("Matched path {}; shouldIncludeForHierarchy: {}", path, shouldIncludeForHierarchy);
            return shouldIncludeForHierarchy ? new FulltextIndex.FulltextResultRow(path, hit.getScore(), null,
                    null, null)
                    : null;
        }
        return null;
    }

    public interface RowInclusionPredicate {
        boolean shouldInclude(@NotNull String path, @NotNull IndexPlan plan);

        RowInclusionPredicate NOOP = (@NotNull String path, @NotNull IndexPlan plan) -> true;
    }

    /**
     * Get the Elasticsearch query for the given filter.
     *
     * @param plan       index plan containing filter details
     * @param planResult
     * @return the Lucene query
     */
    static QueryBuilder getESRequest(IndexPlan plan, PlanResult planResult) {
        List<QueryBuilder> qs = new ArrayList<>();
        Filter filter = plan.getFilter();
        FullTextExpression ft = filter.getFullTextConstraint();
        ElasticsearchIndexDefinition defn = (ElasticsearchIndexDefinition) planResult.indexDefinition;

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
                return matchAllQuery();
            }

            throw new IllegalStateException("No query created for filter " + filter);
        }
        return performAdditionalWraps(qs);
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

        if (isNodePath(p)) {
            if (pr.isPathTransformed()) {
                p = PathUtils.getName(p);
            } else {
                //Get rid of /* as aggregated fulltext field name is the
                //node relative path
                p = FieldNames.createFulltextFieldName(PathUtils.getParentPath(p));
            }
        } else {
            if (pr.isPathTransformed()) {
                p = PathUtils.getName(p);
            }
            p = FieldNames.createAnalyzedFieldName(p);
        }

        if ("*".equals(p)) {
            p = FieldNames.FULLTEXT;
        }
        return p;
    }

    /**
     * Perform additional wraps on the list of queries to allow, for example, the NOT CONTAINS to
     * play properly when sent to lucene.
     *
     * @param qs the list of queries. Cannot be null.
     * @return the request facade
     */
    @NotNull
    private static QueryBuilder performAdditionalWraps(@NotNull List<QueryBuilder> qs) {
        if (qs.size() == 1) {
            // we don't need to worry about all-negatives in a bool query as
            // BoolQueryBuilder.adjustPureNegative is on by default anyway
            return qs.get(0);
        }
        BoolQueryBuilder bq = new BoolQueryBuilder();
        // TODO: while I've attempted to translate oak-lucene code to corresponding ES one but I am
        // unable to make sense of this code
        for (QueryBuilder q : qs) {
            boolean unwrapped = false;
            if (q instanceof BoolQueryBuilder) {
                unwrapped = unwrapMustNot((BoolQueryBuilder) q, bq);
            }

            if (!unwrapped) {
                bq.must(q);
            }
        }
        return bq;
    }

    /**
     * unwraps any NOT clauses from the provided boolean query into another boolean query.
     *
     * @param input  the query to be analysed for the existence of NOT clauses. Cannot be null.
     * @param output the query where the unwrapped NOTs will be saved into. Cannot be null.
     * @return true if there where at least one unwrapped NOT. false otherwise.
     */
    private static boolean unwrapMustNot(@NotNull BoolQueryBuilder input, @NotNull BoolQueryBuilder output) {
        boolean unwrapped = false;
        for (QueryBuilder mustNot : input.mustNot()) {
            output.mustNot(mustNot);
            unwrapped = true;
        }
        if (unwrapped) {
            // if we have unwrapped "must not" conditions,
            // then we need to unwrap "must" conditions as well
            for (QueryBuilder must : input.must()) {
                output.must(must);
            }
        }

        return unwrapped;
    }

    private static void addNonFullTextConstraints(List<QueryBuilder> qs,
                                                  IndexPlan plan, PlanResult planResult) {
        final BiPredicate<Iterable<String>, String> any = (iterable, value) ->
                StreamSupport.stream(iterable.spliterator(), false).anyMatch(value::equals);

        Filter filter = plan.getFilter();
        IndexDefinition defn = planResult.indexDefinition;
        if (!filter.matchesAllTypes()) {
            addNodeTypeConstraints(planResult.indexingRule, qs, filter);
        }

        String path = FulltextIndex.getPathRestriction(plan);
        switch (filter.getPathRestriction()) {
            case ALL_CHILDREN:
                if (defn.evaluatePathRestrictions()) {
                    if ("/".equals(path)) {
                        break;
                    }
                    qs.add(newAncestorQuery(path));
                }
                break;
            case DIRECT_CHILDREN:
                if (defn.evaluatePathRestrictions()) {
                    BoolQueryBuilder bq = boolQuery();
                    bq.must(newAncestorQuery(path));
                    bq.must(newDepthQuery(path, planResult));
                    qs.add(bq);
                }
                break;
            case EXACT:
                // For transformed paths, we can only add path restriction if absolute path to property can be
                // deduced
                if (planResult.isPathTransformed()) {
                    String parentPathSegment = planResult.getParentPathSegment();
                    if (!any.test(PathUtils.elements(parentPathSegment), "*")) {
                        qs.add(newPathQuery(path + parentPathSegment));
                    }
                } else {
                    qs.add(newPathQuery(path));
                }
                break;
            case PARENT:
                if (denotesRoot(path)) {
                    // there's no parent of the root node
                    // we add a path that can not possibly occur because there
                    // is no way to say "match no documents" in Lucene
                    qs.add(newPathQuery("///"));
                } else {
                    // For transformed paths, we can only add path restriction if absolute path to property can be
                    // deduced
                    if (planResult.isPathTransformed()) {
                        String parentPathSegment = planResult.getParentPathSegment();
                        if (!any.test(PathUtils.elements(parentPathSegment), "*")) {
                            qs.add(newPathQuery(getParentPath(path) + parentPathSegment));
                        }
                    } else {
                        qs.add(newPathQuery(getParentPath(path)));
                    }
                }
                break;
            case NO_RESTRICTION:
                break;
        }

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
                String first = pr.first.getValue(STRING);
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
        String first = pr.first != null ? pr.first.getValue(STRING) : null;
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
    private static QueryBuilder createQuery(String propertyName, Filter.PropertyRestriction pr,
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

        QueryBuilder in;
        switch (propType) {
            case PropertyType.DATE: {
                in = newPropertyRestrictionQuery(propertyName, false, pr,
                        value -> parse(value.getValue(Type.DATE)).getTime());
                break;
            }
            case PropertyType.DOUBLE: {
                in = newPropertyRestrictionQuery(propertyName, false, pr,
                        value -> value.getValue(Type.DOUBLE));
                break;
            }
            case PropertyType.LONG: {
                in = newPropertyRestrictionQuery(propertyName, false, pr,
                        value -> value.getValue(Type.LONG));
                break;
            }
            default: {
                if (pr.isLike) {
                    return createLikeQuery(propertyName, pr.first.getValue(STRING));
                }

                //TODO Confirm that all other types can be treated as string
                in = newPropertyRestrictionQuery(propertyName, true, pr,
                        value -> value.getValue(Type.STRING));
            }
        }

        if (in != null) {
            return in;
        }

        throw new IllegalStateException("PropertyRestriction not handled " + pr + " for index " + defn);
    }

    private static String idToPath(String id) throws UnsupportedEncodingException {
        return URLDecoder.decode(id, "UTF-8");
    }
}
