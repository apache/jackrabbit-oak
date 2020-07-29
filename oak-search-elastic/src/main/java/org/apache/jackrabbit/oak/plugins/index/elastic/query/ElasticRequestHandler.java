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
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.facets.ElasticFacetProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.MoreLikeThisHelperUtil;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
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
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchBoolPrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newAncestorQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newDepthQuery;
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
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_SCORE;
import static org.apache.jackrabbit.util.ISO8601.parse;
import static org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Class to map query plans into Elastic request objects.
 */
public class ElasticRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticRequestHandler.class);
    private final static String SPELLCHECK_PREFIX = "spellcheck?term=";
    protected final static String SUGGEST_PREFIX = "suggest?term=";
    private static final String ES_TRIGRAM_SUFFIX = ".trigram";
    private static final List<FieldSortBuilder> DEFAULT_SORTS = Arrays.asList(
            SortBuilders.fieldSort("_score").order(SortOrder.DESC),
            SortBuilders.fieldSort(FieldNames.PATH).order(SortOrder.ASC) // tie-breaker
    );

    private final IndexPlan indexPlan;
    private final Filter filter;
    private final PlanResult planResult;
    private final ElasticIndexDefinition elasticIndexDefinition;
    private final String propertyRestrictionQuery;

    ElasticRequestHandler(@NotNull IndexPlan indexPlan, @NotNull FulltextIndexPlanner.PlanResult planResult) {
        this.indexPlan = indexPlan;
        this.filter = indexPlan.getFilter();
        this.planResult = planResult;
        this.elasticIndexDefinition = (ElasticIndexDefinition) planResult.indexDefinition;

        //Check if native function is supported
        Filter.PropertyRestriction pr = null;
        if (elasticIndexDefinition.hasFunctionDefined()) {
            pr = filter.getPropertyRestriction(elasticIndexDefinition.getFunctionName());
        }

        this.propertyRestrictionQuery = pr != null ? String.valueOf(pr.first.getValue(pr.first.getType())) : null;
    }

    public BoolQueryBuilder baseQuery() {
        final BoolQueryBuilder boolQuery = boolQuery();

        FullTextExpression ft = filter.getFullTextConstraint();

        if (ft != null) {
            boolQuery.must(fullTextQuery(ft, planResult));
        }

        if (propertyRestrictionQuery != null) {
            if (propertyRestrictionQuery.startsWith("mlt?")) {
                // SimilarityImpl in oak-core sets property restriction for sim search and the query is something like
                // mlt?mlt.fl=:path&mlt.mindf=0&stream.body=<path> . We need parse this query string and turn into a query
                // elastic can understand.
                String mltQueryString = propertyRestrictionQuery.replace("mlt?", "");
                boolQuery.must(moreLikeThisQuery(mltQueryString));

            } else {
                boolQuery.must(queryStringQuery(propertyRestrictionQuery));
            }

        } else if (planResult.evaluateNonFullTextConstraints()) {
            for (QueryBuilder constraint : nonFullTextConstraints(indexPlan, planResult)) {
                boolQuery.filter(constraint);
            }
        }

        if (!boolQuery.hasClauses()) {
            // TODO: what happens here in planning mode (specially, apparently for things like rep:similar)
            //For purely nodeType based queries all the documents would have to
            //be returned (if the index definition has a single rule)
            if (planResult.evaluateNodeTypeRestriction()) {
                boolQuery.must(matchAllQuery());
            }
        }

        return boolQuery;
    }

    public @NotNull List<FieldSortBuilder> baseSorts() {
        List<QueryIndex.OrderEntry> sortOrder = indexPlan.getSortOrder();
        if (sortOrder == null || sortOrder.isEmpty()) {
            return DEFAULT_SORTS;
        }
        Map<String, List<PropertyDefinition>> indexProperties = elasticIndexDefinition.getPropertiesByName();
        boolean hasTieBreaker = false;
        List<FieldSortBuilder> list = new ArrayList<>();
        for (QueryIndex.OrderEntry o : sortOrder) {
            hasTieBreaker = false;
            String sortPropertyName = o.getPropertyName();
            String fieldName;
            if (JCR_PATH.equals(sortPropertyName)) {
                fieldName = FieldNames.PATH;
                hasTieBreaker = true;
            } else if (JCR_SCORE.equals(sortPropertyName)) {
                fieldName = "_score";
            } else if (indexProperties.containsKey(sortPropertyName)) {
                fieldName = elasticIndexDefinition.getElasticKeyword(sortPropertyName);
            } else {
                LOG.warn("Unable to sort by {} for index {}", sortPropertyName, elasticIndexDefinition.getIndexName());
                continue;
            }
            FieldSortBuilder order = SortBuilders.fieldSort(fieldName)
                    .order(QueryIndex.OrderEntry.Order.ASCENDING.equals(o.getOrder()) ? SortOrder.ASC : SortOrder.DESC);
            list.add(order);
        }

        if (!hasTieBreaker) {
            list.add(SortBuilders.fieldSort(FieldNames.PATH).order(SortOrder.ASC));
        }

        return list;
    }

    public String getPropertyRestrictionQuery() {
        return propertyRestrictionQuery;
    }

    public boolean requiresSpellCheck() {
        return propertyRestrictionQuery != null && propertyRestrictionQuery.startsWith(SPELLCHECK_PREFIX);
    }

    public boolean requiresSuggestion() {
        return propertyRestrictionQuery != null && propertyRestrictionQuery.startsWith(SUGGEST_PREFIX);
    }

    public ElasticFacetProvider getAsyncFacetProvider(ElasticResponseHandler responseHandler) {
        return requiresFacets() ?
                ElasticFacetProvider.getProvider(
                        planResult.indexDefinition.getSecureFacetConfiguration(),
                        this, responseHandler,
                        filter::isAccessible
                ) : null;
    }

    private boolean requiresFacets() {
        return filter.getPropertyRestrictions()
                .stream()
                .anyMatch(pr -> QueryConstants.REP_FACET.equals(pr.propertyName));
    }

    public Stream<TermsAggregationBuilder> aggregations() {
        return facetFields()
                .map(facetProp ->
                        AggregationBuilders.terms(facetProp)
                                .field(elasticIndexDefinition.getElasticKeyword(facetProp))
                                .size(elasticIndexDefinition.getNumberOfTopFacets())
                );
    }

    public Stream<String> facetFields() {
        return filter.getPropertyRestrictions()
                .stream()
                .filter(pr -> QueryConstants.REP_FACET.equals(pr.propertyName))
                .map(pr -> FulltextIndex.parseFacetField(pr.first.getValue(Type.STRING)));
    }

    public Stream<String> spellCheckFields() {
        return StreamSupport
                .stream(planResult.indexingRule.getProperties().spliterator(), false)
                .filter(pd -> pd.useInSpellcheck)
                .map(pd -> pd.name);
    }

    /*
    Generates mlt query builder from the given mltQueryString
    There could be 2 cases here -
    1) select [jcr:path] from [nt:base] where similar(., '/test/a') [Return nodes with similar content to /test/a]
    Xpath variant - //element(*, nt:base)[rep:similar(., '/test/a')]
    In this case org.apache.jackrabbit.oak.query.ast.SimilarImpl creates the mltQueryString as
    mlt?mlt.fl=:path&mlt.mindf=0&stream.body=/test/a
    2) select [jcr:path] from [nt:base] where " +
       "native('elastic-sim', 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0')
       In this case the the exact mlt query passed above is passed to this method. This can be useful if we want to
       fine tune the various default parameters.
       The function name passed to native func ('elastic-sim') needs to be defined on index def
       Refer https://jackrabbit.apache.org/oak/docs/query/lucene.html#native-query
       TODO : Docs for writing a native mlt query with the various parameters that can be tuned
       (The above is important since this is not a one-size-fits-all situation and the default values might not
       be useful in every situation based on the type of content)
     */
    private QueryBuilder moreLikeThisQuery(String mltQueryString) {
        MoreLikeThisQueryBuilder mlt;
        Map<String, String> paramMap = MoreLikeThisHelperUtil.getParamMapFromMltQuery(mltQueryString);
        String text = paramMap.get(MoreLikeThisHelperUtil.MLT_STREAM_BODY);
        String fields = paramMap.get(MoreLikeThisHelperUtil.MLT_FILED);

        if (text != null) {
            // It's expected the text here to be the path of the doc
            // In case the path of a node is greater than 512 bytes,
            // we hash it before storing it as the _id for the elastic doc
            text = ElasticIndexUtils.idFromPath(text);
            if (FieldNames.PATH.equals(fields) || fields == null) {
                // Handle the case 1) where default query sent by SimilarImpl (No Custom fields)
                // We just need to specify the doc (Item) whose similar content we need to find
                // We store path as the _id so no need to do anything extra here
                // We expect Similar impl to send a query where text would have evaluated to node path.
                mlt = new MoreLikeThisQueryBuilder(null, new Item[]{new Item(null, text)});
            } else {
                // This is for native queries if someone send additional fields via mlt.fl=field1,field2
                String[] fieldsArray = fields.split(",");
                mlt = new MoreLikeThisQueryBuilder(fieldsArray, null, new Item[]{new Item(null, text)});
            }
            // TODO : See if we might want to support like Text here (passed as null in above constructors)
            // IT is not supported in our lucene implementation.
        } else {
            throw new RuntimeException("Missing required field stream.body in  MLT query: " + mltQueryString);
        }

        for (String key : paramMap.keySet()) {
            String val = paramMap.get(key);
            if (MoreLikeThisHelperUtil.MLT_MIN_DOC_FREQ.equals(key)) {
                mlt.minDocFreq(Integer.parseInt(val));
            } else if (MoreLikeThisHelperUtil.MLT_MIN_TERM_FREQ.equals(key)) {
                mlt.minTermFreq(Integer.parseInt(val));
            } else if (MoreLikeThisHelperUtil.MLT_BOOST_FACTOR.equals(key)) {
                mlt.boost(Float.parseFloat(val));
            } else if (MoreLikeThisHelperUtil.MLT_MAX_DOC_FREQ.equals(key)) {
                mlt.maxDocFreq(Integer.parseInt(val));
            } else if (MoreLikeThisHelperUtil.MLT_MAX_QUERY_TERMS.equals(key)) {
                mlt.maxQueryTerms(Integer.parseInt(val));
            } else if (MoreLikeThisHelperUtil.MLT_MAX_WORD_LENGTH.equals(key)) {
                mlt.maxWordLength(Integer.parseInt(val));
            } else if (MoreLikeThisHelperUtil.MLT_MIN_WORD_LENGTH.equals(key)) {
                mlt.minWordLength(Integer.parseInt(val));
            } else if (MoreLikeThisHelperUtil.MLT_MIN_SHOULD_MATCH.equals(key)) {
                mlt.minimumShouldMatch(val);
            } else if (MoreLikeThisHelperUtil.MLT_STOP_WORDS.equals(key)) {
                // TODO : Read this from a stopwords text file, configured via index defn maybe ?
                String[] stopWords = val.split(",");
                mlt.stopWords(stopWords);
            } else {
                LOG.warn("Unrecognized param {} in the mlt query {}", key, mltQueryString);
            }
        }

        return mlt;
    }

    public PhraseSuggestionBuilder suggestQuery(String field, String spellCheckQuery) {
        BoolQueryBuilder query = boolQuery()
                .must(new MatchPhraseQueryBuilder(field, "{{suggestion}}"));

        nonFullTextConstraints(indexPlan, planResult).forEach(query::must);

        PhraseSuggestionBuilder.CandidateGenerator candidateGeneratorBuilder =
                new DirectCandidateGeneratorBuilder(getTrigramField(field)).suggestMode("missing");
        return SuggestBuilders
                .phraseSuggestion(getTrigramField(field))
                .size(10)
                .addCandidateGenerator(candidateGeneratorBuilder)
                .text(spellCheckQuery)
                .collateQuery(query.toString());
    }

    public BoolQueryBuilder suggestMatchQuery(String suggestion, String[] fields) {
        BoolQueryBuilder query = boolQuery()
                .must(new MultiMatchQueryBuilder(suggestion, fields)
                        .operator(Operator.AND).fuzzyTranspositions(false)
                        .autoGenerateSynonymsPhraseQuery(false)
                        .type(MatchQuery.Type.PHRASE));

        nonFullTextConstraints(indexPlan, planResult).forEach(query::must);

        return query;
    }

    private String getTrigramField(String field) {
        return field + ES_TRIGRAM_SUFFIX;
    }

    private QueryBuilder fullTextQuery(FullTextExpression ft, final PlanResult pr) {
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
                    q.should(fullTextQuery(e, pr));
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BoolQueryBuilder q = boolQuery();
                for (FullTextExpression e : and.list) {
                    QueryBuilder x = fullTextQuery(e, pr);
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
                QueryBuilder q = fullTextQuery(text, getElasticFieldName(propertyName), pr);
                if (boost != null) {
                    q.boost(Float.parseFloat(boost));
                }
                if (not) {
                    BoolQueryBuilder bq = boolQuery().mustNot(q);
                    result.set(bq);
                } else {
                    result.set(q);
                }
                return true;
            }
        });
        return result.get();
    }

    private List<QueryBuilder> nonFullTextConstraints(IndexPlan plan, PlanResult planResult) {
        final BiPredicate<Iterable<String>, String> any = (iterable, value) ->
                StreamSupport.stream(iterable.spliterator(), false).anyMatch(value::equals);

        final List<QueryBuilder> queries = new ArrayList<>();

        Filter filter = plan.getFilter();
        if (!filter.matchesAllTypes()) {
            queries.add(nodeTypeConstraints(planResult.indexingRule, filter));
        }

        String path = FulltextIndex.getPathRestriction(plan);
        switch (filter.getPathRestriction()) {
            case ALL_CHILDREN:
                if (!"/".equals(path)) {
                    queries.add(newAncestorQuery(path));
                }
                break;
            case DIRECT_CHILDREN:
                BoolQueryBuilder bq = boolQuery()
                        .must(newAncestorQuery(path))
                        .must(newDepthQuery(path, planResult));
                queries.add(bq);
                break;
            case EXACT:
                // For transformed paths, we can only add path restriction if absolute path to property can be
                // deduced
                if (planResult.isPathTransformed()) {
                    String parentPathSegment = planResult.getParentPathSegment();
                    if (!any.test(PathUtils.elements(parentPathSegment), "*")) {
                        queries.add(newPathQuery(path + parentPathSegment));
                    }
                } else {
                    queries.add(newPathQuery(path));
                }
                break;
            case PARENT:
                if (denotesRoot(path)) {
                    // there's no parent of the root node
                    // we add a path that can not possibly occur because there
                    // is no way to say "match no documents" in Lucene
                    queries.add(newPathQuery("///"));
                } else {
                    // For transformed paths, we can only add path restriction if absolute path to property can be
                    // deduced
                    if (planResult.isPathTransformed()) {
                        String parentPathSegment = planResult.getParentPathSegment();
                        if (!any.test(PathUtils.elements(parentPathSegment), "*")) {
                            queries.add(newPathQuery(getParentPath(path) + parentPathSegment));
                        }
                    } else {
                        queries.add(newPathQuery(getParentPath(path)));
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
                    QueryBuilder q = nodeName(pr);
                    if (q != null) {
                        queries.add(q);
                    }
                }
                continue;
            }

            if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
                String first = pr.first.getValue(Type.STRING);
                first = first.replace("\\", "");
                if (JCR_PATH.equals(name)) {
                    queries.add(newPathQuery(first));
                    continue;
                } else if ("*".equals(name)) {
                    //TODO Revisit reference constraint. For performant impl
                    //references need to be indexed in a different manner
                    queries.add(referenceConstraint(first));
                    continue;
                }
            }

            PropertyDefinition pd = planResult.getPropDefn(pr);
            if (pd == null) {
                continue;
            }

            QueryBuilder q = createQuery(planResult.getPropertyName(pr), pr, pd);
            if (q != null) {
                queries.add(q);
            }
        }
        return queries;
    }

    public BoolQueryBuilder suggestionMatchQuery(String suggestion) {
        QueryBuilder qb = new MatchBoolPrefixQueryBuilder(FieldNames.SUGGEST + ".suggestion", suggestion).operator(Operator.AND);
        NestedQueryBuilder nestedQueryBuilder = nestedQuery(FieldNames.SUGGEST, qb, ScoreMode.Max);
        InnerHitBuilder in = new InnerHitBuilder().setSize(100);
        nestedQueryBuilder.innerHit(in);
        BoolQueryBuilder query = boolQuery()
                .must(nestedQueryBuilder);
        nonFullTextConstraints(indexPlan, planResult).forEach(query::must);
        return query;
    }

    private static QueryBuilder nodeTypeConstraints(IndexDefinition.IndexingRule defn, Filter filter) {
        final BoolQueryBuilder bq = boolQuery();
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

        return bq;
    }

    private static QueryBuilder nodeName(Filter.PropertyRestriction pr) {
        String first = pr.first != null ? pr.first.getValue(Type.STRING) : null;
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                && pr.lastIncluding) {
            // [property]=[value]
            return termQuery(FieldNames.NODE_NAME, first);
        }

        if (pr.isLike) {
            return like(FieldNames.NODE_NAME, first);
        }

        throw new IllegalStateException("For nodeName queries only EQUALS and LIKE are supported " + pr);
    }

    private static QueryBuilder like(String name, String first) {
        first = first.replace('%', WildcardQuery.WILDCARD_STRING);
        first = first.replace('_', WildcardQuery.WILDCARD_CHAR);

        int indexOfWS = first.indexOf(WildcardQuery.WILDCARD_STRING);
        int indexOfWC = first.indexOf(WildcardQuery.WILDCARD_CHAR);
        int len = first.length();

        if (indexOfWS == len || indexOfWC == len) {
            // remove trailing "*" for prefix query
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

    private static QueryBuilder referenceConstraint(String uuid) {
        // TODO: this seems very bad as a query - do we really want to support it. In fact, is it even used?
        // reference query
        return QueryBuilders.multiMatchQuery(uuid);
    }

    private static QueryBuilder fullTextQuery(String text, String fieldName, PlanResult pr) {
        // default match query are executed in OR, we need to use AND instead to avoid that
        // every document having at least one term in the `text` will match. If there are multiple
        // contains clause they will go to different match queries and will be executed in OR
        if (FieldNames.FULLTEXT.equals(fieldName) && !pr.indexingRule.getNodeScopeAnalyzedProps().isEmpty()) {
            MultiMatchQueryBuilder multiMatchQuery = multiMatchQuery(text)
                    .operator(Operator.AND)
                    .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);
            pr.indexingRule.getNodeScopeAnalyzedProps().forEach(pd -> multiMatchQuery.field(pd.name, pd.boost));
            // Add the query for actual fulltext field also. That query would not be boosted
            // and could contain other parts like renditions, node name, etc
            return multiMatchQuery.field(fieldName);
        } else {
            return matchQuery(fieldName, text).operator(Operator.AND);
        }
    }

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

        final String field = elasticIndexDefinition.getElasticKeyword(propertyName);

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
                    return like(propertyName, pr.first.getValue(Type.STRING));
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

    private String getElasticFieldName(@Nullable String p) {
        if (p == null) {
            return FieldNames.FULLTEXT;
        }

        if (planResult.isPathTransformed()) {
            p = PathUtils.getName(p);
        }

        if ("*".equals(p)) {
            p = FieldNames.FULLTEXT;
        }
        return p;
    }
}
