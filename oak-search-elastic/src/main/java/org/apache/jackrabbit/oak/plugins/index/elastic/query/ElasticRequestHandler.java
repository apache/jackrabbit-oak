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

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils.toDoubles;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newAncestorQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newDepthQuery2;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPrefixPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPrefixQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPropertyRestrictionQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newWildcardPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newWildcardQuery;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_PATH;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_SCORE;
import static org.apache.jackrabbit.util.ISO8601.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.jcr.PropertyType;

import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.facets.ElasticFacetProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.MoreLikeThisHelperUtil;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.BlobByteSource;
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
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SuggestMode;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.MoreLikeThisQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.NestedQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TextQueryType;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.InnerHits;
import co.elastic.clients.elasticsearch.core.search.PhraseSuggester;
import co.elastic.clients.json.JsonData;

/**
 * Class to map query plans into Elastic request objects.
 */
public class ElasticRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticRequestHandler.class);
    private final static String SPELLCHECK_PREFIX = "spellcheck?term=";
    protected final static String SUGGEST_PREFIX = "suggest?term=";
    private static final List<SortOptions> DEFAULT_SORTS = Arrays.asList(
            SortOptions.of(f -> f.field(fs -> fs.field("_score").order(SortOrder.Desc))),
            SortOptions.of(f -> f.field(fs -> fs.field(FieldNames.PATH).order(SortOrder.Asc)))// tie-breaker
    );

    private final IndexPlan indexPlan;
    private final Filter filter;
    private final PlanResult planResult;
    private final ElasticIndexDefinition elasticIndexDefinition;
    private final String propertyRestrictionQuery;
    private final NodeState rootState;

    ElasticRequestHandler(@NotNull IndexPlan indexPlan, @NotNull FulltextIndexPlanner.PlanResult planResult,
            NodeState rootState) {
        this.indexPlan = indexPlan;
        this.filter = indexPlan.getFilter();
        this.planResult = planResult;
        this.elasticIndexDefinition = (ElasticIndexDefinition) planResult.indexDefinition;

        // Check if native function is supported
        Filter.PropertyRestriction pr = null;
        if (elasticIndexDefinition.hasFunctionDefined()) {
            pr = filter.getPropertyRestriction(elasticIndexDefinition.getFunctionName());
        }

        this.propertyRestrictionQuery = pr != null ? String.valueOf(pr.first.getValue(pr.first.getType())) : null;
        this.rootState = rootState;
    }

    public BoolQuery baseQuery() {
        final BoolQuery.Builder bqBuilder = new BoolQuery.Builder();

        FullTextExpression ft = filter.getFullTextConstraint();

        if (ft != null) {
            bqBuilder.must(fullTextQuery(ft, planResult));
        }

        if (propertyRestrictionQuery != null) {
            if (propertyRestrictionQuery.startsWith("mlt?")) {
                List<PropertyDefinition> sp = elasticIndexDefinition.getSimilarityProperties();
                String mltQueryString = propertyRestrictionQuery.substring("mlt?".length());
                Map<String, String> mltParams = MoreLikeThisHelperUtil.getParamMapFromMltQuery(mltQueryString);
                String queryNodePath = mltParams.get(MoreLikeThisHelperUtil.MLT_STREAM_BODY);

                if (queryNodePath == null) {
                    // TODO : See if we might want to support like Text here (passed as null in
                    // above constructors)
                    // IT is not supported in our lucene implementation.
                    throw new IllegalArgumentException(
                            "Missing required field stream.body in MLT query: " + mltQueryString);
                }
                if (sp.isEmpty()) {
                    // SimilarityImpl in oak-core sets property restriction for sim search and the
                    // query is something like
                    // mlt?mlt.fl=:path&mlt.mindf=0&stream.body=<path> . We need parse this query
                    // string and turn into a query
                    // elastic can understand.
                    MoreLikeThisQuery mltq = mltQuery(mltParams);
                    bqBuilder.must(m->m
                            .moreLikeThis(mltq));
                    // add should clause to improve relevance using similarity tags
                    bqBuilder.should(s->s
                            .moreLikeThis(m->m
                                    .fields(ElasticIndexDefinition.SIMILARITY_TAGS)
                                    .like(mltq.like())
                                    .minTermFreq(1)
                                    .minDocFreq(1)));
                } else {
                    bqBuilder.must(m->m
                            .bool(similarityQuery(queryNodePath, sp)));
                    if (elasticIndexDefinition.areSimilarityTagsEnabled()) {
                        // add should clause to improve relevance using similarity tags
                        bqBuilder.should(s->s
                                .moreLikeThis(m->m
                                        .fields(ElasticIndexDefinition.SIMILARITY_TAGS)
                                        .like(l->l
                                                .document(d->d
                                                        .doc(JsonData.of(ElasticIndexUtils.idFromPath(queryNodePath)))))
                                        .minTermFreq(1)
                                        .minDocFreq(1)
                                        .boost(elasticIndexDefinition.getSimilarityTagsBoost())));
                        //TODO Angela check the like.docuemnt.doc JsonData...
                    }
                }
            } else {
                bqBuilder.must(m->m
                        .queryString(qs->qs
                                .query(propertyRestrictionQuery)));
            }

        } else if (planResult.evaluateNonFullTextConstraints()) {
            for (Query constraint : nonFullTextConstraints(indexPlan, planResult)) {
                bqBuilder.filter(constraint);
            }
        }

        // TODO Angela check this case
        if (bqBuilder.equals(new BoolQuery.Builder())) { // has NO query
            // TODO: what happens here in planning mode (specially, apparently for things
            // like rep:similar)
            // For purely nodeType based queries all the documents would have to
            // be returned (if the index definition has a single rule)
            if (planResult.evaluateNodeTypeRestriction()) {
                bqBuilder.must(Query.of(q->q.matchAll(m->m)));
            }
        }

        return bqBuilder.build();
    }

    public @NotNull List<SortOptions> baseSorts() {
        List<QueryIndex.OrderEntry> sortOrder = indexPlan.getSortOrder();
        if (sortOrder == null || sortOrder.isEmpty()) {
            return DEFAULT_SORTS;
        }
        Map<String, List<PropertyDefinition>> indexProperties = elasticIndexDefinition.getPropertiesByName();
        boolean hasTieBreaker = false;
        List<SortOptions> sortOptions = new ArrayList<>();
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
            sortOptions.add(SortOptions.of(f -> f.field(fs -> fs.field(fieldName)
                    .order(QueryIndex.OrderEntry.Order.ASCENDING.equals(o.getOrder()) ? SortOrder.Asc : SortOrder.Desc)))
            );
        }

        if (!hasTieBreaker) {
            sortOptions.add(SortOptions.of(f -> f.field(fs -> fs.field(FieldNames.PATH).order(SortOrder.Asc))));
        }

        return sortOptions;
    }
    
    /**
     * Receives a {@link SearchRequest} as input and converts it to a low
     * level {@link Request} reducing the response in order to reduce size and
     * improve speed.
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#common-options-response-filtering
     *
     * @param searchReq the search request
     * @return a low level {@link Request} instance
     */
    public Request createLowLevelRequest(SearchRequest searchReq) {
        String endpoint = "/" + String.join(",", searchReq.index())
                + "/_search?filter_path=took,timed_out,hits.total.value,hits.hits._score,hits.hits.sort,hits.hits._source,aggregations";
        Request request = new Request("POST", endpoint);
        String jsonString = ElasticIndexUtils.toString(searchReq);
        request.setJsonEntity(jsonString);
        return request;
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
        return requiresFacets()
                ? ElasticFacetProvider.getProvider(planResult.indexDefinition.getSecureFacetConfiguration(), this,
                        responseHandler, filter::isAccessible)
                : null;
    }

    private boolean requiresFacets() {
        return filter.getPropertyRestrictions().stream()
                .anyMatch(pr -> QueryConstants.REP_FACET.equals(pr.propertyName));
    }

    public Stream<Aggregation> aggregations() {
        return facetFields().map(facetProp -> Aggregation.of(af ->
                af.terms(tf -> tf.field(elasticIndexDefinition.getElasticKeyword(facetProp))
                        .size(elasticIndexDefinition.getNumberOfTopFacets()))
                ));

//                AggregationBuilders.terms(facetProp)
//                .field(elasticIndexDefinition.getElasticKeyword(facetProp))
//                .size(elasticIndexDefinition.getNumberOfTopFacets()));
    }

    public Stream<String> facetFields() {
        return filter.getPropertyRestrictions().stream().filter(pr -> QueryConstants.REP_FACET.equals(pr.propertyName))
                .map(pr -> FulltextIndex.parseFacetField(pr.first.getValue(Type.STRING)));
    }

    private BoolQuery similarityQuery(@NotNull String text, List<PropertyDefinition> sp) {
        BoolQuery.Builder query = new BoolQuery.Builder();
        if (!sp.isEmpty()) {
            LOG.debug("generating similarity query for {}", text);
            NodeState targetNodeState = rootState;
            for (String token : PathUtils.elements(text)) {
                targetNodeState = targetNodeState.getChildNode(token);
            }
            if (!targetNodeState.exists()) {
                throw new IllegalArgumentException("Could not find node " + text);
            }
            for (PropertyDefinition propertyDefinition : sp) {
                ElasticPropertyDefinition pd = (ElasticPropertyDefinition) propertyDefinition;
                String propertyPath = PathUtils.getParentPath(pd.name);
                String propertyName = PathUtils.getName(pd.name);
                NodeState tempState = targetNodeState;
                for (String token : PathUtils.elements(propertyPath)) {
                    if (token.isEmpty()) {
                        break;
                    }
                    tempState = tempState.getChildNode(token);
                }
                PropertyState ps = tempState.getProperty(propertyName);
                Blob property = ps != null ? ps.getValue(Type.BINARY) : null;
                if (property == null) {
                    LOG.warn("Couldn't find property {} on {}", pd.name, text);
                    continue;
                }
                byte[] bytes;
                try {
                    bytes = new BlobByteSource(property).read();
                } catch (IOException e) {
                    LOG.error("Error reading bytes from property " + pd.name + " on " + text, e);
                    continue;
                }
                String similarityPropFieldName = FieldNames.createSimilarityFieldName(pd.name);
                try {
                    XContentBuilder contentBuilder = JsonXContent.contentBuilder();
                    contentBuilder.startObject();
                    contentBuilder.field("elastiknn_nearest_neighbors");
                    contentBuilder.startObject();
                    {
                        contentBuilder.field("field", similarityPropFieldName);
                        contentBuilder.field("vec");
                        contentBuilder.startObject();
                        {
                            contentBuilder.field("values");
                            contentBuilder.startArray();
                            for (Double d : toDoubles(bytes)) {
                                contentBuilder.value(d);
                            }
                            contentBuilder.endArray();
                        }
                        contentBuilder.endObject();
                        contentBuilder.field("model", pd.getSimilaritySearchParameters().getQueryModel());
                        contentBuilder.field("similarity",
                                pd.getSimilaritySearchParameters().getQueryTimeSimilarityFunction());
                        contentBuilder.field("candidates", pd.getSimilaritySearchParameters().getCandidates());
                        contentBuilder.field("probes", pd.getSimilaritySearchParameters().getProbes());
                    }
                    contentBuilder.endObject();
                    contentBuilder.endObject();
                    
                    // TODO Angela check 
                    query.should(s->s
                            .wrapper(w->w
                                    .query(Strings.toString(contentBuilder))));
                } catch (IOException e) {
                    LOG.error("Could not create similarity query ", e);
                }
            }
        }
        return query.build();
    }

    /*
     * Generates mlt query builder from the given mltQueryString There could be 2
     * cases here - 1) select [jcr:path] from [nt:base] where similar(., '/test/a')
     * [Return nodes with similar content to /test/a] Xpath variant - //element(*,
     * nt:base)[rep:similar(., '/test/a')] In this case
     * org.apache.jackrabbit.oak.query.ast.SimilarImpl creates the mltQueryString as
     * mlt?mlt.fl=:path&mlt.mindf=0&stream.body=/test/a 2) select [jcr:path] from
     * [nt:base] where " + "native('elastic-sim',
     * 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0') In this case
     * the the exact mlt query passed above is passed to this method. This can be
     * useful if we want to fine tune the various default parameters. The function
     * name passed to native func ('elastic-sim') needs to be defined on index def
     * Refer https://jackrabbit.apache.org/oak/docs/query/lucene.html#native-query
     * TODO : Docs for writing a native mlt query with the various parameters that
     * can be tuned (The above is important since this is not a one-size-fits-all
     * situation and the default values might not be useful in every situation based
     * on the type of content)
     */
    private MoreLikeThisQuery mltQuery(Map<String, String> mltParams) {
        // creates a shallow copy of mltParams so we can remove the entries to
        // improve validation without changing the original structure
        Map<String, String> shallowMltParams = new HashMap<>(mltParams);
        String text1 = shallowMltParams.remove(MoreLikeThisHelperUtil.MLT_STREAM_BODY);

        MoreLikeThisQuery.Builder mlt = new MoreLikeThisQuery.Builder();
        String fields = shallowMltParams.remove(MoreLikeThisHelperUtil.MLT_FILED);
        // It's expected the text here to be the path of the doc
        // In case the path of a node is greater than 512 bytes,
        // we hash it before storing it as the _id for the elastic doc
        String text = ElasticIndexUtils.idFromPath(text1);
        if (fields == null || FieldNames.PATH.equals(fields)) {
            // Handle the case 1) where default query sent by SimilarImpl (No Custom fields)
            // We just need to specify the doc (Item) whose similar content we need to find
            // We store path as the _id so no need to do anything extra here
            // We expect Similar impl to send a query where text would have evaluated to
            // node path.
            mlt.like(l->l
                    .document(d->d
                            .doc(JsonData.of(text))));
        } else {
            // This is for native queries if someone send additional fields via
            // mlt.fl=field1,field2
            mlt.like(l->l
                    .document(d->d
                            .fields(Arrays.asList(fields.split(",")))
                            .doc(JsonData.of(text))));
        }
        // include the input doc to align the Lucene behaviour TODO: add configuration
        // parameter
        mlt.include(true);

        if (!shallowMltParams.isEmpty()) {
            BiConsumer<String, Consumer<String>> mltParamSetter = (key, setter) -> {
                String val = shallowMltParams.remove(key);
                if (val != null) {
                    setter.accept(val);
                }
            };

            mltParamSetter.accept(MoreLikeThisHelperUtil.MLT_MIN_DOC_FREQ,
                    (val) -> mlt.minDocFreq(Integer.parseInt(val)));
            mltParamSetter.accept(MoreLikeThisHelperUtil.MLT_MIN_TERM_FREQ,
                    (val) -> mlt.minTermFreq(Integer.parseInt(val)));
            mltParamSetter.accept(MoreLikeThisHelperUtil.MLT_BOOST_FACTOR, (val) -> mlt.boost(Float.parseFloat(val)));
            mltParamSetter.accept(MoreLikeThisHelperUtil.MLT_MAX_DOC_FREQ,
                    (val) -> mlt.maxDocFreq(Integer.parseInt(val)));
            mltParamSetter.accept(MoreLikeThisHelperUtil.MLT_MAX_QUERY_TERMS,
                    (val) -> mlt.maxQueryTerms(Integer.parseInt(val)));
            mltParamSetter.accept(MoreLikeThisHelperUtil.MLT_MAX_WORD_LENGTH,
                    (val) -> mlt.maxWordLength(Integer.parseInt(val)));
            mltParamSetter.accept(MoreLikeThisHelperUtil.MLT_MIN_WORD_LENGTH,
                    (val) -> mlt.minWordLength(Integer.parseInt(val)));
            mltParamSetter.accept(MoreLikeThisHelperUtil.MLT_MIN_SHOULD_MATCH, mlt::minimumShouldMatch);
            mltParamSetter.accept(MoreLikeThisHelperUtil.MLT_STOP_WORDS, (val) -> {
                // TODO : Read this from a stopwords text file, configured via index defn maybe
                // ?
                mlt.stopWords(Arrays.asList(val.split(",")));
            });

            if (!shallowMltParams.isEmpty()) {
                LOG.warn("mlt query contains unrecognized params {} that will be skipped", shallowMltParams);
            }
        }

        return mlt.build();
    }

    public PhraseSuggester suggestQuery(String spellCheckQuery) {
        BoolQuery.Builder query = new BoolQuery.Builder()
                .must(m->m
                        .matchPhrase(mp->mp
                                .field(FieldNames.SPELLCHECK)
                                .query("{{suggestion}}")));

        nonFullTextConstraints(indexPlan, planResult).forEach(query::must);

        return PhraseSuggester.of(ps->ps
                .field(FieldNames.SPELLCHECK)
                .size(10)
                .directGenerator(d->d
                        .field(FieldNames.SPELLCHECK)
                        .suggestMode(SuggestMode.Missing))
                .text(spellCheckQuery)
                .collate(c->c
                        .query(q->q
                                .source(query.toString()))));
    }

    public BoolQuery suggestMatchQuery2(String suggestion) {
        BoolQuery.Builder query = new BoolQuery.Builder()
                .must(m -> m
                        .match(mm -> mm
                                .field(FieldNames.SPELLCHECK)
                                .query(FieldValue.of(suggestion))
                                .operator(co.elastic.clients.elasticsearch._types.query_dsl.Operator.And)
                                .fuzzyTranspositions(false)
                                .autoGenerateSynonymsPhraseQuery(false)));
        nonFullTextConstraints(indexPlan, planResult).forEach(query::must);
        return query.build();
    }

    private Query fullTextQuery(FullTextExpression ft, final PlanResult pr) {
        // a reference to the query, so it can be set in the visitor
        // (a "non-local return")
        final AtomicReference<BoolQuery> result = new AtomicReference<>();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextContains contains) {
                // this 'hack' is needed because NotFullTextSearchImpl transforms the raw text
                // prepending a '-'. This causes
                // a double negation since the contains is already of type NOT. The same does
                // not happen in Lucene because
                // at this stage the code is parsed with the standard lucene parser.
                if (contains.getBase() instanceof FullTextTerm) {
                    visitTerm(contains.getPropertyName(), ((FullTextTerm) contains.getBase()).getText(), null,
                            contains.isNot());
                } else {
                    visitTerm(contains.getPropertyName(), contains.getRawText(), null, contains.isNot());
                }
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                BoolQuery.Builder q = new BoolQuery.Builder();
                for (FullTextExpression e : or.list) {
                    q.should(fullTextQuery(e, pr));
                }
                result.set(q.build());
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BoolQuery.Builder q = new co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery.Builder();
                for (FullTextExpression e : and.list) {
                    Query x = fullTextQuery(e, pr);
                    // TODO: see OAK-2434 and see if ES also can't work without unwrapping
                    /* Only unwrap the clause if MUST_NOT(x) */
                    boolean hasMustNot = false;
                    if (x.isBool()) {
                        BoolQuery bq = x.bool();
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
                result.set(q.build());
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                return visitTerm(term.getPropertyName(), term.getText(), term.getBoost(), term.isNot());
            }

            private boolean visitTerm(String propertyName, String text, String boost, boolean not) {
                // base query
                QueryStringQuery.Builder fullTextQuery = fullTextQuery(text, getElasticFieldName(propertyName), pr);
                if (boost != null) {
                    fullTextQuery.boost(Float.valueOf(boost));
                }
                BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder()
                        .must(m->m
                                .queryString(fullTextQuery.build()));
                Stream<NestedQuery> dynamicScoreQueries = dynamicScoreQueries(text);
                dynamicScoreQueries.forEach(dsq -> boolQueryBuilder.should(s->s.nested(dsq)));

                if (not) {
                    result.set(BoolQuery.of(b->b
                            .mustNot(mn->mn
                                    .bool(boolQueryBuilder.build()))));
                } else {
                    result.set(boolQueryBuilder.build());
                }
                return true;
            }
        });
        return Query.of(q->q
                .bool(result.get()));
    }

    private Stream<NestedQuery> dynamicScoreQueries(String text) {
        return elasticIndexDefinition.getDynamicBoostProperties().stream()
                .map(pd -> NestedQuery.of(n->n
                        .path(pd.nodeName)
                        .query(q->q
                                .functionScore(s->s
                                        .query(fq->fq
                                                .match(m->m
                                                        .field(pd.nodeName + ".value")
                                                        .query(FieldValue.of(text))))
                                        .functions(f->f
                                                .fieldValueFactor(fv->fv
                                                        .field(pd.nodeName + ".boost")))))
                        .scoreMode(ChildScoreMode.Avg)));
    }

    private List<Query> nonFullTextConstraints(IndexPlan plan, PlanResult planResult) {
        final BiPredicate<Iterable<String>, String> any = (iterable, value) -> StreamSupport
                .stream(iterable.spliterator(), false).anyMatch(value::equals);

        final List<Query> queries = new ArrayList<>();

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
            queries.add(
                    Query.of(q -> q.bool(b -> b.must(newAncestorQuery(path)).must(newDepthQuery2(path, planResult)))));
            break;
        case EXACT:
            // For transformed paths, we can only add path restriction if absolute path to
            // property can be
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
            if (PathUtils.denotesRoot(path)) {
                // there's no parent of the root node
                // we add a path that can not possibly occur because there
                // is no way to say "match no documents" in Lucene
                queries.add(newPathQuery("///"));
            } else {
                // For transformed paths, we can only add path restriction if absolute path to
                // property can be
                // deduced
                if (planResult.isPathTransformed()) {
                    String parentPathSegment = planResult.getParentPathSegment();
                    if (!any.test(PathUtils.elements(parentPathSegment), "*")) {
                        queries.add(newPathQuery(PathUtils.getParentPath(path) + parentPathSegment));
                    }
                } else {
                    queries.add(newPathQuery(PathUtils.getParentPath(path)));
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
                    queries.add(nodeName(pr));
                }
                continue;
            }

            if (IndexConstants.INDEX_TAG_OPTION.equals(name) || IndexConstants.INDEX_NAME_OPTION.equals(name)) {
                continue;
            }

            if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
                String first = pr.first.getValue(Type.STRING);
                first = first.replace("\\", "");
                if (JCR_PATH.equals(name)) {
                    queries.add(newPathQuery(first));
                    continue;
                } else if ("*".equals(name)) {
                    // TODO Revisit reference constraint. For performant impl
                    // references need to be indexed in a different manner
                    queries.add(referenceConstraint(first));
                    continue;
                }
            }

            PropertyDefinition pd = planResult.getPropDefn(pr);
            if (pd == null) {
                continue;
            }

            queries.add(createQuery(planResult.getPropertyName(pr), pr, pd));
        }
        return queries;
    }

    public Query suggestionMatchQuery(String suggestion) {
        BoolQuery.Builder bb = new BoolQuery.Builder()
                .must(m -> m
                        .nested(n -> n
                                .path(FieldNames.SUGGEST)
                                .query(qq -> qq
                                        .matchBoolPrefix(mb -> mb
                                                .field(FieldNames.SUGGEST + ".value")
                                                .query(suggestion)
                                                .operator(co.elastic.clients.elasticsearch._types.query_dsl.Operator.And)))
                                .scoreMode(ChildScoreMode.Max)
                                .innerHits(InnerHits.of(h -> h
                                        .size(100)))));
//        nonFullTextConstraints(indexPlan, planResult).forEach(q->bb.must(q));
        return Query.of(q->q
                .bool(bb.build()));
    }

    private static Query nodeTypeConstraints(IndexDefinition.IndexingRule defn, Filter filter) {
        final BoolQuery.Builder bq = new BoolQuery.Builder();
        PropertyDefinition primaryType = defn.getConfig(JCR_PRIMARYTYPE);
        // TODO OAK-2198 Add proper nodeType query support

        if (primaryType != null && primaryType.propertyIndex) {
            for (String type : filter.getPrimaryTypes()) {
                bq.should(q -> q.term(t -> t.field(JCR_PRIMARYTYPE).value(FieldValue.of(type))));
            }
        }

        PropertyDefinition mixinType = defn.getConfig(JCR_MIXINTYPES);
        if (mixinType != null && mixinType.propertyIndex) {
            for (String type : filter.getMixinTypes()) {
                bq.should(q -> q.term(t -> t.field(JCR_MIXINTYPES).value(FieldValue.of(type))));
            }
        }
        return Query.of(q -> q.bool(bq.build()));
    }

    private static Query nodeName(Filter.PropertyRestriction pr) {
        String first = pr.first != null ? pr.first.getValue(Type.STRING) : null;
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            // [property]=[value]
            return Query.of(q -> q.term(t -> t.field(FieldNames.NODE_NAME).value(FieldValue.of(first))));
        }

        if (pr.isLike) {
            return like(FieldNames.NODE_NAME, first);
        }

        throw new IllegalStateException("For nodeName queries only EQUALS and LIKE are supported " + pr);
    }

    private static Query like(String name, String first) {
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

    private static Query referenceConstraint(String uuid) {
        // TODO: this seems very bad as a query - do we really want to support it. In
        // fact, is it even used?
        // reference query
        return Query.of(q -> q.multiMatch(m -> m.fields(uuid)));
    }

    private static QueryStringQuery.Builder fullTextQuery(String text, String fieldName, PlanResult pr) {
        LOG.debug("fullTextQuery for text: '{}', fieldName: '{}'", text, fieldName);
        QueryStringQuery.Builder queryStringQueryBuilder = new QueryStringQuery.Builder()
                .query(FulltextIndex.rewriteQueryText(text))
                .defaultOperator(co.elastic.clients.elasticsearch._types.query_dsl.Operator.And)
                .type(TextQueryType.CrossFields);
        if (FieldNames.FULLTEXT.equals(fieldName)) {
            for(PropertyDefinition pd: pr.indexingRule.getNodeScopeAnalyzedProps()) {
                queryStringQueryBuilder.fields(pd.name);
                queryStringQueryBuilder.boost(pd.boost);
            }
        }
        return queryStringQueryBuilder.fields(fieldName);
    }

    private Query createQuery(String propertyName, Filter.PropertyRestriction pr, PropertyDefinition defn) {
        int propType = FulltextIndex.determinePropertyType(defn, pr);

        if (pr.isNullRestriction()) {
            return Query.of(q->q
                    .bool(b->b
                            .mustNot(m->m
                                    .exists(e->e
                                            .field(propertyName)))));
        }
        if (pr.isNotNullRestriction()) {
            return Query.of(q->q
                    .exists(e->e
                            .field(propertyName)));
        }

        final String field = elasticIndexDefinition.getElasticKeyword(propertyName);

        Query in;
        switch (propType) {
        case PropertyType.DATE: {
            in = newPropertyRestrictionQuery(field, pr, value -> parse(value.getValue(Type.DATE)).getTimeInMillis());
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
