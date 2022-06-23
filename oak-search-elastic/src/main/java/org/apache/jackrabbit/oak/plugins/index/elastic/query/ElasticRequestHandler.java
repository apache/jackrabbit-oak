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
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newDepthQuery;
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
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.jcr.PropertyType;

import co.elastic.clients.elasticsearch.core.search.Highlight;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.SuggestMode;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.MoreLikeThisQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.NestedQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TextQueryType;
import co.elastic.clients.elasticsearch.core.search.InnerHits;
import co.elastic.clients.elasticsearch.core.search.PhraseSuggester;

/**
 * Class to map query plans into Elastic request objects.
 */
public class ElasticRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticRequestHandler.class);
    private final static String SPELLCHECK_PREFIX = "spellcheck?term=";
    protected final static String SUGGEST_PREFIX = "suggest?term=";
    private static final List<SortOptions> DEFAULT_SORTS = Arrays.asList(
            SortOptions.of(so -> so.field(f -> f.field("_score").order(SortOrder.Desc))),
            SortOptions.of(so -> so.field(f -> f.field(FieldNames.PATH).order(SortOrder.Asc)))
    );

    private static final String HIGHLIGHT_PREFIX = "<strong>";
    private static final String HIGHLIGHT_SUFFIX = "</strong>";

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

    public Query baseQuery() {
        return Query.of(fn -> {
                    fn.bool(fnb -> {

                        FullTextExpression ft = filter.getFullTextConstraint();

                        if (ft != null) {
                            fnb.must(fullTextQuery(ft, planResult));
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
                                    fnb.must(m -> m.moreLikeThis(mltQuery(mltParams)));
                                } else {
                                    fnb.must(m -> m.bool(similarityQuery(queryNodePath, sp)));
                                }

                                if (elasticIndexDefinition.areSimilarityTagsEnabled()) {
                                    // add should clause to improve relevance using similarity tags
                                    fnb.should(s -> s
                                            .moreLikeThis(m -> m
                                                    .fields(ElasticIndexDefinition.SIMILARITY_TAGS)
                                                    .like(l -> l.document(d -> d.id(ElasticIndexUtils.idFromPath(queryNodePath))))
                                                    .minTermFreq(1)
                                                    .minDocFreq(1)
                                                    .boost(elasticIndexDefinition.getSimilarityTagsBoost())
                                            )
                                    );
                                }

                            } else {
                                fnb.must(m -> m.queryString(qs -> qs.query(propertyRestrictionQuery)));
                            }

                        } else if (planResult.evaluateNonFullTextConstraints()) {
                            for (Query constraint : nonFullTextConstraints(indexPlan, planResult)) {
                                fnb.filter(constraint);
                            }
                        }
                        return fnb;
                    });
                    return fn;
                }
        );
    }
    
    public @NotNull List<SortOptions> baseSorts() {
        List<QueryIndex.OrderEntry> sortOrder = indexPlan.getSortOrder();
        if (sortOrder == null || sortOrder.isEmpty()) {
            return DEFAULT_SORTS;
        }
        Map<String, List<PropertyDefinition>> indexProperties = elasticIndexDefinition.getPropertiesByName();
        boolean hasTieBreaker = false;
        List<SortOptions> list = new ArrayList<>();
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
            SortOptions order = SortOptions.of(so -> so
                    .field(f -> f
                            .field(fieldName)
                            .order(QueryIndex.OrderEntry.Order.ASCENDING.equals(o.getOrder()) ? SortOrder.Asc : SortOrder.Desc)));
            list.add(order);
        }

        if (!hasTieBreaker) {
            list.add(SortOptions.of(so -> so.field(f -> f.field(FieldNames.PATH).order(SortOrder.Asc))));
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
        return requiresFacets()
                ? ElasticFacetProvider.getProvider(planResult.indexDefinition.getSecureFacetConfiguration(), this,
                        responseHandler, filter::isAccessible)
                : null;
    }

    private boolean requiresFacets() {
        return filter.getPropertyRestrictions().stream()
                .anyMatch(pr -> QueryConstants.REP_FACET.equals(pr.propertyName));
    }
    
    public Map<String, Aggregation> aggregations() {
        return facetFields().collect(Collectors.toMap(Function.identity(), facetProp -> Aggregation.of(af ->
                af.terms(tf -> tf.field(elasticIndexDefinition.getElasticKeyword(facetProp))
                        .size(elasticIndexDefinition.getNumberOfTopFacets()))
        )));
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

                String knnQuery = "{" +
                        "  \"elastiknn_nearest_neighbors\": {" +
                        "    \"field\": \"" + FieldNames.createSimilarityFieldName(pd.name) + "\"," +
                        "    \"model\": \"" + pd.getSimilaritySearchParameters().getQueryModel() + "\"," +
                        "    \"similarity\": \"" + pd.getSimilaritySearchParameters().getQueryTimeSimilarityFunction() + "\"," +
                        "    \"candidates\": " + pd.getSimilaritySearchParameters().getCandidates() + "," +
                        "    \"probes\": " + pd.getSimilaritySearchParameters().getProbes() + "," +
                        "    \"vec\": {" +
                        "      \"values\": [" +
                        toDoubles(bytes).stream().map(Objects::toString).collect(Collectors.joining(",")) +
                        "      ]" +
                        "    }" +
                        "  }" +
                        "}";

                query.should(s -> s
                        .wrapper(w -> w.query(Base64.getEncoder().encodeToString(knnQuery.getBytes(StandardCharsets.UTF_8))))
                );
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
        String text = shallowMltParams.remove(MoreLikeThisHelperUtil.MLT_STREAM_BODY);

        MoreLikeThisQuery.Builder mlt = new MoreLikeThisQuery.Builder();
        String fields = shallowMltParams.remove(MoreLikeThisHelperUtil.MLT_FILED);
        // It's expected the text here to be the path of the doc
        // In case the path of a node is greater than 512 bytes,
        // we hash it before storing it as the _id for the elastic doc
        String id = ElasticIndexUtils.idFromPath(text);
        if (fields == null || FieldNames.PATH.equals(fields)) {
            // Handle the case 1) where default query sent by SimilarImpl (No Custom fields)
            // We just need to specify the doc (Item) whose similar content we need to find
            // We store path as the _id so no need to do anything extra here
            // We expect Similar impl to send a query where text would have evaluated to
            // node path.
            mlt.like(l -> l.document(d -> d.id(id)));
        } else {
            // This is for native queries if someone sends additional fields via
            // mlt.fl=field1,field2
            mlt.like(l -> l.document(d -> d.fields(Arrays.asList(fields.split(","))).id(id)));
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

    public PhraseSuggester suggestQuery() {
        BoolQuery.Builder bqBuilder = new BoolQuery.Builder().must(m -> m.matchPhrase(mp -> mp
                .field(FieldNames.SPELLCHECK)
                .query("{{suggestion}}"))
        );

        nonFullTextConstraints(indexPlan, planResult).forEach(bqBuilder::must);
        Query query = Query.of(q -> q.bool(bqBuilder.build()));
        return PhraseSuggester.of(ps -> ps
                .field(FieldNames.SPELLCHECK)
                .size(10)
                .directGenerator(d -> d.field(FieldNames.SPELLCHECK).suggestMode(SuggestMode.Missing))
                .collate(c -> c.query(q -> q.source(ElasticIndexUtils.toString(query))))
        );
    }

    public BoolQuery suggestMatchQuery(String suggestion) {
        BoolQuery.Builder query = new BoolQuery.Builder().must(m -> m
                .match(mm -> mm
                        .field(FieldNames.SPELLCHECK)
                        .query(FieldValue.of(suggestion))
                        .operator(Operator.And)
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
                BoolQuery.Builder bqBuilder = new BoolQuery.Builder();
                for (FullTextExpression e : or.list) {
                    bqBuilder.should(fullTextQuery(e, pr));
                }
                result.set(bqBuilder.build());
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BoolQuery.Builder bqBuilder = new BoolQuery.Builder();
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
                            bqBuilder.mustNot(bq.mustNot().get(0));
                        }
                    }

                    if (!hasMustNot) {
                        bqBuilder.must(x);
                    }
                }
                result.set(bqBuilder.build());
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                return visitTerm(term.getPropertyName(), term.getText(), term.getBoost(), term.isNot());
            }

            private boolean visitTerm(String propertyName, String text, String boost, boolean not) {
                // base query
                QueryStringQuery.Builder qsqBuilder = fullTextQuery(text, getElasticFieldName(propertyName), pr);
                if (boost != null) {
                    qsqBuilder.boost(Float.valueOf(boost));
                }
                BoolQuery.Builder bqBuilder = new BoolQuery.Builder()
                        .must(m->m
                                .queryString(qsqBuilder.build()));
                Stream<NestedQuery> dynamicScoreQueries = dynamicScoreQueries(text);
                dynamicScoreQueries.forEach(dsq -> bqBuilder.should(s->s.nested(dsq)));

                if (not) {
                    result.set(BoolQuery.of(b->b
                            .mustNot(mn->mn
                                    .bool(bqBuilder.build()))));
                } else {
                    result.set(bqBuilder.build());
                }
                return true;
            }
        });
        return Query.of(q->q
                .bool(result.get()));
    }

    private Stream<NestedQuery> dynamicScoreQueries(String text) {
        return elasticIndexDefinition.getDynamicBoostProperties().stream().map(pd -> NestedQuery.of(n -> n
                .path(pd.nodeName)
                .query(q -> q.functionScore(s -> s
                        .query(fq -> fq.match(m -> m.field(pd.nodeName + ".value").query(FieldValue.of(text))))
                        .functions(f -> f.fieldValueFactor(fv -> fv.field(pd.nodeName + ".boost")))))
                .scoreMode(ChildScoreMode.Avg))
        );
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
            queries.add(Query.of(q -> q.bool(b -> b.must(newAncestorQuery(path)).must(newDepthQuery(path, planResult)))));
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
        BoolQuery.Builder bqBuilder = new BoolQuery.Builder()
                .must(m -> m.nested(n -> n.path(FieldNames.SUGGEST).query(qq -> qq
                                .matchBoolPrefix(mb ->
                                        mb.field(FieldNames.SUGGEST + ".value").query(suggestion).operator(Operator.And))
                        )
                        .scoreMode(ChildScoreMode.Max)
                        .innerHits(InnerHits.of(h -> h.size(100)))));
        nonFullTextConstraints(indexPlan, planResult).forEach(bqBuilder::must);
        return Query.of(q -> q.bool(bqBuilder.build()));
    }

    /**
     * Generates a Highlight that is the search request part necessary to obtain excerpts.
     * rep:excerpt() and rep:excerpt(.) makes use of :fulltext
     * rep:excerpt(FIELD) makes use of FIELD
     *
     * @return a Highlight object representing the excerpts to request or null if none should be requested
     */
    public Highlight highlight() {
        Map<String, HighlightField> excerpts = indexPlan.getFilter().getPropertyRestrictions().stream()
                .filter(pr -> pr.propertyName.startsWith(QueryConstants.REP_EXCERPT))
                .map(this::excerptField)
                .distinct()
                .collect(Collectors.toMap(
                        Function.identity(),
                        field -> HighlightField.of(hf -> hf.withJson(new StringReader("{}"))))
                );

        if (excerpts.isEmpty()) {
            return null;
        }

        return Highlight.of(h -> h
                .preTags(HIGHLIGHT_PREFIX)
                .postTags(HIGHLIGHT_SUFFIX)
                .fields(excerpts)
                .numberOfFragments(1)
                .requireFieldMatch(false));
    }

    private @NotNull String excerptField(Filter.PropertyRestriction pr) {
        String name = pr.first.toString();
        int length = QueryConstants.REP_EXCERPT.length();
        if (name.length() > length) {
            String field = name.substring(length + 1, name.length() - 1);
            if (field.length() > 0 && !field.equals(".")) {
                return field;
            }
        }
        return ":fulltext";
    }

    private static Query nodeTypeConstraints(IndexDefinition.IndexingRule defn, Filter filter) {
        final BoolQuery.Builder bqBuilder = new BoolQuery.Builder();
        PropertyDefinition primaryType = defn.getConfig(JCR_PRIMARYTYPE);
        // TODO OAK-2198 Add proper nodeType query support

        if (primaryType != null && primaryType.propertyIndex) {
            for (String type : filter.getPrimaryTypes()) {
                bqBuilder.should(q -> q.term(t -> t.field(JCR_PRIMARYTYPE).value(FieldValue.of(type))));
            }
        }

        PropertyDefinition mixinType = defn.getConfig(JCR_MIXINTYPES);
        if (mixinType != null && mixinType.propertyIndex) {
            for (String type : filter.getMixinTypes()) {
                bqBuilder.should(q -> q.term(t -> t.field(JCR_MIXINTYPES).value(FieldValue.of(type))));
            }
        }
        return Query.of(q -> q.bool(bqBuilder.build()));
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
        QueryStringQuery.Builder qsqBuilder = new QueryStringQuery.Builder()
                .query(FulltextIndex.rewriteQueryText(text))
                .defaultOperator(co.elastic.clients.elasticsearch._types.query_dsl.Operator.And)
                .type(TextQueryType.CrossFields);
        if (FieldNames.FULLTEXT.equals(fieldName)) {
            for(PropertyDefinition pd: pr.indexingRule.getNodeScopeAnalyzedProps()) {
                qsqBuilder.fields(pd.name);
                qsqBuilder.boost(pd.boost);
            }
        }
        return qsqBuilder.fields(fieldName);
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
