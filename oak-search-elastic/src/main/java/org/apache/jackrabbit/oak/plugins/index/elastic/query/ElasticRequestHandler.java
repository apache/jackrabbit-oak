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
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPropertyDefinition.DEFAULT_SIMILARITY_METRIC;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils.toFloats;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newAncestorQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newDepthQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPrefixPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPrefixQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPropertyRestrictionQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newWildcardPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newWildcardQuery;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.DYNAMIC_BOOST_WEIGHT;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_PATH;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_SCORE;
import static org.apache.jackrabbit.util.ISO8601.parse;

import co.elastic.clients.util.ObjectBuilder;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.facets.ElasticFacetProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceService;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceServiceManager;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.MoreLikeThisHelperUtil;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.BlobByteSource;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.search.util.QueryUtils;
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
import co.elastic.clients.elasticsearch._types.KnnQuery;
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
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TextQueryType;
import co.elastic.clients.elasticsearch.core.search.Highlight;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.elasticsearch.core.search.InnerHits;
import co.elastic.clients.elasticsearch.core.search.PhraseSuggester;
import co.elastic.clients.json.JsonpUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.jcr.PropertyType;

/**
 * Class to map query plans into Elastic request objects.
 */
public class ElasticRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticRequestHandler.class);
    private final static String SPELLCHECK_PREFIX = "spellcheck?term=";
    protected final static String SUGGEST_PREFIX = "suggest?term=";
    private static final List<SortOptions> DEFAULT_SORTS = List.of(
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
        return Query.of(q -> q.bool(baseQueryBuilder().build()));
    }

    public BoolQuery.Builder baseQueryBuilder() {
        BoolQuery.Builder bqb = new BoolQuery.Builder();
        FullTextExpression ft = filter.getFullTextConstraint();

        if (ft != null) {
            bqb.must(fullTextQuery(ft, planResult));
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
                    String fields = mltParams.remove(MoreLikeThisHelperUtil.MLT_FILED);
                    if (fields == null || FieldNames.PATH.equals(fields)) {
                        for (String stf : elasticIndexDefinition.getSimilarityTagsFields()) {
                            Map<String, String> shallowMltParams = new HashMap<>(MoreLikeThisHelperUtil.getParamMapFromMltQuery(stf));
                            shallowMltParams.putAll(mltParams);
                            bqb.should(m -> m.moreLikeThis(mltQuery(shallowMltParams)));
                        }
                    } else {
                        bqb.must(m -> m.moreLikeThis(mltQuery(mltParams)));
                    }
                } else {
                  similarityQuery(queryNodePath, sp).ifPresent(similarityQuery ->
                    bqb.filter(fb -> fb.exists(ef -> ef.field(similarityQuery.field())))
                        .should(s -> s.knn(similarityQuery))
                  );
                }

                // Add should clause to improve relevance using similarity tags only when similarity is
                // enabled and there is at least one similarity tag property
                if (elasticIndexDefinition.areSimilarityTagsEnabled() &&
                        !elasticIndexDefinition.getSimilarityTagsProperties().isEmpty()) {
                    // add should clause to improve relevance using similarity tags
                    bqb.should(s -> s
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
                bqb.must(m -> m.queryString(qs -> qs.query(propertyRestrictionQuery)));
            }

        } else if (planResult.evaluateNonFullTextConstraints()) {
            for (Query constraint : nonFullTextConstraints(indexPlan, planResult)) {
                bqb.filter(constraint);
            }
        }

        return bqb;
    }

  public Optional<KnnQuery> similarityQuery(@NotNull String text, List<PropertyDefinition> sp) {
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
          LOG.error("Error reading bytes from property {} on {}", pd.name, text, e);
          continue;
        }

        String similarityPropFieldName = FieldNames.createSimilarityFieldName(pd.name);
        KnnQuery knnQuery = baseKnnQueryBuilder(similarityPropFieldName, bytes, pd).build();
        return Optional.of(knnQuery);
      }
    }
    return Optional.empty();
  }

    @NotNull
    private KnnQuery.Builder baseKnnQueryBuilder(String similarityPropFieldName, byte[] bytes, ElasticPropertyDefinition pd) {
        KnnQuery.Builder knnQueryBuilder = new KnnQuery.Builder()
            .field(similarityPropFieldName)
            .queryVector(toFloats(bytes))
            .numCandidates(pd.getKnnSearchParameters().getCandidates());
        if (!pd.getKnnSearchParameters().getSimilarityMetric().equals(DEFAULT_SIMILARITY_METRIC)) {
            knnQueryBuilder.similarity(pd.getKnnSearchParameters().getSimilarity());
        }
        return knnQueryBuilder;
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
            } else if (indexProperties.containsKey(sortPropertyName) || elasticIndexDefinition.getDefinedRules()
                    .stream().anyMatch(rule -> rule.getConfig(sortPropertyName) != null)) {
                // There are 2 conditions in this if statement -
                // First one returns true if sortPropertyName is one of the defined indexed properties on the index
                // Second condition returns true if sortPropertyName might not be explicitly defined but covered by a regex property
                // in any of the defined index rules.
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

    public ElasticFacetProvider getAsyncFacetProvider(ElasticConnection connection, ElasticResponseHandler responseHandler) {
        return requiresFacets()
                ? ElasticFacetProvider.getProvider(planResult.indexDefinition.getSecureFacetConfiguration(), connection,
                elasticIndexDefinition, this, responseHandler, filter::isAccessible)
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
            mlt.like(l -> l.document(d -> d.fields(List.of(fields.split(","))).id(id)));
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
                mlt.stopWords(List.of(val.split(",")));
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
        StringBuilder queryString = JsonpUtils.toString(query, new StringBuilder());
        return PhraseSuggester.of(ps -> ps
                .field(FieldNames.SPELLCHECK)
                .size(10)
                .directGenerator(d -> d.field(FieldNames.SPELLCHECK).suggestMode(SuggestMode.Missing).size(10))
                .collate(c -> c.query(q -> q.source(queryString.toString())))
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
                // We could have possibly used ((FullTextTerm)contains.getBase()).getText() to fix this,
                // but that causes other divergence from behaviour of lucene (such as it removes any escape characters added by client in the jcr query)
                // So we simply remove the prepending '-' here.
                String rawText = contains.getRawText();
                if (contains.getBase() instanceof FullTextTerm && contains.isNot() && rawText.startsWith("-")) {
                    // Replace the prepending '-' in raw text to avoid double negation
                    String text = rawText.replaceFirst("-", "");
                    visitTerm(contains.getPropertyName(), text, null,
                            true);
                } else {
                    visitTerm(contains.getPropertyName(), rawText, null, contains.isNot());
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
                BoolQuery.Builder bqBuilder = new BoolQuery.Builder();
                if (propertyName != null && FulltextIndex.isNodePath(propertyName) && !pr.isPathTransformed()) {
                    //Get rid of /* as aggregated fulltext field name is the
                    //node relative path
                    String p = PathUtils.getParentPath(propertyName);
                    bqBuilder.must(m -> m.nested(nf ->
                            nf.path(ElasticIndexDefinition.DYNAMIC_PROPERTIES)
                                    .query(Query.of(q -> q.term(t -> t.field(ElasticIndexDefinition.DYNAMIC_PROPERTIES + ".name").value(p))))
                    ));
                    QueryStringQuery.Builder qsqBuilder = fullTextQuery(text, ElasticIndexDefinition.DYNAMIC_PROPERTIES + ".value", pr, false);
                    bqBuilder.must(m -> m.nested(nf -> nf.path(ElasticIndexDefinition.DYNAMIC_PROPERTIES).query(Query.of(q -> q.queryString(qsqBuilder.build())))));
                } else {
                    boolean dbEnabled = !elasticIndexDefinition.getDynamicBoostProperties().isEmpty();

                    // Experimental support for inference queries
                    if (elasticIndexDefinition.inferenceDefinition != null && elasticIndexDefinition.inferenceDefinition.queries != null) {
                        bqBuilder.must(m -> m.bool(b -> inference(b, propertyName, text, pr, dbEnabled)));
                    } else {
                        QueryStringQuery.Builder qsqBuilder = fullTextQuery(text, getElasticFieldName(propertyName), pr, dbEnabled);
                        bqBuilder.must(m -> m.queryString(qsqBuilder.build()));
                    }
                }

                if (boost != null) {
                    bqBuilder.boost(Float.valueOf(boost));
                }

                Stream<NestedQuery> dynamicScoreQueries = dynamicScoreQueries(text);
                dynamicScoreQueries.forEach(dsq -> bqBuilder.should(s -> s.nested(dsq)));

                if (not) {
                    result.set(BoolQuery.of(b -> b.mustNot(mn -> mn.bool(bqBuilder.build()))));
                } else {
                    result.set(bqBuilder.build());
                }
                return true;
            }
        });

        return Query.of(q -> q.bool(result.get()));
    }

    private ObjectBuilder<BoolQuery> inference(BoolQuery.Builder b, String propertyName, String text, PlanResult pr, boolean dbEnabled) {
        ElasticIndexDefinition.InferenceDefinition.Query q = null;
        // select first query eligible for the given text
        // TODO: evaluate if/how to handle multiple queries
        String  queryText = text;
        for (ElasticIndexDefinition.InferenceDefinition.Query query : elasticIndexDefinition.inferenceDefinition.queries) {
            if (query.isEligibleForInput(queryText)) {
                queryText = query.rewrite(queryText);
                if (query.hasMinTerms(queryText)) {
                    q = query;
                    break;
                }
            }
        }

        QueryStringQuery.Builder qsqBuilder = fullTextQuery(queryText, getElasticFieldName(propertyName), pr, dbEnabled);

        // the query can be null if no inference query is eligible for the given text or the min terms are not met
        // in this case, we fall back to the default full-text query
        if (q != null) {
            LOG.info("Using inference query: {}", q);
            try {
                // let's retrieve the fields with the same model as the query
                final ElasticIndexDefinition.InferenceDefinition.Query query = q;
                List<ElasticIndexDefinition.InferenceDefinition.Property> properties = elasticIndexDefinition.inferenceDefinition.properties.stream()
                        .filter(pd -> pd.model.equals(query.model))
                        .collect(Collectors.toList());
                if (!properties.isEmpty()) {
                    InferenceService inferenceService = InferenceServiceManager.getInstance(q.serviceUrl, q.model);
                    List<Float> embeddings = inferenceService.embeddings(queryText, (int) q.timeout);
                    if (embeddings != null) {
                        for (ElasticIndexDefinition.InferenceDefinition.Property p : properties) {
                            // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-knn-query.html
                            KnnQuery.Builder knnQueryBuilder = new KnnQuery.Builder();
                            knnQueryBuilder.field(p.name + ".value");
                            knnQueryBuilder.numCandidates(q.numCandidates);
                            knnQueryBuilder.queryVector(embeddings);
                            knnQueryBuilder.similarity(q.similarityThreshold);
                            b.should(s -> s.knn(knnQueryBuilder.build()));
                        }
                        int tokens = queryText.split("\\s+").length;
                        // the more tokens, the less important the full-text query is
                        // TODO: make it configurable
                        double qsBoost = (tokens > 1) ? 1.0d / (5 * tokens) : 1.0d;
                        return b.should(s -> s.queryString(qsqBuilder.boost((float) qsBoost).build()));
                    } else {
                        LOG.warn("No embeddings found for text {}", text);
                    }
                } else {
                    LOG.warn("No properties with model {} found", query.model);
                }
            } catch (Exception e) {
                LOG.warn("Error while calling inference service. Query won't use embeddings", e);
            }
        }
        return b.must(mm -> mm.queryString(qsqBuilder.build()));
    }

    private Stream<NestedQuery> dynamicScoreQueries(String text) {
        return elasticIndexDefinition.getDynamicBoostProperties().stream().map(pd -> NestedQuery.of(n -> n
                .path(pd.nodeName)
                .query(q -> q.functionScore(s -> s
                        .boost(DYNAMIC_BOOST_WEIGHT)
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
            Optional<Query> nodeTypeConstraints = nodeTypeConstraints(planResult.indexingRule, filter, elasticIndexDefinition);
            nodeTypeConstraints.ifPresent(queries::add);
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
                // For transformed paths, we can only add path restriction if absolute path to property can be deduced
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
                    // For transformed paths, we can only add path restriction if absolute path to property can be deduced
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
            if (!field.isEmpty() && !field.equals(".")) {
                return field;
            }
        }
        return ":fulltext";
    }

    private static Optional<Query> nodeTypeConstraints(IndexDefinition.IndexingRule defn, Filter filter, ElasticIndexDefinition definition) {
        List<Query> queries = new ArrayList<>();
        PropertyDefinition primaryType = defn.getConfig(JCR_PRIMARYTYPE);
        // TODO OAK-2198 Add proper nodeType query support

        if (primaryType != null && primaryType.propertyIndex) {
            for (String type : filter.getPrimaryTypes()) {
                queries.add(TermQuery.of(t -> t.field(definition.getElasticKeyword(JCR_PRIMARYTYPE)).value(FieldValue.of(type)))._toQuery());
            }
        }

        PropertyDefinition mixinType = defn.getConfig(JCR_MIXINTYPES);
        if (mixinType != null && mixinType.propertyIndex) {
            for (String type : filter.getMixinTypes()) {
                queries.add(TermQuery.of(t -> t.field(definition.getElasticKeyword(JCR_MIXINTYPES)).value(FieldValue.of(type)))._toQuery());
            }
        }

        return queries.isEmpty() ? Optional.empty() : Optional.of(Query.of(qb -> qb.bool(b -> b.should(queries))));
    }

    private Query nodeName(Filter.PropertyRestriction pr) {
        String first = pr.first != null ? pr.first.getValue(Type.STRING) : null;
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            // [property]=[value]
            return Query.of(q -> q.term(t -> t.field(elasticIndexDefinition.getElasticKeyword(FieldNames.NODE_NAME)).value(FieldValue.of(first))));
        }

        if (pr.isLike) {
            return like(FieldNames.NODE_NAME, first);
        }

        throw new IllegalStateException("For nodeName queries only EQUALS and LIKE are supported " + pr);
    }

    private Query like(String name, String first) {
        first = QueryUtils.sqlLikeToLuceneWildcardQuery(first);

        // If the query ends in a wildcard string (*) and has no other wildcard characters, use a prefix match query
        boolean optimizeToPrefixQuery = first.indexOf(WildcardQuery.WILDCARD_STRING) == first.length() - 1 &&
                first.indexOf(WildcardQuery.WILDCARD_ESCAPE) == -1 &&
                first.indexOf(WildcardQuery.WILDCARD_CHAR) == -1;

        // Non full text (Non analyzed) properties are keyword types in ES. For those field would be equal to name.
        // Analyzed properties, however are of text type on which we can't perform wildcard or prefix queries so we use the keyword (sub) field
        // by appending .keyword to the name here.
        String field = elasticIndexDefinition.getElasticKeyword(name);

        if (optimizeToPrefixQuery) {
            // remove trailing "*" for prefix query
            first = first.substring(0, first.length() - 1);
            if (JCR_PATH.equals(name)) {
                return newPrefixPathQuery(first);
            } else {
                return newPrefixQuery(field, first);
            }
        } else {
            if (JCR_PATH.equals(name)) {
                return newWildcardPathQuery(first);
            } else {
                return newWildcardQuery(field, first);
            }
        }
    }

    private static Query referenceConstraint(String uuid) {
        // TODO: this seems very bad as a query - do we really want to support it. In
        // fact, is it even used?
        // reference query
        return Query.of(q -> q.multiMatch(m -> m.fields(uuid)));
    }

    private static QueryStringQuery.Builder fullTextQuery(String text, String fieldName, PlanResult pr, boolean dynamicBoostEnabled) {
        LOG.debug("fullTextQuery for text: '{}', fieldName: '{}'", text, fieldName);
        QueryStringQuery.Builder qsqBuilder = new QueryStringQuery.Builder()
                .query(FulltextIndex.rewriteQueryText(text))
                .defaultOperator(Operator.And)
                .type(TextQueryType.CrossFields)
                .tieBreaker(0.5d);
        if (FieldNames.FULLTEXT.equals(fieldName)) {
            for(PropertyDefinition pd: pr.indexingRule.getNodeScopeAnalyzedProps()) {
                qsqBuilder.fields(pd.name + "^" + pd.boost);
            }
            // dynamic boost is included only for :fulltext field
            if (dynamicBoostEnabled) {
                qsqBuilder.fields(ElasticIndexDefinition.DYNAMIC_BOOST_FULLTEXT + "^" + DYNAMIC_BOOST_WEIGHT);
            }
        }
        return qsqBuilder.fields(fieldName);
    }

    private Query createQuery(String propertyName, Filter.PropertyRestriction pr, PropertyDefinition defn) {
        int propType = FulltextIndex.determinePropertyType(defn, pr);

        if (pr.isNullRestriction()) {
            return Query.of(q -> q.bool(b -> b.mustNot(m -> m.exists(e -> e.field(propertyName)))));
        }
        if (pr.isNotNullRestriction()) {
            return Query.of(q -> q.exists(e -> e.field(propertyName)));
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
            case PropertyType.BOOLEAN: {
                in = newPropertyRestrictionQuery(field, pr, value -> value.getValue(Type.BOOLEAN));
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
