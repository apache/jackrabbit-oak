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

import javax.jcr.PropertyType;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.collect.FluentIterable;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.fv.SimSearchUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.MoreLikeThisHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.PathStoredFieldVisitor;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SpellcheckHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.SizeEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PropertyIndexResult;
import org.apache.jackrabbit.oak.plugins.index.search.util.QueryUtils;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.postingshighlight.PostingsHighlighter;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.DOUBLE;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexLookupUtil.LUCENE_INDEX_DEFINITION_PREDICATE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.DYNAMIC_BOOST_WEIGHT;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.NATIVE_SORT_ORDER;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newAncestorTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newName;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_PATH;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.REP_EXCERPT;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.MUST_NOT;

/**
 *
 * Used to query new (compatVersion 2) Lucene indexes.
 *
 * Provides a QueryIndex that does lookups against a Lucene-based index
 *
 * <p>
 * To define a lucene index on a subtree you have to add an
 * <code>oak:index</code> node.
 *
 * Under it follows the index definition node that:
 * <ul>
 * <li>must be of type <code>oak:QueryIndexDefinition</code></li>
 * <li>must have the <code>type</code> property set to <b><code>lucene</code></b></li>
 * <li>must have the <code>async</code> property set to <b><code>async</code></b></li>
 * </ul>
 * <p>
 * Optionally you can add
 * <ul>
 * <li>what subset of property types to be included in the index via the <code>includePropertyTypes</code> property</li>
 * <li>a blacklist of property names: what property to be excluded from the index via the <code>excludePropertyNames</code> property</li>
 * <li>the <code>reindex</code> flag which when set to <code>true</code>, triggers a full content re-index.</li>
 * </ul>
 * <pre>{@code
 * {
 *     NodeBuilder index = root.child("oak:index");
 *     index.child("lucene")
 *         .setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME)
 *         .setProperty("type", "lucene")
 *         .setProperty("async", "async")
 *         .setProperty("reindex", "true");
 * }
 * }</pre>
 *
 * @see org.apache.jackrabbit.oak.spi.query.QueryIndex
 *
 */
public class LucenePropertyIndex extends FulltextIndex {

    private static final Logger LOG = LoggerFactory
            .getLogger(LucenePropertyIndex.class);
    private static final PerfLogger PERF_LOGGER =
        new PerfLogger(LoggerFactory.getLogger(LucenePropertyIndex.class.getName() + ".perf"));

    private final static long LOAD_DOCS_WARN = Long.getLong("oak.lucene.loadDocsWarn", 30 * 1000L);
    private final static long LOAD_DOCS_STOP = Long.getLong("oak.lucene.loadDocsStop", 3 * 60 * 1000L);
    private final static boolean NON_LAZY = SystemPropertySupplier.create("oak.lucene.nonLazyIndex", true).loggingTo(LOG).get();
    public final static String OLD_FACET_PROVIDER_CONFIG_NAME = "oak.lucene.oldFacetProvider";
    private final static boolean OLD_FACET_PROVIDER = Boolean.getBoolean(OLD_FACET_PROVIDER_CONFIG_NAME);
    public final static String CACHE_FACET_RESULTS_NAME = "oak.lucene.cacheFacetResults";
    private final boolean CACHE_FACET_RESULTS =
            Boolean.parseBoolean(System.getProperty(CACHE_FACET_RESULTS_NAME, "true"));
    public final static String EAGER_FACET_CACHE_FILL_NAME = "oak.lucene.cacheFacetEagerFill";
    private final static boolean EAGER_FACET_CACHE_FILL =
            Boolean.parseBoolean(System.getProperty(EAGER_FACET_CACHE_FILL_NAME, "true"));

    private static boolean FLAG_CACHE_FACET_RESULTS_CHANGE = true;

    /**
     * Batch size for fetching results from Lucene queries.
     */
    public static final int LUCENE_QUERY_BATCH_SIZE = 50;

    protected final IndexTracker tracker;

    private final Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter("<strong>", "</strong>"),
        new SimpleHTMLEncoder(), null);

    private final PostingsHighlighter postingsHighlighter = new PostingsHighlighter();

    private final IndexAugmentorFactory augmentorFactory;

    static {
        if (!NON_LAZY) {
            LOG.warn("Lazy index download is enabled explicitly; this is not recommended, see OAK-10102.");
        }
    }

    public LucenePropertyIndex(IndexTracker tracker) {
        this(tracker, null);
    }

    public LucenePropertyIndex(IndexTracker tracker, IndexAugmentorFactory augmentorFactory) {
        this.tracker = tracker;
        this.augmentorFactory = augmentorFactory;
        logConfigsOnce();
    }

    private void logConfigsOnce() {
        if (FLAG_CACHE_FACET_RESULTS_CHANGE) {
            LOG.info(OLD_FACET_PROVIDER_CONFIG_NAME + " = " + OLD_FACET_PROVIDER);
            LOG.info(CACHE_FACET_RESULTS_NAME + " = " + CACHE_FACET_RESULTS);
            FLAG_CACHE_FACET_RESULTS_CHANGE = false;
        }
    }

    @Override
    public String getIndexName() {
        return "lucene-property";
    }

    @Override
    public Cursor query(final IndexPlan plan, NodeState rootState) {
        if (plan.isDeprecated()) {
            LOG.warn("This index is deprecated: {}; it is used for query {}. " +
                    "Please change the query or the index definitions.", plan.getPlanName(), plan.getFilter());
        }
        final Filter filter = plan.getFilter();
        final Sort sort = getSort(plan);
        final PlanResult pr = getPlanResult(plan);
        QueryLimits settings = filter.getQueryLimits();
        LuceneResultRowIterator rItr = new LuceneResultRowIterator() {
            private final Deque<FulltextResultRow> queue = new ArrayDeque<>();
            private final Set<String> seenPaths = new HashSet<>();
            private ScoreDoc lastDoc;
            private int nextBatchSize = LUCENE_QUERY_BATCH_SIZE;
            private boolean noDocs = false;
            private IndexSearcher indexSearcher;
            private int indexNodeId = -1;
            private FacetProvider facetProvider;
            private int rewoundCount = 0;

            @Override
            protected FulltextResultRow computeNext() {
                while (!queue.isEmpty() || loadDocs()) {
                    return queue.remove();
                }
                releaseSearcher();
                return endOfData();
            }

            @Override
            public int rewoundCount() {
                return rewoundCount;
            }

            private FulltextResultRow convertToRow(ScoreDoc doc, IndexSearcher searcher, Map<String, String> excerpts,
                                                   FacetProvider facetProvider,
                                                   String explanation) throws IOException {
                IndexReader reader = searcher.getIndexReader();
                //TODO Look into usage of field cache for retrieving the path
                //instead of reading via reader if no of docs in index are limited
                PathStoredFieldVisitor visitor = new PathStoredFieldVisitor();
                reader.document(doc.doc, visitor);
                String path = visitor.getPath();
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

                        // avoid duplicate entries
                        if (seenPaths.contains(path)) {
                            LOG.trace("Ignoring path {} : Duplicate post transformation", originalPath);
                            return null;
                        }
                        seenPaths.add(path);
                    }

                    boolean shouldIncludeForHierarchy = shouldInclude(path, plan);
                    LOG.trace("Matched path {}; shouldIncludeForHierarchy: {}", path, shouldIncludeForHierarchy);
                    return shouldIncludeForHierarchy ? new FulltextResultRow(path, doc.score, excerpts,
                            facetProvider, explanation)
                            : null;
                }
                return null;
            }

            /**
             * Loads the lucene documents in batches
             * @return true if any document is loaded
             */
            private boolean loadDocs() {

                if (noDocs) {
                    return false;
                }

                ScoreDoc lastDocToRecord = null;

                final LuceneIndexNode indexNode = acquireIndexNode(plan);
                Validate.checkState(indexNode != null);
                try {
                    IndexSearcher searcher = getCurrentSearcher(indexNode);
                    LuceneRequestFacade luceneRequestFacade = getLuceneRequest(plan, augmentorFactory, searcher.getIndexReader());
                    if (luceneRequestFacade.getLuceneRequest() instanceof Query) {
                        Query query = (Query) luceneRequestFacade.getLuceneRequest();

                        TopDocs docs;
                        long start = PERF_LOGGER.start();
                        long startLoop = System.currentTimeMillis();
                        for (int repeated = 0;; repeated++) {
                            if (repeated > 0) {
                                long now = System.currentTimeMillis();
                                if (now > startLoop + LOAD_DOCS_WARN) {
                                    LOG.warn("loadDocs lastDoc {} repeated {} times for query {}", lastDoc, repeated, query);
                                    if (repeated > 1 && now > startLoop + LOAD_DOCS_STOP) {
                                        LOG.error("loadDocs stops", new Exception());
                                        break;
                                    }
                                }
                            }
                            if (lastDoc != null) {
                                LOG.debug("loading the next {} entries for query {}", nextBatchSize, query);
                                if (sort == null) {
                                    docs = searcher.searchAfter(lastDoc, query, nextBatchSize);
                                } else {
                                    docs = searcher.searchAfter(lastDoc, query, nextBatchSize, sort);
                                }
                            } else {
                                LOG.debug("loading the first {} entries for query {}", nextBatchSize, query);
                                if (sort == null) {
                                    docs = searcher.search(query, nextBatchSize);
                                } else {
                                    docs = searcher.search(query, nextBatchSize, sort);
                                }
                            }
                            PERF_LOGGER.end(start, -1, "{} ...", docs.scoreDocs.length);
                            nextBatchSize = (int) Math.min(nextBatchSize * 2L, 100000);

                            if (facetProvider == null) {
                                long f = PERF_LOGGER.start();
                                if (OLD_FACET_PROVIDER) {
                                    // here the current searcher gets referenced for later
                                    // but the searcher might get closed in the meantime
                                    facetProvider = new LuceneFacetProvider(
                                            FacetHelper.getFacets(searcher, query, plan, indexNode.getDefinition().getSecureFacetConfiguration())
                                    );
                                } else {
                                    // a new searcher is opened and closed when needed
                                    facetProvider = new DelayedLuceneFacetProvider(LucenePropertyIndex.this, query, plan, indexNode.getDefinition().getSecureFacetConfiguration());
                                }
                                PERF_LOGGER.end(f, -1, "facets retrieved");
                            }

                            Set<String> excerptFields = new HashSet<>();
                            for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
                                if (QueryConstants.REP_EXCERPT.equals(pr.propertyName)) {
                                    String value = pr.first.getValue(Type.STRING);
                                    excerptFields.add(value);
                                }
                            }
                            boolean addExcerpt = excerptFields.size() > 0;

                            PropertyRestriction restriction = filter.getPropertyRestriction(QueryConstants.OAK_SCORE_EXPLANATION);
                            boolean addExplain = restriction != null && restriction.isNotNullRestriction();

                            Analyzer analyzer = indexNode.getDefinition().getAnalyzer();

                            FieldInfos mergedFieldInfos = null;
                            if (addExcerpt) {
                                // setup highlighter
                                QueryScorer scorer = new QueryScorer(query);
                                scorer.setExpandMultiTermQuery(true);
                                highlighter.setFragmentScorer(scorer);
                                mergedFieldInfos = MultiFields.getMergedFieldInfos(searcher.getIndexReader());
                            }

                            boolean earlyStop = false;
                            if (docs.scoreDocs.length > 1) {
                                // reranking step for fv sim search
                                PropertyRestriction pr = null;
                                LuceneIndexDefinition defn = indexNode.getDefinition();
                                if (defn.hasFunctionDefined()) {
                                    pr = filter.getPropertyRestriction(defn.getFunctionName());
                                }
                                if (pr != null) {
                                    String queryString = String.valueOf(pr.first.getValue(pr.first.getType()));
                                    if (queryString.startsWith("mlt?")) {
                                        List<PropertyDefinition> sp = new LinkedList<>();
                                        for (IndexingRule r : defn.getDefinedRules()) {
                                            List<PropertyDefinition> similarityProperties = r.getSimilarityProperties();
                                            for (PropertyDefinition pd : similarityProperties) {
                                                if (pd.similarityRerank) {
                                                    sp.add(pd);
                                                }
                                            }
                                        }
                                        if (!sp.isEmpty()) {
                                            long fvs = PERF_LOGGER.start();
                                            SimSearchUtils.bruteForceFVRerank(sp, docs, indexSearcher);
                                            PERF_LOGGER.end(fvs, -1, "fv reranking done");
                                            earlyStop = true;
                                        }
                                    }
                                }
                            }

                            for (ScoreDoc doc : docs.scoreDocs) {
                                Map<String, String> excerpts = null;
                                if (addExcerpt) {
                                    excerpts = getExcerpt(query, excerptFields, analyzer, searcher, doc, mergedFieldInfos);
                                }

                                String explanation = null;
                                if (addExplain) {
                                    explanation = searcher.explain(query, doc.doc).toString();
                                }

                                FulltextResultRow row = convertToRow(doc, searcher, excerpts, facetProvider, explanation);
                                if (row != null) {
                                    queue.add(row);
                                }
                                lastDocToRecord = doc;
                            }

                            if (earlyStop) {
                                noDocs = true;
                                break;
                            }
                            if (queue.isEmpty() && docs.scoreDocs.length > 0) {
                                //queue is still empty but more results can be fetched
                                //from Lucene so still continue
                                lastDoc = lastDocToRecord;
                            } else {
                                break;
                            }
                        }
                    } else if (luceneRequestFacade.getLuceneRequest() instanceof SpellcheckHelper.SpellcheckQuery) {
                        String aclCheckField = indexNode.getDefinition().isFullTextEnabled() ? FieldNames.FULLTEXT : FieldNames.SPELLCHECK;
                        noDocs = true;
                        SpellcheckHelper.SpellcheckQuery spellcheckQuery = (SpellcheckHelper.SpellcheckQuery) luceneRequestFacade.getLuceneRequest();
                        SuggestWord[] suggestWords = SpellcheckHelper.getSpellcheck(spellcheckQuery);

                        // ACL filter spellchecks
                        QueryParser qp = new QueryParser(Version.LUCENE_47, aclCheckField, indexNode.getDefinition().getAnalyzer());
                        for (SuggestWord suggestion : suggestWords) {
                            Query query = qp.createPhraseQuery(aclCheckField, QueryParserBase.escape(suggestion.string));

                            query = addDescendantClauseIfRequired(query, plan);

                            TopDocs topDocs = searcher.search(query, 100);
                            if (topDocs.totalHits > 0) {
                                for (ScoreDoc doc : topDocs.scoreDocs) {
                                    Document retrievedDoc = searcher.doc(doc.doc);
                                    String prefix = filter.getPath();
                                    if (prefix.length() == 1) {
                                        prefix = "";
                                    }
                                    if (filter.isAccessible(prefix + retrievedDoc.get(FieldNames.PATH))) {
                                        queue.add(new FulltextResultRow(suggestion.string));
                                        break;
                                    }
                                }
                            }
                        }

                    } else if (luceneRequestFacade.getLuceneRequest() instanceof SuggestHelper.SuggestQuery) {
                        SuggestHelper.SuggestQuery suggestQuery = (SuggestHelper.SuggestQuery) luceneRequestFacade.getLuceneRequest();
                        noDocs = true;

                        List<Lookup.LookupResult> lookupResults = SuggestHelper.getSuggestions(indexNode.getLookup(), suggestQuery);

                        QueryParser qp = new QueryParser(Version.LUCENE_47, FieldNames.SUGGEST,
                                indexNode.getDefinition().isSuggestAnalyzed() ? indexNode.getDefinition().getAnalyzer() :
                                SuggestHelper.getAnalyzer());

                        // ACL filter suggestions
                        for (Lookup.LookupResult suggestion : lookupResults) {
                            Query query = qp.parse("\"" + QueryParserBase.escape(suggestion.key.toString()) + "\"");

                            query = addDescendantClauseIfRequired(query, plan);

                            TopDocs topDocs = searcher.search(query, 100);
                            if (topDocs.totalHits > 0) {
                                for (ScoreDoc doc : topDocs.scoreDocs) {
                                    Document retrievedDoc = searcher.doc(doc.doc);
                                    String prefix = filter.getPath();
                                    if (prefix.length() == 1) {
                                        prefix = "";
                                    }
                                    if (filter.isAccessible(prefix + retrievedDoc.get(FieldNames.PATH))) {
                                        queue.add(new FulltextResultRow(suggestion.key.toString(), suggestion.value));
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("query [{}] via {} failed.", plan.getFilter(), LucenePropertyIndex.this.getClass().getCanonicalName(), e);
                } finally {
                    indexNode.release();
                }

                if (lastDocToRecord != null) {
                    this.lastDoc = lastDocToRecord;
                }

                return !queue.isEmpty();
            }

            private IndexSearcher getCurrentSearcher(LuceneIndexNode indexNode) {
                //The searcher once obtained is held till either cursor is finished
                //or if the index gets updated. It needs to be ensured that
                //searcher is obtained via this method only in this iterator

                //Refresh the searcher if change in indexNode is detected
                //For NRT case its fine to keep a reference to searcher i.e. not
                //acquire it for every loadDocs call otherwise with frequent change
                //the reset of lastDoc would happen very frequently.
                //Upon LuceneIndexNode change i.e. when new async index update is detected
                //the searcher would be refreshed as done earlier
                if (indexNodeId != indexNode.getIndexNodeId()) {
                    //if already initialized then log about change
                    if (indexNodeId > 0) {
                        LOG.info("Change in index version detected. Query would be performed without offset");
                        rewoundCount++;
                    }

                    indexSearcher = indexNode.getSearcher();
                    indexNodeId = indexNode.getIndexNodeId();
                    lastDoc = null;
                }
                return indexSearcher;
            }

            private void releaseSearcher() {
                //For now nullifying it.
                indexSearcher = null;
            }
        };
        Iterator<FulltextResultRow> itr = rItr;
        SizeEstimator sizeEstimator = getSizeEstimator(plan);

        if (pr.hasPropertyIndexResult() || pr.evaluateSyncNodeTypeRestriction()) {
            itr = mergePropertyIndexResult(plan, rootState, itr);
        }

        return new FulltextPathCursor(itr, rItr, plan, settings, sizeEstimator);
    }

    private static Query addDescendantClauseIfRequired(Query query, IndexPlan plan) {
        Filter filter = plan.getFilter();

        if (filter.getPathRestriction() == Filter.PathRestriction.ALL_CHILDREN) {
            String path = getPathRestriction(plan);
            if (!PathUtils.denotesRoot(path)) {
                if (getPlanResult(plan).indexDefinition.evaluatePathRestrictions()) {

                    BooleanQuery compositeQuery = new BooleanQuery();
                    compositeQuery.add(query, BooleanClause.Occur.MUST);

                    Query pathQuery = new TermQuery(newAncestorTerm(path));
                    compositeQuery.add(pathQuery, BooleanClause.Occur.MUST);

                    query = compositeQuery;
                } else {
                    LOG.warn("Descendant clause could not be added without path restrictions enabled. Plan: {}", plan);
                }
            }
        }

        return query;
    }

    private Map<String, String> getExcerpt(Query query, Set<String> excerptFields,
                              Analyzer analyzer, IndexSearcher searcher, ScoreDoc doc, FieldInfos fieldInfos)
            throws IOException {
        Set<String> excerptFieldNames = new HashSet<>();
        Map<String, String> fieldNameToColumnNameMap = new HashMap<>();
        Map<String, String> columnNameToExcerpts = new HashMap<>();
        Set<String> nodeExcerptColumns = new HashSet<>();

        excerptFields.forEach(columnName -> {
            String fieldName;
            if (REP_EXCERPT.equals(columnName)) {
                fieldName = FulltextIndexConstants.EXCERPT_NODE_FIELD_NAME;
            } else {
                fieldName = columnName.substring(REP_EXCERPT.length() + 1, columnName.length() - 1);
            }

            if (!FulltextIndexConstants.EXCERPT_NODE_FIELD_NAME.equals(fieldName)) {
                excerptFieldNames.add(fieldName);
                fieldNameToColumnNameMap.put(fieldName, columnName);
            } else {
                nodeExcerptColumns.add(columnName);
            }
        });

        final boolean requireNodeLevelExcerpt = nodeExcerptColumns.size() > 0;

        int docID = doc.doc;
        List<String> names = new LinkedList<>();

        for (IndexableField field : searcher.getIndexReader().document(docID).getFields()) {
            String name = field.name();
            // postings highlighter can be used on analyzed fields with docs, freqs, positions and offsets stored.
            if (name.startsWith(FieldNames.ANALYZED_FIELD_PREFIX) && fieldInfos.hasProx() && fieldInfos.hasOffsets()) {
                names.add(name);
            }
        }

        if (!requireNodeLevelExcerpt) {
            names.retainAll(excerptFieldNames);
        }

        if (names.size() > 0) {
            int[] maxPassages = new int[names.size()];
            Arrays.fill(maxPassages, 1);
            try {
                Map<String, String[]> stringMap = postingsHighlighter.highlightFields(names.toArray(new String[names.size()]),
                        query, searcher, new int[]{docID}, maxPassages);
                for (Map.Entry<String, String[]> entry : stringMap.entrySet()) {
                    String value = Arrays.toString(entry.getValue());
                    if (value.contains("<b>")) {
                        String fieldName = entry.getKey();
                        String columnName = fieldNameToColumnNameMap.get(fieldName);

                        columnNameToExcerpts.put(columnName, value);
                    }
                }
            } catch (Exception e) {
                LOG.debug("postings highlighting failed", e);
            }
        }

        // fallback if no excerpt could be retrieved using postings highlighter
        if (columnNameToExcerpts.size() == 0) {
            for (IndexableField field : searcher.getIndexReader().document(doc.doc).getFields()) {
                String name = field.name();
                // only full text or analyzed fields
                if (name.startsWith(FieldNames.FULLTEXT) || name.startsWith(FieldNames.ANALYZED_FIELD_PREFIX)) {
                    String text = field.stringValue();
                    TokenStream tokenStream = analyzer.tokenStream(name, text);

                    try {
                        TextFragment[] textFragments = highlighter.getBestTextFragments(tokenStream, text, true, 1);
                        if (textFragments != null && textFragments.length > 0) {
                            for (TextFragment fragment : textFragments) {
                                String columnName = null;
                                if (name.startsWith(FieldNames.ANALYZED_FIELD_PREFIX)) {
                                    columnName = fieldNameToColumnNameMap.get(name.substring(FieldNames.ANALYZED_FIELD_PREFIX.length()));
                                }
                                if (columnName == null && requireNodeLevelExcerpt) {
                                    columnName = name;
                                }

                                if (columnName != null) {
                                    columnNameToExcerpts.put(columnName, fragment.toString());
                                }
                            }
                            if (excerptFieldNames.size() == 0) {
                                break;
                            }
                        }
                    } catch (InvalidTokenOffsetsException e) {
                        LOG.error("highlighting failed", e);
                    }
                }
            }
        }

        if (requireNodeLevelExcerpt) {
            String nodeExcerpt = String.join("...", columnNameToExcerpts.values());

            nodeExcerptColumns.forEach(nodeExcerptColumnName -> columnNameToExcerpts.put(nodeExcerptColumnName, nodeExcerpt));
        }

        columnNameToExcerpts.keySet().retainAll(excerptFields);

        return columnNameToExcerpts;
    }

    @Override
    protected LuceneIndexNode acquireIndexNode(String indexPath) {
        if (NON_LAZY) {
            return tracker.acquireIndexNode(indexPath);
        }
        return new LazyLuceneIndexNode(tracker, indexPath);
    }

    @Override
    protected LuceneIndexNode acquireIndexNode(IndexPlan plan) {
        return (LuceneIndexNode) super.acquireIndexNode(plan);
    }

    @Override
    protected String getType() {
        return TYPE_LUCENE;
    }

    @Override
    protected boolean filterReplacedIndexes() {
        return tracker.getMountInfoProvider().hasNonDefaultMounts();
    }

    @Override
    protected boolean runIsActiveIndexCheck() {
        return filterReplacedIndexes();
    }

    @Override
    protected SizeEstimator getSizeEstimator(IndexPlan plan) {
        return () -> {
            LuceneIndexNode indexNode = acquireIndexNode(plan);
            Validate.checkState(indexNode != null);
            try {
                IndexSearcher searcher = indexNode.getSearcher();
                LuceneRequestFacade luceneRequestFacade = getLuceneRequest(plan, augmentorFactory, searcher.getIndexReader());
                if (luceneRequestFacade.getLuceneRequest() instanceof Query) {
                    Query query = (Query) luceneRequestFacade.getLuceneRequest();
                    TotalHitCountCollector collector = new TotalHitCountCollector();
                    searcher.search(query, collector);
                    int totalHits = collector.getTotalHits();
                    LOG.debug("Estimated size for query {} is {}", query, totalHits);
                    return totalHits;
                }
                LOG.debug("estimate size: not a Query: {}", luceneRequestFacade.getLuceneRequest());
            } catch (IOException e) {
                LOG.warn("query via {} failed.", LucenePropertyIndex.this, e);
            } finally {
                indexNode.release();
            }
            return -1;
        };
    }

    @Override
    protected Predicate<NodeState> getIndexDefinitionPredicate() {
        return LUCENE_INDEX_DEFINITION_PREDICATE;
    }

    @Override
    protected String getFulltextRequestString(IndexPlan plan, IndexNode indexNode, NodeState root) {
        return getLuceneRequest(plan, augmentorFactory, null).toString();
    }

    private static Sort getSort(IndexPlan plan) {
        List<OrderEntry> sortOrder = plan.getSortOrder();
        if (sortOrder == null || sortOrder.isEmpty()) {
            return null;
        }

        sortOrder = removeNativeSort(sortOrder);
        List<SortField> fieldsList = new ArrayList<>(sortOrder.size());
        PlanResult planResult = getPlanResult(plan);
        for (int i = 0; i < sortOrder.size(); i++) {
            OrderEntry oe = sortOrder.get(i);
            PropertyDefinition pd = planResult.getOrderedProperty(i);
            boolean reverse = oe.getOrder() != OrderEntry.Order.ASCENDING;
            String propName = oe.getPropertyName();
            propName = FieldNames.createDocValFieldName(propName);
            fieldsList.add(new SortField(propName, toLuceneSortType(oe, pd), reverse));
        }

        if (fieldsList.isEmpty()) {
            return null;
        } else {
            return new Sort(fieldsList.toArray(new SortField[0]));
        }
    }

    /**
     * Remove all "jcr:score" entries.
     *
     * @param original the original list (is not modified)
     * @return the list with the entries removed
     */
    private static List<OrderEntry> removeNativeSort(List<OrderEntry> original) {
        if (original == null || original.isEmpty()) {
            return original;
        }
        ArrayList<OrderEntry> result = new ArrayList<>();
        for (OrderEntry oe : original) {
            if (!isNativeSort(oe)) {
                result.add(oe);
            }
        }
        return result;
    }

    /**
     * Identifies the default sort order used by the index (@jcr:score descending)
     *
     * @param oe order entry
     * @return
     */
    private static boolean isNativeSort(OrderEntry oe) {
        return oe.getPropertyName().equals(NATIVE_SORT_ORDER.getPropertyName());
    }

    private static SortField.Type toLuceneSortType(OrderEntry oe, PropertyDefinition defn) {
        Type<?> t = oe.getPropertyType();
        Validate.checkState(t != null, "Type cannot be null");
        Validate.checkState(!t.isArray(), "Array types are not supported");

        int type = getPropertyType(defn, oe.getPropertyName(), t.tag());
        switch (type) {
            case PropertyType.LONG:
            case PropertyType.DATE:
                return SortField.Type.LONG;
            case PropertyType.DOUBLE:
                return SortField.Type.DOUBLE;
            default:
                //TODO Check about SortField.Type.STRING_VAL
                return SortField.Type.STRING;
        }
    }

    /**
     * Get the Lucene query for the given filter.
     *
     * @param plan   index plan containing filter details
     * @param reader the Lucene reader
     * @return the Lucene query
     */
    private static LuceneRequestFacade getLuceneRequest(IndexPlan plan, IndexAugmentorFactory augmentorFactory, IndexReader reader) {
        FulltextQueryTermsProvider augmentor = getIndexAgumentor(plan, augmentorFactory);
        List<Query> qs = new ArrayList<>();
        Filter filter = plan.getFilter();
        FullTextExpression ft = filter.getFullTextConstraint();
        PlanResult planResult = getPlanResult(plan);
        LuceneIndexDefinition defn = (LuceneIndexDefinition) planResult.indexDefinition;
        Analyzer analyzer = defn.getAnalyzer();
        if (ft == null) {
            // there might be no full-text constraint
            // when using the LowCostLuceneIndexProvider
            // which is used for testing
        } else {
            qs.add(getFullTextQuery(plan, ft, analyzer, augmentor));
        }


        //Check if native function is supported
        PropertyRestriction pr = null;
        if (defn.hasFunctionDefined()) {
            pr = filter.getPropertyRestriction(defn.getFunctionName());
        }

        if (pr != null) {
            String query = String.valueOf(pr.first.getValue(pr.first.getType()));
            QueryParser queryParser = new QueryParser(VERSION, "", analyzer);
            if (query.startsWith("mlt?")) {
                String mltQueryString = query.replace("mlt?", "");
                if (reader != null) {
                    List<PropertyDefinition> sp = new LinkedList<>();
                    for (IndexingRule r : defn.getDefinedRules()) {
                        sp.addAll(r.getSimilarityProperties());
                    }
                    if (sp.isEmpty()) {
                        Query moreLikeThis = MoreLikeThisHelper.getMoreLikeThis(reader, analyzer, mltQueryString);
                        if (moreLikeThis != null) {
                            qs.add(moreLikeThis);
                        }
                    } else {
                        Query similarityQuery = SimSearchUtils.getSimilarityQuery(sp, reader, mltQueryString);
                        if (similarityQuery != null) {
                            qs.add(similarityQuery);
                        }
                    }
                }
            } else if (query.startsWith("spellcheck?")) {
                String spellcheckQueryString = query.replace("spellcheck?", "");
                if (reader != null) {
                    return new LuceneRequestFacade<>(SpellcheckHelper.getSpellcheckQuery(spellcheckQueryString, reader));
                }
            } else if (query.startsWith("suggest?")) {
                String suggestQueryString = query.replace("suggest?", "");
                if (reader != null) {
                    return new LuceneRequestFacade<>(SuggestHelper.getSuggestQuery(suggestQueryString));
                }
            } else {
                try {
                    qs.add(queryParser.parse(query));
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        } else if (planResult.evaluateNonFullTextConstraints()) {
            addNonFullTextConstraints(qs, plan, reader);
        }

        if (qs.size() == 0
            && plan.getSortOrder() != null) {
            //This case indicates that query just had order by and no
            //property restriction defined. In this case property
            //existence queries for each sort entry
            List<OrderEntry> orders = removeNativeSort(plan.getSortOrder());
            for (int i = 0; i < orders.size(); i++) {
                OrderEntry oe = orders.get(i);
                PropertyDefinition pd = planResult.getOrderedProperty(i);
                PropertyRestriction orderRest = new PropertyRestriction();
                orderRest.propertyName = oe.getPropertyName();
                Query q = createQuery(oe.getPropertyName(), orderRest, pd);
                if (q != null) {
                    qs.add(q);
                }
            }
        }

        if (qs.size() == 0) {
            if (reader == null) {
                //When called in planning mode then some queries like rep:similar
                //cannot create query as reader is not provided. In such case we
                //just return match all queries
                return new LuceneRequestFacade<Query>(new MatchAllDocsQuery());
            }
            //For purely nodeType based queries all the documents would have to
            //be returned (if the index definition has a single rule)
            if (planResult.evaluateNodeTypeRestriction()) {
                return new LuceneRequestFacade<Query>(new MatchAllDocsQuery());
            }

            throw new IllegalStateException("No query created for filter " + filter);
        }
        return performAdditionalWraps(qs);
    }

    /**
     * Perform additional wraps on the list of queries to allow, for example, the NOT CONTAINS to
     * play properly when sent to lucene.
     *
     * @param qs the list of queries. Cannot be null.
     * @return the request facade
     */
    @NotNull
    public static LuceneRequestFacade<Query> performAdditionalWraps(@NotNull List<Query> qs) {
        if (qs.size() == 1) {
            Query q = qs.get(0);
            if (q instanceof BooleanQuery) {
                BooleanQuery ibq = (BooleanQuery) q;
                boolean onlyNotClauses = true;
                for (BooleanClause c : ibq.getClauses()) {
                    if (c.getOccur() != BooleanClause.Occur.MUST_NOT) {
                        onlyNotClauses = false;
                        break;
                    }
                }
                if (onlyNotClauses) {
                    // if we have only NOT CLAUSES we have to add a match all docs (*.*) for the
                    // query to work
                    // This check is needed now for Older version of lucene(Implementation in LuceneIndex.java)
                    ibq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
                }
            }
            return new LuceneRequestFacade<>(qs.get(0));
        }
        BooleanQuery bq = new BooleanQuery();
        for (Query q : qs) {
            boolean unwrapped = false;
            if (q instanceof BooleanQuery) {
                unwrapped = unwrapMustNot((BooleanQuery) q, bq);
            }

            if (!unwrapped) {
                bq.add(q, MUST);
            }
        }
        return new LuceneRequestFacade<>(bq);
    }

    /**
     * unwraps any NOT clauses from the provided boolean query into another boolean query.
     *
     * @param input  the query to be analysed for the existence of NOT clauses. Cannot be null.
     * @param output the query where the unwrapped NOTs will be saved into. Cannot be null.
     * @return true if there where at least one unwrapped NOT. false otherwise.
     */
    private static boolean unwrapMustNot(@NotNull BooleanQuery input, @NotNull BooleanQuery output) {
        requireNonNull(input);
        requireNonNull(output);
        boolean unwrapped = false;
        for (BooleanClause bc : input.getClauses()) {
            if (bc.getOccur() == BooleanClause.Occur.MUST_NOT) {
                output.add(bc);
                unwrapped = true;
            }
        }
        if (unwrapped) {
            // if we have unwrapped "must not" conditions,
            // then we need to unwrap "must" conditions as well
            for (BooleanClause bc : input.getClauses()) {
                if (bc.getOccur() == BooleanClause.Occur.MUST) {
                    output.add(bc);
                }
            }
        }

        return unwrapped;
    }

    private static FulltextQueryTermsProvider getIndexAgumentor(IndexPlan plan, IndexAugmentorFactory augmentorFactory) {
        PlanResult planResult = getPlanResult(plan);

        if (augmentorFactory != null) {
            return augmentorFactory.getFulltextQueryTermsProvider(planResult.indexingRule.getNodeTypeName());
        }

        return null;
    }

    private static void addNonFullTextConstraints(List<Query> qs,
                                                  IndexPlan plan, IndexReader reader) {
        Filter filter = plan.getFilter();
        PlanResult planResult = getPlanResult(plan);
        IndexDefinition defn = planResult.indexDefinition;
        if (!filter.matchesAllTypes()) {
            addNodeTypeConstraints(planResult.indexingRule, qs, filter);
        }

        String path = getPathRestriction(plan);
        switch (filter.getPathRestriction()) {
            case ALL_CHILDREN:
                if (defn.evaluatePathRestrictions()) {
                    if ("/".equals(path)) {
                        break;
                    }
                    qs.add(new TermQuery(newAncestorTerm(path)));
                }
                break;
            case DIRECT_CHILDREN:
                if (defn.evaluatePathRestrictions()) {
                    BooleanQuery bq = new BooleanQuery();
                    bq.add(new BooleanClause(new TermQuery(newAncestorTerm(path)), BooleanClause.Occur.MUST));
                    bq.add(new BooleanClause(newDepthQuery(path, planResult), BooleanClause.Occur.MUST));
                    qs.add(bq);
                }
                break;
            case EXACT:
                // For transformed paths, we can only add path restriction if absolute path to property can be
                // deduced
                if (planResult.isPathTransformed()) {
                    String parentPathSegment = planResult.getParentPathSegment();
                    if (!CollectionUtils.toStream(PathUtils.elements(parentPathSegment)).anyMatch("*"::equals)) {
                        qs.add(new TermQuery(newPathTerm(path + parentPathSegment)));
                    }
                } else {
                    qs.add(new TermQuery(newPathTerm(path)));
                }
                break;
            case PARENT:
                if (denotesRoot(path)) {
                    // there's no parent of the root node
                    // we add a path that can not possibly occur because there
                    // is no way to say "match no documents" in Lucene
                    qs.add(new TermQuery(new Term(FieldNames.PATH, "///")));
                } else {
                    // For transformed paths, we can only add path restriction if absolute path to property can be
                    // deduced
                    if (planResult.isPathTransformed()) {
                        String parentPathSegment = planResult.getParentPathSegment();
                        if (!CollectionUtils.toStream(PathUtils.elements(parentPathSegment)).anyMatch("*"::equals)) {
                            qs.add(new TermQuery(newPathTerm(getParentPath(path) + parentPathSegment)));
                        }
                    } else {
                        qs.add(new TermQuery(newPathTerm(getParentPath(path))));
                    }
                }
                break;
            case NO_RESTRICTION:
                break;
        }

        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String name = pr.propertyName;

            if (QueryConstants.REP_EXCERPT.equals(name) || QueryConstants.OAK_SCORE_EXPLANATION.equals(name)
                    || QueryConstants.REP_FACET.equals(name)) {
                continue;
            }

            if (QueryConstants.RESTRICTION_LOCAL_NAME.equals(name)) {
                if (planResult.evaluateNodeNameRestriction()) {
                    Query q = createNodeNameQuery(pr);
                    if (q != null) {
                        qs.add(q);
                    }
                }
                continue;
            }

            if (IndexConstants.INDEX_TAG_OPTION.equals(name) ||
                    IndexConstants.INDEX_NAME_OPTION.equals(name)) {
                continue;
            }

            if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                    && pr.lastIncluding) {
                String first = pr.first.getValue(STRING);
                first = first.replace("\\", "");
                if (JCR_PATH.equals(name)) {
                    qs.add(new TermQuery(newPathTerm(first)));
                    continue;
                } else if ("*".equals(name)) {
                    //TODO Revisit reference constraint. For performant impl
                    //references need to be indexed in a different manner
                    addReferenceConstraint(first, qs, reader);
                    continue;
                }
            }

            PropertyDefinition pd = planResult.getPropDefn(pr);
            if (pd == null) {
                continue;
            }

            Query q = createQuery(planResult.getPropertyName(pr), pr, pd);
            if (q != null) {
                qs.add(q);
            }
        }
    }

    private static Query createLikeQuery(String name, String first) {
        first = QueryUtils.sqlLikeToLuceneWildcardQuery(first);

        int indexOfWS = first.indexOf(WildcardQuery.WILDCARD_STRING);
        int indexOfWC = first.indexOf(WildcardQuery.WILDCARD_CHAR);
        int len = first.length();

        if (indexOfWS == len || indexOfWC == len) {
            // remove trailing "*" for prefix query
            first = first.substring(0, first.length() - 1);
            if (JCR_PATH.equals(name)) {
                return new PrefixQuery(newPathTerm(first));
            } else {
                return new PrefixQuery(new Term(name, first));
            }
        } else {
            if (JCR_PATH.equals(name)) {
                return new WildcardQuery(newPathTerm(first));
            } else {
                return new WildcardQuery(new Term(name, first));
            }
        }
    }

    @Nullable
    private static Query createQuery(String propertyName, PropertyRestriction pr,
                                     PropertyDefinition defn) {
        int propType = determinePropertyType(defn, pr);

        if (pr.isNullRestriction()) {
            return new TermQuery(new Term(FieldNames.NULL_PROPS, defn.name));
        }

        //If notNullCheckEnabled explicitly enabled use the simple TermQuery
        //otherwise later fallback to range query
        if (pr.isNotNullRestriction() && defn.notNullCheckEnabled) {
            return new TermQuery(new Term(FieldNames.NOT_NULL_PROPS, defn.name));
        }

        switch (propType) {
            case PropertyType.DATE: {
                Long first = pr.first != null ? FieldFactory.dateToLong(pr.first.getValue(Type.DATE)) : null;
                Long last = pr.last != null ? FieldFactory.dateToLong(pr.last.getValue(Type.DATE)) : null;
                Long not = pr.not != null ? FieldFactory.dateToLong(pr.not.getValue(Type.DATE)) : null;
                if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                        && pr.lastIncluding) {
                    // [property]=[value]
                    return NumericRangeQuery.newLongRange(propertyName, first, first, true, true);
                } else if (pr.first != null && pr.last != null) {
                    return NumericRangeQuery.newLongRange(propertyName, first, last,
                            pr.firstIncluding, pr.lastIncluding);
                } else if (pr.first != null && pr.last == null) {
                    // '>' & '>=' use cases
                    return NumericRangeQuery.newLongRange(propertyName, first, null, pr.firstIncluding, true);
                } else if (pr.last != null && !pr.last.equals(pr.first)) {
                    // '<' & '<='
                    return NumericRangeQuery.newLongRange(propertyName, null, last, true, pr.lastIncluding);
                } else if (pr.list != null) {
                    BooleanQuery in = new BooleanQuery();
                    for (PropertyValue value : pr.list) {
                        Long dateVal = FieldFactory.dateToLong(value.getValue(Type.DATE));
                        in.add(NumericRangeQuery.newLongRange(propertyName, dateVal, dateVal, true, true), BooleanClause.Occur.SHOULD);
                    }
                    return in;
                } else if (pr.isNotNullRestriction()) {
                    // not null. As we are indexing generic dates which can be beyond epoch. So using complete numeric range
                    return NumericRangeQuery.newLongRange(propertyName, Long.MIN_VALUE, Long.MAX_VALUE, true, true);
                } else if (pr.isNot && pr.not != null) {
                    // -[property]=[value]
                    BooleanQuery bool = new BooleanQuery();
                    // This will exclude entries with [property]=[value]
                    bool.add(NumericRangeQuery.newLongRange(propertyName, not, not, true, true), MUST_NOT);
                    return bool;
                }

                break;
            }
            case PropertyType.DOUBLE: {
                Double first = pr.first != null ? pr.first.getValue(DOUBLE) : null;
                Double last = pr.last != null ? pr.last.getValue(DOUBLE) : null;
                if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                        && pr.lastIncluding) {
                    // [property]=[value]
                    return NumericRangeQuery.newDoubleRange(propertyName, first, first, true, true);
                } else if (pr.first != null && pr.last != null) {
                    return NumericRangeQuery.newDoubleRange(propertyName, first, last,
                            pr.firstIncluding, pr.lastIncluding);
                } else if (pr.first != null && pr.last == null) {
                    // '>' & '>=' use cases
                    return NumericRangeQuery.newDoubleRange(propertyName, first, null, pr.firstIncluding, true);
                } else if (pr.last != null && !pr.last.equals(pr.first)) {
                    // '<' & '<='
                    return NumericRangeQuery.newDoubleRange(propertyName, null, last, true, pr.lastIncluding);
                } else if (pr.list != null) {
                    BooleanQuery in = new BooleanQuery();
                    for (PropertyValue value : pr.list) {
                        Double doubleVal = value.getValue(DOUBLE);
                        in.add(NumericRangeQuery.newDoubleRange(propertyName, doubleVal, doubleVal, true, true), BooleanClause.Occur.SHOULD);
                    }
                    return in;
                } else if (pr.isNotNullRestriction()) {
                    // not null.
                    return NumericRangeQuery.newDoubleRange(propertyName, Double.MIN_VALUE, Double.MAX_VALUE, true, true);
                } else if (pr.isNot && pr.not != null) {
                    // -[property]=[value]
                    BooleanQuery bool = new BooleanQuery();
                    // This will exclude entries with [property]=[value]
                    bool.add(NumericRangeQuery.newDoubleRange(propertyName, pr.not.getValue(DOUBLE), pr.not.getValue(DOUBLE), true, true), MUST_NOT);
                    return bool;
                }
                break;
            }
            case PropertyType.LONG: {
                Long first = pr.first != null ? pr.first.getValue(LONG) : null;
                Long last = pr.last != null ? pr.last.getValue(LONG) : null;
                if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                        && pr.lastIncluding) {
                    // [property]=[value]
                    return NumericRangeQuery.newLongRange(propertyName, first, first, true, true);
                } else if (pr.first != null && pr.last != null) {
                    return NumericRangeQuery.newLongRange(propertyName, first, last,
                            pr.firstIncluding, pr.lastIncluding);
                } else if (pr.first != null && pr.last == null) {
                    // '>' & '>=' use cases
                    return NumericRangeQuery.newLongRange(propertyName, first, null, pr.firstIncluding, true);
                } else if (pr.last != null && !pr.last.equals(pr.first)) {
                    // '<' & '<='
                    return NumericRangeQuery.newLongRange(propertyName, null, last, true, pr.lastIncluding);
                } else if (pr.list != null) {
                    BooleanQuery in = new BooleanQuery();
                    for (PropertyValue value : pr.list) {
                        Long longVal = value.getValue(LONG);
                        in.add(NumericRangeQuery.newLongRange(propertyName, longVal, longVal, true, true), BooleanClause.Occur.SHOULD);
                    }
                    return in;
                } else if (pr.isNotNullRestriction()) {
                    // not null.
                    return NumericRangeQuery.newLongRange(propertyName, Long.MIN_VALUE, Long.MAX_VALUE, true, true);
                } else if (pr.isNot && pr.not != null) {
                    // -[property]=[value]
                    BooleanQuery bool = new BooleanQuery();
                    // This will exclude entries with [property]=[value]
                    bool.add(NumericRangeQuery.newLongRange(propertyName, pr.not.getValue(LONG), pr.not.getValue(LONG), true, true), MUST_NOT);
                    return bool;
                }
                break;
            }
            default: {
                if (pr.isLike) {
                    return createLikeQuery(propertyName, pr.first.getValue(STRING));
                }

                //TODO Confirm that all other types can be treated as string
                String first = pr.first != null ? pr.first.getValue(STRING) : null;
                String last = pr.last != null ? pr.last.getValue(STRING) : null;
                if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                        && pr.lastIncluding) {
                    // [property]=[value]
                    return new TermQuery(new Term(propertyName, first));
                } else if (pr.first != null && pr.last != null) {
                    return TermRangeQuery.newStringRange(propertyName, first, last,
                            pr.firstIncluding, pr.lastIncluding);
                } else if (pr.first != null && pr.last == null) {
                    // '>' & '>=' use cases
                    return TermRangeQuery.newStringRange(propertyName, first, null, pr.firstIncluding, true);
                } else if (pr.last != null && !pr.last.equals(pr.first)) {
                    // '<' & '<='
                    return TermRangeQuery.newStringRange(propertyName, null, last, true, pr.lastIncluding);
                } else if (pr.list != null) {
                    BooleanQuery in = new BooleanQuery();
                    for (PropertyValue value : pr.list) {
                        String strVal = value.getValue(STRING);
                        in.add(new TermQuery(new Term(propertyName, strVal)), BooleanClause.Occur.SHOULD);
                    }
                    return in;
                } else if (pr.isNotNullRestriction()) {
                    return new TermRangeQuery(propertyName, null, null, true, true);
                } else if (pr.isNot && pr.not != null) {
                    // -[property]=[value]
                    BooleanQuery bool = new BooleanQuery();
                    // This will exclude entries with [property]=[value]
                    bool.add(new TermQuery(new Term(propertyName, pr.not.getValue(STRING))), MUST_NOT);
                    return bool;
                }
            }
        }
        throw new IllegalStateException("PropertyRestriction not handled " + pr + " for index " + defn);
    }

    static long getVersion(IndexSearcher indexSearcher) {
        IndexReader reader = indexSearcher.getIndexReader();
        if (reader instanceof DirectoryReader) {
            return ((DirectoryReader) reader).getVersion();
        }
        return -1;
    }

    private static Query createNodeNameQuery(PropertyRestriction pr) {
        String first = pr.first != null ? pr.first.getValue(STRING) : null;
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                && pr.lastIncluding) {
            // [property]=[value]
            return new TermQuery(new Term(FieldNames.NODE_NAME, first));
        }

        if (pr.isLike) {
            return createLikeQuery(FieldNames.NODE_NAME, first);
        }

        throw new IllegalStateException("For nodeName queries only EQUALS and LIKE are supported " + pr);
    }

    private static void addReferenceConstraint(String uuid, List<Query> qs,
                                               IndexReader reader) {
        if (reader == null) {
            // getPlan call
            qs.add(new TermQuery(new Term("*", uuid)));
            return;
        }

        // reference query
        BooleanQuery bq = new BooleanQuery();
        Collection<String> fields = MultiFields.getIndexedFields(reader);
        for (String f : fields) {
            bq.add(new TermQuery(new Term(f, uuid)), SHOULD);
        }
        qs.add(bq);
    }

    private static void addNodeTypeConstraints(IndexingRule defn, List<Query> qs, Filter filter) {
        BooleanQuery bq = new BooleanQuery();
        PropertyDefinition primaryType = defn.getConfig(JCR_PRIMARYTYPE);
        //TODO OAK-2198 Add proper nodeType query support

        if (primaryType != null && primaryType.propertyIndex) {
            for (String type : filter.getPrimaryTypes()) {
                bq.add(new TermQuery(new Term(JCR_PRIMARYTYPE, type)), SHOULD);
            }
        }

        PropertyDefinition mixinType = defn.getConfig(JCR_MIXINTYPES);
        if (mixinType != null && mixinType.propertyIndex) {
            for (String type : filter.getMixinTypes()) {
                bq.add(new TermQuery(new Term(JCR_MIXINTYPES, type)), SHOULD);
            }
        }

        if (bq.clauses().size() != 0) {
            qs.add(bq);
        }
    }

    static Query getFullTextQuery(final IndexPlan plan, FullTextExpression ft,
                                  final Analyzer analyzer, final FulltextQueryTermsProvider augmentor) {
        final PlanResult pr = getPlanResult(plan);
        // a reference to the query, so it can be set in the visitor
        // (a "non-local return")
        final AtomicReference<Query> result = new AtomicReference<>();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextContains contains) {
                visitTerm(contains.getPropertyName(), contains.getRawText(), null, contains.isNot());
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                BooleanQuery q = new BooleanQuery();
                for (FullTextExpression e : or.list) {
                    Query x = getFullTextQuery(plan, e, analyzer, augmentor);
                    q.add(x, SHOULD);
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BooleanQuery q = new BooleanQuery();
                for (FullTextExpression e : and.list) {
                    Query x = getFullTextQuery(plan, e, analyzer, augmentor);
                    /* Only unwrap the clause if MUST_NOT(x) */
                    boolean hasMustNot = false;
                    if (x instanceof BooleanQuery) {
                        BooleanQuery bq = (BooleanQuery) x;
                        if ((bq.getClauses().length == 1) &&
                                (bq.getClauses()[0].getOccur() == BooleanClause.Occur.MUST_NOT)) {
                            hasMustNot = true;
                            q.add(bq.getClauses()[0]);
                        }
                    }

                    if (!hasMustNot) {
                        q.add(x, MUST);
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
                Query q = tokenToQuery(text, p, pr, analyzer, augmentor);
                if (q == null) {
                    return false;
                }
                if (boost != null) {
                    q.setBoost(Float.parseFloat(boost));
                }
                if (not) {
                    BooleanQuery bq = new BooleanQuery();
                    bq.add(q, MUST_NOT);
                    result.set(bq);
                } else {
                    result.set(q);
                }
                return true;
            }
        });
        return result.get();
    }

    static String getLuceneFieldName(@Nullable String p, PlanResult pr) {
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

    private static Query tokenToQuery(String text, String fieldName, PlanResult pr, Analyzer analyzer,
                                      FulltextQueryTermsProvider augmentor) {
        Query ret;
        IndexingRule indexingRule = pr.indexingRule;
        //Expand the query on fulltext field
        if (FieldNames.FULLTEXT.equals(fieldName) &&
                !indexingRule.getNodeScopeAnalyzedProps().isEmpty()) {
            BooleanQuery in = new BooleanQuery();
            for (PropertyDefinition pd : indexingRule.getNodeScopeAnalyzedProps()) {
                Query q = tokenToQuery(text, FieldNames.createAnalyzedFieldName(pd.name), analyzer);
                q.setBoost(pd.boost);
                in.add(q, BooleanClause.Occur.SHOULD);
            }

            //Add the query for actual fulltext field also. That query would
            //not be boosted
            in.add(tokenToQuery(text, fieldName, analyzer), BooleanClause.Occur.SHOULD);
            ret = in;
        } else {
            ret = tokenToQuery(text, fieldName, analyzer);
        }

        //Augment query terms if available (as a 'SHOULD' clause)
        if (FieldNames.FULLTEXT.equals(fieldName)) {
            Query subQuery = new BooleanQuery();
            if (pr.indexDefinition.isDynamicBoostLiteEnabled()) {
                subQuery = tokenToQuery(text, FieldNames.SIMILARITY_TAGS, analyzer);
                // De-boosting dynamic boost based query so other clauses will have more relevance
                subQuery.setBoost(DYNAMIC_BOOST_WEIGHT);
            } else if (augmentor != null) {
                subQuery = augmentor.getQueryTerm(text, analyzer, pr.indexDefinition.getDefinitionNodeState());
            }

            if (subQuery != null) {
                BooleanQuery query = new BooleanQuery();

                query.add(ret, BooleanClause.Occur.SHOULD);
                query.add(subQuery, BooleanClause.Occur.SHOULD);

                ret = query;
            }
        }

        return ret;
    }

    static Query tokenToQuery(String text, String fieldName, Analyzer analyzer) {
        if (analyzer == null) {
            return null;
        }
        StandardQueryParser parserHelper = new StandardQueryParser(analyzer);
        parserHelper.setAllowLeadingWildcard(true);
        parserHelper.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);

        text = rewriteQueryText(text);

        try {
            return parserHelper.parse(text, fieldName);
        } catch (QueryNodeException e) {
            throw new RuntimeException(e);
        }
    }

    private static Query newDepthQuery(String path, PlanResult planResult) {
        int depth = PathUtils.getDepth(path) + planResult.getParentDepth() + 1;
        return NumericRangeQuery.newIntRange(FieldNames.PATH_DEPTH, depth, depth, true, true);
    }

    @SuppressWarnings("Guava")
    private static Iterator<FulltextResultRow> mergePropertyIndexResult(IndexPlan plan, NodeState rootState,
                                                                        Iterator<FulltextResultRow> itr) {
        PlanResult pr = getPlanResult(plan);
        HybridPropertyIndexLookup lookup = new HybridPropertyIndexLookup(pr.indexPath,
                NodeStateUtils.getNode(rootState, pr.indexPath), plan.getPathPrefix(), false);
        PropertyIndexResult pir = pr.getPropertyIndexResult();

        FluentIterable<String> paths;
        if (pir != null) {
            Iterable<String> queryResult = lookup.query(plan.getFilter(), pir.propertyName, pir.pr);
            paths = FluentIterable.from(queryResult)
                    .transform(path -> pr.isPathTransformed() ? pr.transformPath(path) : path)
                    .filter(x -> x != null);
        } else {
            Validate.checkState(pr.evaluateSyncNodeTypeRestriction()); //Either of property or nodetype should not be null
            Filter filter = plan.getFilter();
            paths = FluentIterable.from(Iterables.concat(
                    lookup.query(filter, JCR_PRIMARYTYPE, newName(filter.getPrimaryTypes())),
                    lookup.query(filter, JCR_MIXINTYPES, newName(filter.getMixinTypes()))));
        }

        //No need for path restriction evaluation as thats taken care by PropertyIndex impl itself
        //via content mirror strategy
        FluentIterable<FulltextResultRow> propIndex = paths
                .transform(path -> new FulltextResultRow(path, 0, null, null, null));

        //Property index itr should come first
        return Iterators.concat(propIndex.iterator(), itr);
    }

    class DelayedLuceneFacetProvider implements FacetProvider {
        private final LucenePropertyIndex index;
        private final Query query;
        private final IndexPlan plan;
        private final SecureFacetConfiguration config;
        private final Map<String, List<Facet>> cachedResults = new HashMap<>();

        DelayedLuceneFacetProvider(LucenePropertyIndex index, Query query, IndexPlan plan, SecureFacetConfiguration config) {
            this.index = index;
            this.query = query;
            this.plan = plan;
            this.config = config;
        }

        @Override
        public List<Facet> getFacets(int numberOfFacets, String columnName) throws IOException {
            if (!CACHE_FACET_RESULTS) {
                LOG.trace("{} = {} getting uncached results for columnName = {}", CACHE_FACET_RESULTS_NAME, CACHE_FACET_RESULTS, columnName);
                return getFacetsUncached(numberOfFacets, columnName);
            }
            String cacheKey = columnName + "/" + numberOfFacets;
            if (cachedResults.containsKey(cacheKey)) {
                LOG.trace("columnName = {} returning Facet Data from cache.", columnName);
                return cachedResults.get(cacheKey);
            }
            LOG.trace("columnName = {} facet Data not present in cache...", columnName);
            if (EAGER_FACET_CACHE_FILL) {
                fillFacetCache(numberOfFacets);
                if (cachedResults.containsKey(cacheKey)) {
                    LOG.trace("columnName = {} now found", cacheKey);
                    return cachedResults.get(cacheKey);
                }
                LOG.warn("Facet data for {} not found: read using query", cacheKey);
            }
            List<Facet> result = getFacetsUncached(numberOfFacets, columnName);
            cachedResults.put(cacheKey, result);
            return result;
        }

        private List<Facet> fillFacetCache(int numberOfFacets) throws IOException {
            List<Facet> result = null;
            LuceneIndexNode indexNode = index.acquireIndexNode(plan);
            try {
                IndexSearcher searcher = indexNode.getSearcher();
                Facets facets = FacetHelper.getFacets(searcher, query, plan, config);
                if (facets != null) {
                    List<String> allColumnNames = FacetHelper.getFacetColumnNamesFromPlan(plan);
                    for (String column : allColumnNames) {
                        result = getFacetsUncached(facets, numberOfFacets, column);
                        String cc = column + "/" + numberOfFacets;
                        cachedResults.put(cc, result);
                    }
                }
            } finally {
                indexNode.release();
            }
            return result;
        }

        private List<Facet> getFacetsUncached(int numberOfFacets, String columnName) throws IOException {
            LuceneIndexNode indexNode = index.acquireIndexNode(plan);
            try {
                IndexSearcher searcher = indexNode.getSearcher();
                String facetFieldName = FulltextIndex.parseFacetField(columnName);
                Facets facets = FacetHelper.getFacets(searcher, query, plan, config);
                if (facets != null) {
                    try {
                        ImmutableList.Builder<Facet> res = new ImmutableList.Builder<>();
                        FacetResult topChildren = facets.getTopChildren(numberOfFacets, facetFieldName);
                        if (topChildren != null) {
                            for (LabelAndValue lav : topChildren.labelValues) {
                                res.add(new Facet(
                                        lav.label, lav.value.intValue()
                                ));
                            }
                            return res.build();
                        }
                    } catch (IllegalArgumentException iae) {
                        LOG.debug(iae.getMessage(), iae);
                        LOG.warn("facets for {} not yet indexed: " + iae, facetFieldName);
                    }
                }
                return null;
            } finally {
                indexNode.release();
            }
        }

        private List<Facet> getFacetsUncached(Facets facets, int numberOfFacets, String columnName) throws IOException {
            String facetFieldName = FulltextIndex.parseFacetField(columnName);
            try {
                ImmutableList.Builder<Facet> res = new ImmutableList.Builder<>();
                FacetResult topChildren = facets.getTopChildren(numberOfFacets, facetFieldName);
                if (topChildren == null) {
                    return null;
                }
                for (LabelAndValue lav : topChildren.labelValues) {
                    res.add(new Facet(
                            lav.label, lav.value.intValue()
                    ));
                }
                return res.build();
            } catch (IllegalArgumentException iae) {
                LOG.debug(iae.getMessage(), iae);
                LOG.warn("facets for {} not yet indexed: {}", facetFieldName, iae);
                return null;
            }
        }

    }

    static class LuceneFacetProvider implements FacetProvider {

        private final Facets facets;

        LuceneFacetProvider(Facets facets) {
            this.facets = facets;
        }

        @Override
        public List<Facet> getFacets(int numberOfFacets, String columnName) throws IOException {
            String facetFieldName = FulltextIndex.parseFacetField(columnName);

            if (facets != null) {
                try {
                    ImmutableList.Builder<Facet> res = new ImmutableList.Builder<>();
                    FacetResult topChildren = facets.getTopChildren(numberOfFacets, facetFieldName);
                    if (topChildren != null) {
                        for (LabelAndValue lav : topChildren.labelValues) {
                            res.add(new Facet(
                                lav.label, lav.value.intValue()
                            ));
                        }
                        return res.build();
                    }
                } catch (IllegalArgumentException iae) {
                    LOG.debug(iae.getMessage(), iae);
                    LOG.warn("facets for {} not yet indexed: " + iae, facetFieldName);
                }
            }

            return null;
        }
    }

    /**
     * A index node implementation that acquires the underlying index only if
     * actually needed. This is to avoid downloading the index for the planning
     * phase, if there is no chance that the index is actually used.
     */
    static class LazyLuceneIndexNode implements LuceneIndexNode {
        private AtomicBoolean released = new AtomicBoolean();
        private IndexTracker tracker;
        private String indexPath;
        private volatile LuceneIndexNode indexNode;

        LazyLuceneIndexNode(IndexTracker tracker, String indexPath) {
            this.tracker = tracker;
            this.indexPath = indexPath;
        }

        @Override
        public void release() {
            if (released.getAndSet(true)) {
                // already released
                return;
            }
            if (indexNode != null) {
                indexNode.release();
            }
            // to ensure it is not used after releasing
            indexNode = null;
            tracker = null;
            indexPath = null;
        }

        private void checkNotReleased() {
            if (released.get()) {
                throw new IllegalStateException("Already released");
            }
        }

        @Override
        public LuceneIndexDefinition getDefinition() {
            checkNotReleased();
            return tracker.getIndexDefinition(indexPath);
        }

        private LuceneIndexNode getIndexNode() {
            LuceneIndexNode n = findIndexNode();
            if (n == null) {
                String message = "No index node, corrupt index? " + indexPath;
                LOG.warn(message);
                throw new IllegalStateException(message);
            }
            return n;
        }

        @Nullable
        private LuceneIndexNode findIndexNode() {
            checkNotReleased();
            LuceneIndexNode n = indexNode;
            // double checked locking implemented in the correct way for Java 5
            // and newer (actually I don't think this is ever called
            // concurrently right now, but better be save)
            if (n == null) {
                synchronized (this) {
                    n = indexNode;
                    if (n == null) {
                        n = indexNode = tracker.acquireIndexNode(indexPath);
                    }
                }
            }
            return n;
        }

        @Override
        public int getIndexNodeId() {
            return getIndexNode().getIndexNodeId();
        }

        @Nullable
        @Override
        public LuceneIndexStatistics getIndexStatistics() {
            LuceneIndexNode n = findIndexNode();
            if (n == null) {
                return null;
            }
            return n.getIndexStatistics();
        }

        @Override
        public IndexSearcher getSearcher() {
            return getIndexNode().getSearcher();
        }

        @Override
        public List<LuceneIndexReader> getPrimaryReaders() {
            return getIndexNode().getPrimaryReaders();
        }

        @Override
        public @Nullable Directory getSuggestDirectory() {
            return getIndexNode().getSuggestDirectory();
        }

        @Override
        public List<LuceneIndexReader> getNRTReaders() {
            return getIndexNode().getNRTReaders();
        }

        @Override
        public @Nullable AnalyzingInfixSuggester getLookup() {
            return getIndexNode().getLookup();
        }

        @Override
        public @Nullable LuceneIndexWriter getLocalWriter() throws IOException {
            return getIndexNode().getLocalWriter();
        }

        @Override
        public void refreshReadersOnWriteIfRequired() {
            getIndexNode().refreshReadersOnWriteIfRequired();
        }

    }

    static abstract class LuceneResultRowIterator extends AbstractIterator<FulltextResultRow> implements IteratorRewoundStateProvider {
    }

}
