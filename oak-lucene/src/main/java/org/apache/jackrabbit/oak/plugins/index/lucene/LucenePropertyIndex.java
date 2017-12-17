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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.primitives.Chars;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexPlanner.PropertyIndexResult;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.MoreLikeThisHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.PathStoredFieldVisitor;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SpellcheckHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.plugins.index.Cursors;
import org.apache.jackrabbit.oak.plugins.index.Cursors.PathCursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvanceFulltextQueryIndex;
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
import org.apache.lucene.queries.CustomScoreQuery;
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
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.ANALYZED_FIELD_PREFIX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.FULLTEXT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.NATIVE_SORT_ORDER;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newAncestorTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newName;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_PATH;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.NativeQueryIndex;
import static org.apache.lucene.search.BooleanClause.Occur.*;

/**
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
 * <p>
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
public class LucenePropertyIndex implements AdvancedQueryIndex, QueryIndex, NativeQueryIndex,
        AdvanceFulltextQueryIndex {

    private static double MIN_COST = 2.1;

    private static final Logger LOG = LoggerFactory
            .getLogger(LucenePropertyIndex.class);
    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(LucenePropertyIndex.class.getName() + ".perf"));

    static final String ATTR_PLAN_RESULT = "oak.lucene.planResult";

    /**
     * Batch size for fetching results from Lucene queries.
     */
    public static final int LUCENE_QUERY_BATCH_SIZE = 50;

    protected final IndexTracker tracker;

    private final ScorerProviderFactory scorerProviderFactory;

    private final Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter("<strong>", "</strong>"),
            new SimpleHTMLEncoder(), null);

    private final PostingsHighlighter postingsHighlighter = new PostingsHighlighter();

    private final IndexAugmentorFactory augmentorFactory;

    public LucenePropertyIndex(IndexTracker tracker) {
        this(tracker, ScorerProviderFactory.DEFAULT);
    }

    public LucenePropertyIndex(IndexTracker tracker, ScorerProviderFactory factory) {
        this(tracker, factory, null);
    }

    public LucenePropertyIndex(IndexTracker tracker, ScorerProviderFactory factory, IndexAugmentorFactory augmentorFactory) {
        this.tracker = tracker;
        this.scorerProviderFactory = factory;
        this.augmentorFactory = augmentorFactory;
    }

    @Override
    public double getMinimumCost() {
        return MIN_COST;
    }

    @Override
    public String getIndexName() {
        return "lucene-property";
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
        Collection<String> indexPaths = new LuceneIndexLookup(rootState).collectIndexNodePaths(filter);
        List<IndexPlan> plans = Lists.newArrayListWithCapacity(indexPaths.size());
        for (String path : indexPaths) {
            IndexNode indexNode = null;
            try {
                indexNode = tracker.acquireIndexNode(path);

                if (indexNode != null) {
                    IndexPlan plan = new IndexPlanner(indexNode, path, filter, sortOrder).getPlan();
                    if (plan != null) {
                        plans.add(plan);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error getting plan for {}", path);
                LOG.error("Exception:", e);
            } finally {
                if (indexNode != null) {
                    indexNode.release();
                }
            }
        }
        return plans;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    @Override
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        Filter filter = plan.getFilter();
        IndexNode index = tracker.acquireIndexNode(getPlanResult(plan).indexPath);
        checkState(index != null, "The Lucene index is not available");
        try {
            FullTextExpression ft = filter.getFullTextConstraint();
            StringBuilder sb = new StringBuilder("lucene:");
            String path = getPlanResult(plan).indexPath;
            sb.append(getIndexName(plan))
                    .append("(")
                    .append(path)
                    .append(") ");
            sb.append(getLuceneRequest(plan, augmentorFactory, null));
            if (plan.getSortOrder() != null && !plan.getSortOrder().isEmpty()) {
                sb.append(" ordering:").append(plan.getSortOrder());
            }
            if (ft != null) {
                sb.append(" ft:(").append(ft).append(")");
            }
            addSyncIndexPlan(plan, sb);
            return sb.toString();
        } finally {
            index.release();
        }
    }

    private static void addSyncIndexPlan(IndexPlan plan, StringBuilder sb) {
        PlanResult pr = getPlanResult(plan);
        if (pr.hasPropertyIndexResult()) {
            PropertyIndexResult pres = pr.getPropertyIndexResult();
            sb.append(" sync:(")
              .append(pres.propertyName);

            if (!pres.propertyName.equals(pres.pr.propertyName)) {
               sb.append("[").append(pres.pr.propertyName).append("]");
            }

            sb.append(" ").append(pres.pr);
            sb.append(")");
        }

        if (pr.evaluateSyncNodeTypeRestriction()) {
            sb.append(" sync:(nodeType");
            sb.append(" primaryTypes : ").append(plan.getFilter().getPrimaryTypes());
            sb.append(" mixinTypes : ").append(plan.getFilter().getMixinTypes());
            sb.append(")");
        }
    }

    @Override
    public Cursor query(final Filter filter, final NodeState root) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    @Override
    public Cursor query(final IndexPlan plan, NodeState rootState) {
        final Filter filter = plan.getFilter();
        final Sort sort = getSort(plan);
        final PlanResult pr = getPlanResult(plan);
        QueryLimits settings = filter.getQueryLimits();
        Iterator<LuceneResultRow> itr = new AbstractIterator<LuceneResultRow>() {
            private final Deque<LuceneResultRow> queue = Queues.newArrayDeque();
            private final Set<String> seenPaths = Sets.newHashSet();
            private ScoreDoc lastDoc;
            private int nextBatchSize = LUCENE_QUERY_BATCH_SIZE;
            private boolean noDocs = false;
            private IndexSearcher indexSearcher;
            private int indexNodeId = -1;

            @Override
            protected LuceneResultRow computeNext() {
                while (!queue.isEmpty() || loadDocs()) {
                    return queue.remove();
                }
                releaseSearcher();
                return endOfData();
            }

            private LuceneResultRow convertToRow(ScoreDoc doc, IndexSearcher searcher, String excerpt,  Facets facets,
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
                    return shouldIncludeForHierarchy? new LuceneResultRow(path, doc.score, excerpt, facets, explanation)
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

                final IndexNode indexNode = acquireIndexNode(plan);
                checkState(indexNode != null);
                try {
                    IndexSearcher searcher = getCurrentSearcher(indexNode);
                    LuceneRequestFacade luceneRequestFacade = getLuceneRequest(plan, augmentorFactory, searcher.getIndexReader());
                    if (luceneRequestFacade.getLuceneRequest() instanceof Query) {
                        Query query = (Query) luceneRequestFacade.getLuceneRequest();

                        CustomScoreQuery customScoreQuery = getCustomScoreQuery(plan, query);

                        if (customScoreQuery != null) {
                            query = customScoreQuery;
                        }

                        TopDocs docs;
                        long start = PERF_LOGGER.start();
                        while (true) {
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

                            long f = PERF_LOGGER.start();
                            Facets facets = FacetHelper.getFacets(searcher, query, docs, plan, indexNode.getDefinition().isSecureFacets());
                            PERF_LOGGER.end(f, -1, "facets retrieved");

                            PropertyRestriction restriction = filter.getPropertyRestriction(QueryConstants.REP_EXCERPT);
                            boolean addExcerpt = restriction != null && restriction.isNotNullRestriction();

                            restriction = filter.getPropertyRestriction(QueryConstants.OAK_SCORE_EXPLANATION);
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

                            for (ScoreDoc doc : docs.scoreDocs) {
                                String excerpt = null;
                                if (addExcerpt) {
                                    excerpt = getExcerpt(query, analyzer, searcher, doc, mergedFieldInfos);
                                }

                                String explanation = null;
                                if (addExplain) {
                                    explanation = searcher.explain(query, doc.doc).toString();
                                }

                                LuceneResultRow row = convertToRow(doc, searcher, excerpt, facets, explanation);
                                if (row != null) {
                                    queue.add(row);
                                }
                                lastDocToRecord = doc;
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
                                        queue.add(new LuceneResultRow(suggestion.string));
                                        break;
                                    }
                                }
                            }
                        }

                    } else if (luceneRequestFacade.getLuceneRequest() instanceof SuggestHelper.SuggestQuery) {
                        SuggestHelper.SuggestQuery suggestQuery = (SuggestHelper.SuggestQuery) luceneRequestFacade.getLuceneRequest();
                        noDocs = true;

                        List<Lookup.LookupResult> lookupResults = SuggestHelper.getSuggestions(indexNode.getLookup(), suggestQuery);

                        QueryParser qp =  new QueryParser(Version.LUCENE_47, FieldNames.SUGGEST,
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
                                        queue.add(new LuceneResultRow(suggestion.key.toString(), suggestion.value));
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("query via {} failed.", LucenePropertyIndex.this, e);
                } finally {
                    indexNode.release();
                }

                if (lastDocToRecord != null) {
                    this.lastDoc = lastDocToRecord;
                }

                return !queue.isEmpty();
            }

            private IndexSearcher getCurrentSearcher(IndexNode indexNode) {
                //The searcher once obtained is held till either cursor is finished
                //or if the index gets updated. It needs to be ensured that
                //searcher is obtained via this method only in this iterator

                //Refresh the searcher if change in indexNode is detected
                //For NRT case its fine to keep a reference to searcher i.e. not
                //acquire it for every loadDocs call otherwise with frequent change
                //the reset of lastDoc would happen very frequently.
                //Upon IndexNode change i.e. when new async index update is detected
                //the searcher would be refreshed as done earlier
                if (indexNodeId != indexNode.getIndexNodeId()){
                    //if already initialized then log about change
                    if (indexNodeId > 0){
                        LOG.debug("Change in index version detected. Query would be performed without offset");
                    }

                    indexSearcher = indexNode.getSearcher();
                    indexNodeId = indexNode.getIndexNodeId();
                    lastDoc = null;
                }
                return indexSearcher;
            }

            private void releaseSearcher() {
                //For now nullifying it.
                indexSearcher =  null;
            }
        };
        SizeEstimator sizeEstimator = new SizeEstimator() {
            @Override
            public long getSize() {
                IndexNode indexNode = acquireIndexNode(plan);
                checkState(indexNode != null);
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
            }
        };

        if (pr.hasPropertyIndexResult() || pr.evaluateSyncNodeTypeRestriction()) {
            itr = mergePropertyIndexResult(plan, rootState, itr);
        }

        return new LucenePathCursor(itr, plan, settings, sizeEstimator);
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

    private static boolean shouldInclude(String docPath, IndexPlan plan) {
        String path = getPathRestriction(plan);

        boolean include = true;

        Filter filter = plan.getFilter();
        switch (filter.getPathRestriction()) {
            case EXACT:
                include = path.equals(docPath);
                break;
            case DIRECT_CHILDREN:
                include = PathUtils.getParentPath(docPath).equals(path);
                break;
            case ALL_CHILDREN:
                include = PathUtils.isAncestor(path, docPath);
                break;
        }

        return include;
    }

    private String getExcerpt(Query query, Analyzer analyzer, IndexSearcher searcher, ScoreDoc doc,
                              FieldInfos fieldInfos) throws IOException {
        StringBuilder excerpt = new StringBuilder();
        int docID = doc.doc;
        List<String> names = new LinkedList<String>();

        for (IndexableField field : searcher.getIndexReader().document(docID).getFields()) {
            String name = field.name();
            // postings highlighter can be used on analyzed fields with docs, freqs, positions and offsets stored.
            if ((name.startsWith(ANALYZED_FIELD_PREFIX) || name.equals(FULLTEXT)) && fieldInfos.hasProx() && fieldInfos.hasOffsets()) {
                names.add(name);
            }
        }

        if (names.size() > 0) {
            int[] maxPassages = new int[names.size()];
            for (int i = 0; i < maxPassages.length; i++) {
                maxPassages[i] = 1;
            }
            try {
                Map<String, String[]> stringMap = postingsHighlighter.highlightFields(names.toArray(new String[names.size()]),
                        query, searcher, new int[]{docID}, maxPassages);
                for (Map.Entry<String, String[]> entry : stringMap.entrySet()) {
                    String value = Arrays.toString(entry.getValue());
                    if (value.contains("<b>")) {
                        if (excerpt.length() > 0) {
                            excerpt.append("...");
                        }
                        excerpt.append(value);
                    }
                }
            } catch (Exception e) {
                LOG.error("postings highlighting failed", e);
            }
        }

        // fallback if no excerpt could be retrieved using postings highlighter
        if (excerpt.length() == 0) {

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
                                if (excerpt.length() > 0) {
                                    excerpt.append("...");
                                }
                                excerpt.append(fragment.toString());
                            }
                            break;
                        }
                    } catch (InvalidTokenOffsetsException e) {
                        LOG.error("higlighting failed", e);
                    }
                }
            }
        }
        return excerpt.toString();
    }

    @Override
    public NodeAggregator getNodeAggregator() {
        return null;
    }

    /**
     * In a fulltext term for jcr:contains(foo, 'bar') 'foo'
     * is the property name. While in jcr:contains(foo/*, 'bar')
     * 'foo' is node name
     *
     * @return true if the term is related to node
     */
    public static boolean isNodePath(String fulltextTermPath) {
        return fulltextTermPath.endsWith("/*");
    }

    private IndexNode acquireIndexNode(IndexPlan plan) {
        return tracker.acquireIndexNode(getPlanResult(plan).indexPath);
    }

    private static Sort getSort(IndexPlan plan) {
        List<OrderEntry> sortOrder = plan.getSortOrder();
        if (sortOrder == null || sortOrder.isEmpty()) {
            return null;
        }

        sortOrder = removeNativeSort(sortOrder);
        List<SortField> fieldsList = newArrayListWithCapacity(sortOrder.size());
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
        ArrayList<OrderEntry> result = new ArrayList<OrderEntry>();
        for(OrderEntry oe : original) {
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
        checkState(t != null, "Type cannot be null");
        checkState(!t.isArray(), "Array types are not supported");

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

    private static String getIndexName(IndexPlan plan) {
        return PathUtils.getName(getPlanResult(plan).indexPath);
    }

    /**
     * Get the Lucene query for the given filter.
     *
     * @param plan index plan containing filter details
     * @param reader the Lucene reader
     * @return the Lucene query
     */
    private static LuceneRequestFacade getLuceneRequest(IndexPlan plan, IndexAugmentorFactory augmentorFactory, IndexReader reader) {
        FulltextQueryTermsProvider augmentor = getIndexAgumentor(plan, augmentorFactory);
        List<Query> qs = new ArrayList<Query>();
        Filter filter = plan.getFilter();
        FullTextExpression ft = filter.getFullTextConstraint();
        PlanResult planResult = getPlanResult(plan);
        IndexDefinition defn = planResult.indexDefinition;
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
                    Query moreLikeThis = MoreLikeThisHelper.getMoreLikeThis(reader, analyzer, mltQueryString);
                    if (moreLikeThis != null) {
                        qs.add(moreLikeThis);
                    }
                }
            } else if (query.startsWith("spellcheck?")) {
                String spellcheckQueryString = query.replace("spellcheck?", "");
                if (reader != null) {
                    return new LuceneRequestFacade<SpellcheckHelper.SpellcheckQuery>(SpellcheckHelper.getSpellcheckQuery(spellcheckQueryString, reader));
                }
            } else if (query.startsWith("suggest?")) {
                String suggestQueryString = query.replace("suggest?", "");
                if (reader != null) {
                    return new LuceneRequestFacade<SuggestHelper.SuggestQuery>(SuggestHelper.getSuggestQuery(suggestQueryString));
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
     * @return
     */
    @Nonnull
    public static LuceneRequestFacade<Query> performAdditionalWraps(@Nonnull List<Query> qs) {
        checkNotNull(qs);
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
                    ibq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
                }
            }
            return new LuceneRequestFacade<Query>(qs.get(0));
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
        return new LuceneRequestFacade<Query>(bq);
    }

    /**
     * unwraps any NOT clauses from the provided boolean query into another boolean query.
     *
     * @param input the query to be analysed for the existence of NOT clauses. Cannot be null.
     * @param output the query where the unwrapped NOTs will be saved into. Cannot be null.
     * @return true if there where at least one unwrapped NOT. false otherwise.
     */
    private static boolean unwrapMustNot(@Nonnull BooleanQuery input, @Nonnull BooleanQuery output) {
        checkNotNull(input);
        checkNotNull(output);
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

    private CustomScoreQuery getCustomScoreQuery(IndexPlan plan, Query subQuery) {
        PlanResult planResult = getPlanResult(plan);
        IndexDefinition idxDef = planResult.indexDefinition;
        String providerName = idxDef.getScorerProviderName();
        if (scorerProviderFactory != null && providerName != null) {
            return scorerProviderFactory.getScorerProvider(providerName)
                    .createCustomScoreQuery(subQuery);
        }
        return null;
    }
    private static FulltextQueryTermsProvider getIndexAgumentor(IndexPlan plan, IndexAugmentorFactory augmentorFactory) {
        PlanResult planResult = getPlanResult(plan);

        if (augmentorFactory != null){
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
                    bq.add(new BooleanClause(newDepthQuery(path), BooleanClause.Occur.MUST));
                    qs.add(bq);
                }
                break;
            case EXACT:
                qs.add(new TermQuery(newPathTerm(path)));
                break;
            case PARENT:
                if (denotesRoot(path)) {
                    // there's no parent of the root node
                    // we add a path that can not possibly occur because there
                    // is no way to say "match no documents" in Lucene
                    qs.add(new TermQuery(new Term(FieldNames.PATH, "///")));
                } else {
                    qs.add(new TermQuery(newPathTerm(getParentPath(path))));
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

    private static int determinePropertyType(PropertyDefinition defn, PropertyRestriction pr) {
        int typeFromRestriction = pr.propertyType;
        if (typeFromRestriction == PropertyType.UNDEFINED) {
            //If no explicit type defined then determine the type from restriction
            //value
            if (pr.first != null && pr.first.getType() != Type.UNDEFINED) {
                typeFromRestriction = pr.first.getType().tag();
            } else if (pr.last != null && pr.last.getType() != Type.UNDEFINED) {
                typeFromRestriction = pr.last.getType().tag();
            } else if (pr.list != null && !pr.list.isEmpty()) {
                typeFromRestriction = pr.list.get(0).getType().tag();
            }
        }
        return getPropertyType(defn, pr.propertyName, typeFromRestriction);
    }

    private static int getPropertyType(PropertyDefinition defn, String name, int defaultVal) {
        if (defn.isTypeDefined()) {
            return defn.getType();
        }
        return defaultVal;
    }

    private static PlanResult getPlanResult(IndexPlan plan) {
        return (PlanResult) plan.getAttribute(ATTR_PLAN_RESULT);
    }

    private static Query createLikeQuery(String name, String first) {
        first = first.replace('%', WildcardQuery.WILDCARD_STRING);
        first = first.replace('_', WildcardQuery.WILDCARD_CHAR);

        int indexOfWS = first.indexOf(WildcardQuery.WILDCARD_STRING);
        int indexOfWC = first.indexOf(WildcardQuery.WILDCARD_CHAR);
        int len = first.length();

        if (indexOfWS == len || indexOfWC == len) {
            // remove trailing "*" for prefixquery
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

    @CheckForNull
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
                    // not null. For date lower bound of zero can be used
                    return NumericRangeQuery.newLongRange(propertyName, 0L, Long.MAX_VALUE, true, true);
                }

                break;
            }
            case PropertyType.DOUBLE: {
                Double first = pr.first != null ? pr.first.getValue(Type.DOUBLE) : null;
                Double last = pr.last != null ? pr.last.getValue(Type.DOUBLE) : null;
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
                        Double doubleVal = value.getValue(Type.DOUBLE);
                        in.add(NumericRangeQuery.newDoubleRange(propertyName, doubleVal, doubleVal, true, true), BooleanClause.Occur.SHOULD);
                    }
                    return in;
                } else if (pr.isNotNullRestriction()) {
                    // not null.
                    return NumericRangeQuery.newDoubleRange(propertyName, Double.MIN_VALUE, Double.MAX_VALUE, true, true);
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
        final AtomicReference<Query> result = new AtomicReference<Query>();
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
        if (augmentor != null && FieldNames.FULLTEXT.equals(fieldName)) {
            Query subQuery = augmentor.getQueryTerm(text, analyzer, pr.indexDefinition.getDefinitionNodeState());
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

    /**
     * Following chars are used as operators in Lucene Query and should be escaped
     */
    private static final char[] LUCENE_QUERY_OPERATORS = {':' , '/', '!', '&', '|', '='};

    /**
     * Following logic is taken from org.apache.jackrabbit.core.query.lucene.JackrabbitQueryParser#parse(java.lang.String)
     */
    static String rewriteQueryText(String textsearch) {
        // replace escaped ' with just '
        StringBuilder rewritten = new StringBuilder();
        // the default lucene query parser recognizes 'AND' and 'NOT' as
        // keywords.
        textsearch = textsearch.replaceAll("AND", "and");
        textsearch = textsearch.replaceAll("NOT", "not");
        boolean escaped = false;
        for (int i = 0; i < textsearch.length(); i++) {
            char c = textsearch.charAt(i);
            if (c == '\\') {
                if (escaped) {
                    rewritten.append("\\\\");
                    escaped = false;
                } else {
                    escaped = true;
                }
            } else if (c == '\'') {
                if (escaped) {
                    escaped = false;
                }
                rewritten.append(c);
            } else if (Chars.contains(LUCENE_QUERY_OPERATORS, c)) {
                rewritten.append('\\').append(c);
            } else {
                if (escaped) {
                    rewritten.append('\\');
                    escaped = false;
                }
                rewritten.append(c);
            }
        }
        return rewritten.toString();
    }

    private static String getPathRestriction(IndexPlan plan) {
        Filter f = plan.getFilter();
        String pathPrefix = plan.getPathPrefix();
        if (pathPrefix.isEmpty()) {
            return f.getPath();
        }
        String relativePath = PathUtils.relativize(pathPrefix, f.getPath());
        return "/" + relativePath;
    }

    private static Query newDepthQuery(String path) {
        int depth = PathUtils.getDepth(path) + 1;
        return NumericRangeQuery.newIntRange(FieldNames.PATH_DEPTH, depth, depth, true, true);
    }

    @SuppressWarnings("Guava")
    private static Iterator<LuceneResultRow> mergePropertyIndexResult(IndexPlan plan, NodeState rootState,
                                                                      Iterator<LuceneResultRow> itr) {
        PlanResult pr = getPlanResult(plan);
        HybridPropertyIndexLookup lookup = new HybridPropertyIndexLookup(pr.indexPath,
                NodeStateUtils.getNode(rootState, pr.indexPath), plan.getPathPrefix(), false);
        PropertyIndexResult pir = pr.getPropertyIndexResult();

        FluentIterable<String> paths = null;
        if (pir != null) {
            Iterable<String> queryResult = lookup.query(plan.getFilter(), pir.propertyName, pir.pr);
            paths = FluentIterable.from(queryResult)
                    .transform(path -> pr.isPathTransformed() ? pr.transformPath(path) : path)
                    .filter(notNull());
        } else {
            checkState(pr.evaluateSyncNodeTypeRestriction()); //Either of property or nodetype should not be null
            Filter filter = plan.getFilter();
            paths = FluentIterable.from(Iterables.concat(
                    lookup.query(filter, JCR_PRIMARYTYPE, newName(filter.getPrimaryTypes())),
                    lookup.query(filter, JCR_MIXINTYPES, newName(filter.getMixinTypes()))));
        }

        //No need for path restriction evaluation as thats taken care by PropertyIndex impl itself
        //via content mirror strategy
        FluentIterable<LuceneResultRow> propIndex = paths
                .transform(path -> new LuceneResultRow(path, 0, null, null, null));

        //Property index itr should come first
        return Iterators.concat(propIndex.iterator(), itr);
    }

    static class LuceneResultRow {
        final String path;
        final double score;
        final String suggestion;
        final boolean isVirutal;
        final String excerpt;
        final String explanation;
        final Facets facets;

        LuceneResultRow(String path, double score, String excerpt, Facets facets, String explanation) {
            this.explanation = explanation;
            this.excerpt = excerpt;
            this.facets = facets;
            this.isVirutal = false;
            this.path = path;
            this.score = score;
            this.suggestion = null;
        }

        LuceneResultRow(String suggestion, long weight) {
            this.isVirutal = true;
            this.path = "/";
            this.score = weight;
            this.suggestion = suggestion;
            this.excerpt = null;
            this.facets = null;
            this.explanation = null;
        }

        LuceneResultRow(String suggestion) {
            this(suggestion, 1);
        }

        @Override
        public String toString() {
            return String.format("%s (%1.2f)", path, score);
        }
    }

    /**
     * A cursor over Lucene results. The result includes the path,
     * and the jcr:score pseudo-property as returned by Lucene.
     */
    static class LucenePathCursor implements Cursor {

        private static final int TRAVERSING_WARNING = Integer.getInteger("oak.traversing.warning", 10000);

        private final Cursor pathCursor;
        private final String pathPrefix;
        LuceneResultRow currentRow;
        private final SizeEstimator sizeEstimator;
        private long estimatedSize;
        private int numberOfFacets;

        LucenePathCursor(final Iterator<LuceneResultRow> it, final IndexPlan plan, QueryLimits settings, SizeEstimator sizeEstimator) {
            pathPrefix = plan.getPathPrefix();
            this.sizeEstimator = sizeEstimator;
            Iterator<String> pathIterator = new Iterator<String>() {

                private int readCount;

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public String next() {
                    currentRow = it.next();
                    readCount++;
                    if (readCount % TRAVERSING_WARNING == 0) {
                        Cursors.checkReadLimit(readCount, settings);
                        LOG.warn("Index-Traversed {} nodes with filter {}", readCount, plan.getFilter());
                    }
                    return currentRow.path;
                }

                @Override
                public void remove() {
                    it.remove();
                }

            };

            PlanResult planResult = getPlanResult(plan);
            pathCursor = new PathCursor(pathIterator, planResult.isUniquePathsRequired(), settings);
            numberOfFacets = planResult.indexDefinition.getNumberOfTopFacets();
        }


        @Override
        public boolean hasNext() {
            return pathCursor.hasNext();
        }

        @Override
        public void remove() {
            pathCursor.remove();
        }

        @Override
        public IndexRow next() {
            final IndexRow pathRow = pathCursor.next();
            return new IndexRow() {

                @Override
                public boolean isVirtualRow() {
                    return currentRow.isVirutal;
                }

                @Override
                public String getPath() {
                    String sub = pathRow.getPath();
                    if (isVirtualRow()) {
                        return sub;
                    } else if (!"".equals(pathPrefix) && PathUtils.denotesRoot(sub)) {
                        return pathPrefix;
                    } else if (PathUtils.isAbsolute(sub)) {
                        return pathPrefix + sub;
                    } else {
                        return PathUtils.concat(pathPrefix, sub);
                    }
                }

                @Override
                public PropertyValue getValue(String columnName) {
                    // overlay the score
                    if (QueryConstants.JCR_SCORE.equals(columnName)) {
                        return PropertyValues.newDouble(currentRow.score);
                    }
                    if (QueryConstants.REP_SPELLCHECK.equals(columnName) || QueryConstants.REP_SUGGEST.equals(columnName)) {
                        return PropertyValues.newString(currentRow.suggestion);
                    }
                    if (QueryConstants.OAK_SCORE_EXPLANATION.equals(columnName)) {
                        return PropertyValues.newString(currentRow.explanation);
                    }
                    if (QueryConstants.REP_EXCERPT.equals(columnName)) {
                        return PropertyValues.newString(currentRow.excerpt);
                    }
                    if (columnName.startsWith(QueryConstants.REP_FACET)) {
                        String facetFieldName = FacetHelper.parseFacetField(columnName);
                        Facets facets = currentRow.facets;
                        try {
                            if (facets != null) {
                                FacetResult topChildren = facets.getTopChildren(numberOfFacets, facetFieldName);
                                if (topChildren != null) {
                                    JsopWriter writer = new JsopBuilder();
                                    writer.object();
                                    for (LabelAndValue lav : topChildren.labelValues) {
                                        writer.key(lav.label).value(lav.value.intValue());
                                    }
                                    writer.endObject();
                                    return PropertyValues.newString(writer.toString());
                                } else {
                                    return null;
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return pathRow.getValue(columnName);
                }

            };
        }


        @Override
        public long getSize(SizePrecision precision, long max) {
            if (estimatedSize != 0) {
                return estimatedSize;
            }
            return estimatedSize = sizeEstimator.getSize();
        }
    }

}
