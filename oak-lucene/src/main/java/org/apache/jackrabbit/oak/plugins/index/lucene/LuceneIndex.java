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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
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
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.*;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newFulltextTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.skipTokenization;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_PATH;
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
 * @see QueryIndex
 *
 */
public class LuceneIndex implements AdvanceFulltextQueryIndex {

    private static final Logger LOG = LoggerFactory
            .getLogger(LuceneIndex.class);
    public static final String NATIVE_QUERY_FUNCTION = "native*lucene";
    private static double MIN_COST = 2.2;

    /**
     * IndexPaln Attribute name which refers to the path of Lucene index to be used
     * to perform query
     */
    static final String ATTR_INDEX_PATH = "oak.lucene.indexPath";

    /**
     * Batch size for fetching results from Lucene queries.
     */
    static final int LUCENE_QUERY_BATCH_SIZE = 50;

    static final boolean USE_PATH_RESTRICTION = Boolean.getBoolean("oak.luceneUsePath");

    static final int MAX_RELOAD_COUNT = Integer.getInteger("oak.luceneMaxReloadCount", 16);

    protected final IndexTracker tracker;

    private final NodeAggregator aggregator;

    private final Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter("<strong>", "</strong>"),
            new SimpleHTMLEncoder(), null);

    public LuceneIndex(IndexTracker tracker, NodeAggregator aggregator) {
        this.tracker = tracker;
        this.aggregator = aggregator;
    }

    @Override
    public double getMinimumCost() {
        return MIN_COST;
    }

    @Override
    public String getIndexName() {
        return "lucene";
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            // no full-text condition: don't use this index,
            // as there might be a better one
            return Collections.emptyList();
        }

        String indexPath = new LuceneIndexLookup(rootState).getOldFullTextIndexPath(filter, tracker);
        if (indexPath == null) { // unusable index
            return Collections.emptyList();
        }
        Set<String> relPaths = getRelativePaths(ft);
        if (relPaths.size() > 1) {
            LOG.warn("More than one relative parent for query " + filter.getQueryStatement());
            // there are multiple "parents", as in
            // "contains(a/x, 'hello') and contains(b/x, 'world')"
            return Collections.emptyList();
        }
        IndexNode node = tracker.acquireIndexNode(indexPath);
        try{
            if (node != null){
                IndexDefinition defn = node.getDefinition();
                return Collections.singletonList(planBuilder(filter)
                        .setEstimatedEntryCount(defn.getFulltextEntryCount(node.getIndexStatistics().numDocs()))
                        .setCostPerExecution(defn.getCostPerExecution())
                        .setCostPerEntry(defn.getCostPerEntry())
                        .setAttribute(ATTR_INDEX_PATH, indexPath)
                        .build());
            }
            //No index node then no plan possible
            return Collections.emptyList();
        } finally {
            if (node != null){
                node.release();
            }
        }
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
        IndexNode index = tracker.acquireIndexNode((String) plan.getAttribute(ATTR_INDEX_PATH));
        checkState(index != null, "The Lucene index is not available");
        try {
            FullTextExpression ft = filter.getFullTextConstraint();
            Set<String> relPaths = getRelativePaths(ft);
            if (relPaths.size() > 1) {
                return new MultiLuceneIndex(filter, root, relPaths).getPlan();
            }
            String parent = relPaths.size() == 0 ? "" : relPaths.iterator().next();
            // we only restrict non-full-text conditions if there is
            // no relative property in the full-text constraint
            boolean nonFullTextConstraints = parent.isEmpty();
            String planDesc = getLuceneRequest(filter, null, nonFullTextConstraints, index.getDefinition()) + " ft:(" + ft + ")";
            if (!parent.isEmpty()) {
                planDesc += " parent:" + parent;
            }
            return planDesc;
        } finally {
            index.release();
        }
    }

    @Override
    public Cursor query(final Filter filter, final NodeState root) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    @Override
    public Cursor query(final IndexPlan plan, NodeState rootState) {
        final Filter filter = plan.getFilter();
        FullTextExpression ft = filter.getFullTextConstraint();
        final Set<String> relPaths = getRelativePaths(ft);
        if (relPaths.size() > 1) {
            return new MultiLuceneIndex(filter, rootState, relPaths).query();
        }

        final String parent = relPaths.size() == 0 ? "" : relPaths.iterator().next();
        // we only restrict non-full-text conditions if there is
        // no relative property in the full-text constraint
        final boolean nonFullTextConstraints = parent.isEmpty();
        final int parentDepth = getDepth(parent);
        QueryLimits settings = filter.getQueryLimits();
        Iterator<LuceneResultRow> itr = new AbstractIterator<LuceneResultRow>() {
            private final Deque<LuceneResultRow> queue = Queues.newArrayDeque();
            private final Set<String> seenPaths = Sets.newHashSet();
            private ScoreDoc lastDoc;
            private int nextBatchSize = LUCENE_QUERY_BATCH_SIZE;
            private boolean noDocs = false;
            private long lastSearchIndexerVersion;
            private int reloadCount;

            @Override
            protected LuceneResultRow computeNext() {
                while (!queue.isEmpty() || loadDocs()) {
                    return queue.remove();
                }
                return endOfData();
            }

            private LuceneResultRow convertToRow(ScoreDoc doc, IndexSearcher searcher, String excerpt) throws IOException {
                IndexReader reader = searcher.getIndexReader();
                PathStoredFieldVisitor visitor = new PathStoredFieldVisitor();
                reader.document(doc.doc, visitor);
                String path = visitor.getPath();
                if (path != null) {
                    if ("".equals(path)) {
                        path = "/";
                    }
                    if (!parent.isEmpty()) {
                        // TODO OAK-828 this breaks node aggregation
                        // get the base path
                        // ensure the path ends with the given
                        // relative path
                        // if (!path.endsWith("/" + parent)) {
                        // continue;
                        // }
                        path = getAncestorPath(path, parentDepth);
                        // avoid duplicate entries
                        if (seenPaths.contains(path)) {
                            return null;
                        }
                        seenPaths.add(path);
                    }

                    return new LuceneResultRow(path, doc.score, excerpt);
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

                IndexNode indexNode = tracker.acquireIndexNode((String) plan.getAttribute(ATTR_INDEX_PATH));
                checkState(indexNode != null);
                try {
                    IndexSearcher searcher = indexNode.getSearcher();
                    LuceneRequestFacade luceneRequestFacade = getLuceneRequest(filter, searcher.getIndexReader(),
                            nonFullTextConstraints, indexNode.getDefinition());
                    if (luceneRequestFacade.getLuceneRequest() instanceof Query) {
                        Query query = (Query) luceneRequestFacade.getLuceneRequest();
                        TopDocs docs;
                        long time = System.currentTimeMillis();
                        checkForIndexVersionChange(searcher);
                        while (true) {
                            if (lastDoc != null) {
                                LOG.debug("loading the next {} entries for query {}", nextBatchSize, query);
                                docs = searcher.searchAfter(lastDoc, query, nextBatchSize);
                            } else {
                                LOG.debug("loading the first {} entries for query {}", nextBatchSize, query);
                                docs = searcher.search(query, nextBatchSize);
                            }
                            time = System.currentTimeMillis() - time;
                            LOG.debug("... took {} ms", time);
                            nextBatchSize = (int) Math.min(nextBatchSize * 2L, 100000);

                            PropertyRestriction restriction = filter.getPropertyRestriction(QueryConstants.REP_EXCERPT);
                            boolean addExcerpt = restriction != null && restriction.isNotNullRestriction();

                            Analyzer analyzer = indexNode.getDefinition().getAnalyzer();

                            if (addExcerpt) {
                                // setup highlighter
                                QueryScorer scorer = new QueryScorer(query);
                                scorer.setExpandMultiTermQuery(true);
                                highlighter.setFragmentScorer(scorer);
                            }

                            for (ScoreDoc doc : docs.scoreDocs) {
                                String excerpt = null;
                                if (addExcerpt) {
                                    excerpt = getExcerpt(analyzer, searcher, doc);
                                }

                                LuceneResultRow row = convertToRow(doc, searcher, excerpt);
                                if (row != null) {
                                    queue.add(row);
                                }
                                lastDocToRecord = doc;
                            }

                            if (queue.isEmpty() && docs.scoreDocs.length > 0) {
                                lastDoc = lastDocToRecord;
                            } else {
                                break;
                            }
                        }
                    } else if (luceneRequestFacade.getLuceneRequest() instanceof SpellcheckHelper.SpellcheckQuery) {
                        SpellcheckHelper.SpellcheckQuery spellcheckQuery = (SpellcheckHelper.SpellcheckQuery) luceneRequestFacade.getLuceneRequest();
                        noDocs = true;
                        SuggestWord[] suggestWords = SpellcheckHelper.getSpellcheck(spellcheckQuery);

                        // ACL filter spellchecks
                        Collection<String> suggestedWords = new ArrayList<String>(suggestWords.length);
                        QueryParser qp = new QueryParser(Version.LUCENE_47, FieldNames.SUGGEST, indexNode.getDefinition().getAnalyzer());
                        for (SuggestWord suggestion : suggestWords) {
                            Query query = qp.createPhraseQuery(FieldNames.SUGGEST, suggestion.string);
                            TopDocs topDocs = searcher.search(query, 100);
                            if (topDocs.totalHits > 0) {
                                for (ScoreDoc doc : topDocs.scoreDocs) {
                                    Document retrievedDoc = searcher.doc(doc.doc);
                                    if (filter.isAccessible(retrievedDoc.get(FieldNames.PATH))) {
                                        suggestedWords.add(suggestion.string);
                                        break;
                                    }
                                }
                            }
                        }

                        queue.add(new LuceneResultRow(suggestedWords));
                    } else if (luceneRequestFacade.getLuceneRequest() instanceof SuggestHelper.SuggestQuery) {
                        SuggestHelper.SuggestQuery suggestQuery = (SuggestHelper.SuggestQuery) luceneRequestFacade.getLuceneRequest();
                        noDocs = true;
                        List<Lookup.LookupResult> lookupResults = SuggestHelper.getSuggestions(indexNode.getLookup(), suggestQuery);

                        // ACL filter suggestions
                        Collection<String> suggestedWords = new ArrayList<String>(lookupResults.size());
                        QueryParser qp = new QueryParser(Version.LUCENE_47, FieldNames.FULLTEXT, indexNode.getDefinition().getAnalyzer());
                        for (Lookup.LookupResult suggestion : lookupResults) {
                            Query query = qp.createPhraseQuery(FieldNames.FULLTEXT, suggestion.key.toString());
                            TopDocs topDocs = searcher.search(query, 100);
                            if (topDocs.totalHits > 0) {
                                for (ScoreDoc doc : topDocs.scoreDocs) {
                                    Document retrievedDoc = searcher.doc(doc.doc);
                                    if (filter.isAccessible(retrievedDoc.get(FieldNames.PATH))) {
                                        suggestedWords.add("{term=" + suggestion.key + ",weight=" + suggestion.value + "}");
                                        break;
                                    }
                                }
                            }
                        }

                        queue.add(new LuceneResultRow(suggestedWords));
                    }
                } catch (IOException e) {
                    LOG.warn("query via {} failed.", LuceneIndex.this, e);
                } finally {
                    indexNode.release();
                }

                if (lastDocToRecord != null) {
                    this.lastDoc = lastDocToRecord;
                }

                return !queue.isEmpty();
            }

            private void checkForIndexVersionChange(IndexSearcher searcher) {
                long currentVersion = LucenePropertyIndex.getVersion(searcher);
                if (currentVersion != lastSearchIndexerVersion && lastDoc != null){
                    reloadCount++;
                    if (reloadCount > MAX_RELOAD_COUNT) {
                        LOG.error("More than {} index version changes detected for query {}",
                                MAX_RELOAD_COUNT,
                                plan);
                        throw new IllegalStateException("Too many version changes");
                    }
                    lastDoc = null;
                    LOG.debug("Change in index version detected {} => {}. Query would be performed without " +
                            "offset; reload {}", currentVersion, lastSearchIndexerVersion, reloadCount);
                }
                this.lastSearchIndexerVersion = currentVersion;
            }
        };
        SizeEstimator sizeEstimator = new SizeEstimator() {
            @Override
            public long getSize() {
                IndexNode indexNode = tracker.acquireIndexNode((String) plan.getAttribute(ATTR_INDEX_PATH));
                checkState(indexNode != null);
                try {
                    IndexSearcher searcher = indexNode.getSearcher();
                    LuceneRequestFacade luceneRequestFacade = getLuceneRequest(filter, searcher.getIndexReader(),
                            nonFullTextConstraints, indexNode.getDefinition());
                    if (luceneRequestFacade.getLuceneRequest() instanceof Query) {
                        Query query = (Query) luceneRequestFacade.getLuceneRequest();
                        TotalHitCountCollector collector = new TotalHitCountCollector();
                        searcher.search(query, collector);
                        int totalHits =  collector.getTotalHits();
                        LOG.debug("Estimated size for query {} is {}", query, totalHits);
                        return totalHits;
                    }
                    LOG.debug("Estimated size: not a Query: {}", luceneRequestFacade.getLuceneRequest());
                } catch (IOException e) {
                    LOG.warn("query via {} failed.", LuceneIndex.this, e);
                } finally {
                    indexNode.release();
                }
                return -1;
            }
        };
        return new LucenePathCursor(itr, settings, sizeEstimator, filter);
    }

    private String getExcerpt(Analyzer analyzer, IndexSearcher searcher, ScoreDoc doc) throws IOException {
        StringBuilder excerpt = new StringBuilder();

        for (IndexableField field : searcher.getIndexReader().document(doc.doc).getFields()) {
            String name = field.name();
            // only full text or analyzed fields
            if (name.startsWith(FieldNames.FULLTEXT) || name.startsWith(FieldNames.ANALYZED_FIELD_PREFIX)) {
                String text = field.stringValue();
                TokenStream tokenStream = analyzer.tokenStream(name, text);
                try {
                    TextFragment[] textFragments = highlighter.getBestTextFragments(tokenStream, text, true, 2);
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
        return excerpt.toString();
    }

    protected static IndexPlan.Builder planBuilder(Filter filter){
        return new IndexPlan.Builder()
                .setCostPerExecution(0) // we're local. Low-cost
                .setCostPerEntry(1)
                .setFilter(filter)
                .setFulltextIndex(true)
                .setEstimatedEntryCount(0) //TODO Fake it to provide constant cost for now
                .setIncludesNodeData(false) // we should not include node data
                .setDelayed(true); //Lucene is always async
    }

    /**
     * Get the set of relative paths of a full-text condition. For example, for
     * the condition "contains(a/b, 'hello') and contains(c/d, 'world'), the set
     * { "a", "c" } is returned. If there are no relative properties, then one
     * entry is returned (the empty string). If there is no expression, then an
     * empty set is returned.
     *
     * @param ft the full-text expression
     * @return the set of relative paths (possibly empty)
     */
    private static Set<String> getRelativePaths(FullTextExpression ft) {
        if (ft == null) {
            // there might be no full-text constraint when using the
            // LowCostLuceneIndexProvider which is used for testing
            // TODO if the LowCostLuceneIndexProvider is removed, we should do
            // the following instead:

            // throw new
            // IllegalStateException("Lucene index is used even when no full-text conditions are used for filter "
            // + filter);

            return Collections.emptySet();
        }
        final HashSet<String> relPaths = new HashSet<String>();
        ft.accept(new FullTextVisitor.FullTextVisitorBase() {

            @Override
            public boolean visit(FullTextTerm term) {
                String p = term.getPropertyName();
                if (p == null) {
                    relPaths.add("");
                } else if (p.startsWith("../") || p.startsWith("./")) {
                    throw new IllegalArgumentException("Relative parent is not supported:" + p);
                } else if (getDepth(p) > 1) {
                    String parent = getParentPath(p);
                    relPaths.add(parent);
                } else {
                    relPaths.add("");
                }
                return true;
            }
        });
        return relPaths;
    }


    /**
     * Get the Lucene query for the given filter.
     *
     * @param filter the filter, including full-text constraint
     * @param reader the Lucene reader
     * @param nonFullTextConstraints whether non-full-text constraints (such a
     *            path, node type, and so on) should be added to the Lucene
     *            query
     * @param indexDefinition nodestate that contains the index definition
     * @return the Lucene query
     */
    private static LuceneRequestFacade getLuceneRequest(Filter filter, IndexReader reader,
                                                        boolean nonFullTextConstraints, IndexDefinition indexDefinition) {
        List<Query> qs = new ArrayList<Query>();
        Analyzer analyzer = indexDefinition.getAnalyzer();
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            // there might be no full-text constraint
            // when using the LowCostLuceneIndexProvider
            // which is used for testing
        } else {
            qs.add(getFullTextQuery(ft, analyzer, reader));
        }
        PropertyRestriction pr = filter.getPropertyRestriction(NATIVE_QUERY_FUNCTION);
        if (pr != null) {
            String query = String.valueOf(pr.first.getValue(pr.first.getType()));
            QueryParser queryParser = new QueryParser(VERSION, "", indexDefinition.getAnalyzer());
            if (query.startsWith("mlt?")) {
                String mltQueryString = query.replace("mlt?", "");
                if (reader != null) {
                    Query moreLikeThis = MoreLikeThisHelper.getMoreLikeThis(reader, analyzer, mltQueryString);
                    if (moreLikeThis != null) {
                        qs.add(moreLikeThis);
                    }
                }
            }
            if (query.startsWith("spellcheck?")) {
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
        } else if (nonFullTextConstraints) {
            addNonFullTextConstraints(qs, filter, reader, analyzer,
                    indexDefinition);
        }
        if (qs.size() == 0) {
            return new LuceneRequestFacade<Query>(new MatchAllDocsQuery());
        }

        return LucenePropertyIndex.performAdditionalWraps(qs);
    }

    private static void addNonFullTextConstraints(List<Query> qs,
            Filter filter, IndexReader reader, Analyzer analyzer, IndexDefinition indexDefinition) {
        if (!filter.matchesAllTypes()) {
            addNodeTypeConstraints(qs, filter);
        }

        String path = filter.getPath();
        switch (filter.getPathRestriction()) {
        case ALL_CHILDREN:
            if (USE_PATH_RESTRICTION) {
                if ("/".equals(path)) {
                    break;
                }
                if (!path.endsWith("/")) {
                    path += "/";
                }
                qs.add(new PrefixQuery(newPathTerm(path)));
            }
            break;
        case DIRECT_CHILDREN:
            if (USE_PATH_RESTRICTION) {
                if (!path.endsWith("/")) {
                    path += "/";
                }
                qs.add(new PrefixQuery(newPathTerm(path)));
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

        //Fulltext index definition used by LuceneIndex only works with old format
        //which is not nodeType based. So just use the nt:base index
        IndexingRule rule = indexDefinition.getApplicableIndexingRule(JcrConstants.NT_BASE);
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {

            if (pr.first == null && pr.last == null) {
                // we only support equality or range queries,
                // but not "in", "is null", "is not null"
                // queries (OAK-1208)
                continue;
            }

            // check excluded properties and types
            if (isExcludedProperty(pr, rule)) {
                continue;
            }

            String name = pr.propertyName;
            if (QueryConstants.REP_EXCERPT.equals(name) || QueryConstants.OAK_SCORE_EXPLANATION.equals(name) || QueryConstants.REP_FACET.equals(name)) {
                continue;
            }
            if (JCR_PRIMARYTYPE.equals(name)) {
                continue;
            }
            if (QueryConstants.RESTRICTION_LOCAL_NAME.equals(name)) {
                continue;
            }

            if (skipTokenization(name)) {
                qs.add(new TermQuery(new Term(name, pr.first
                        .getValue(STRING))));
                continue;
            }

            String first = null;
            String last = null;
            boolean isLike = pr.isLike;

            // TODO what to do with escaped tokens?
            if (pr.first != null) {
                first = pr.first.getValue(STRING);
                first = first.replace("\\", "");
            }
            if (pr.last != null) {
                last = pr.last.getValue(STRING);
                last = last.replace("\\", "");
            }

            if (isLike) {
                first = first.replace('%', WildcardQuery.WILDCARD_STRING);
                first = first.replace('_', WildcardQuery.WILDCARD_CHAR);

                int indexOfWS = first.indexOf(WildcardQuery.WILDCARD_STRING);
                int indexOfWC = first.indexOf(WildcardQuery.WILDCARD_CHAR);
                int len = first.length();

                if (indexOfWS == len || indexOfWC == len) {
                    // remove trailing "*" for prefixquery
                    first = first.substring(0, first.length() - 1);
                    if (JCR_PATH.equals(name)) {
                        qs.add(new PrefixQuery(newPathTerm(first)));
                    } else {
                        qs.add(new PrefixQuery(new Term(name, first)));
                    }
                } else {
                    if (JCR_PATH.equals(name)) {
                        qs.add(new WildcardQuery(newPathTerm(first)));
                    } else {
                        qs.add(new WildcardQuery(new Term(name, first)));
                    }
                }
                continue;
            }

            if (first != null && first.equals(last) && pr.firstIncluding
                    && pr.lastIncluding) {
                if (JCR_PATH.equals(name)) {
                    qs.add(new TermQuery(newPathTerm(first)));
                } else {
                    if ("*".equals(name)) {
                        addReferenceConstraint(first, qs, reader);
                    } else {
                        for (String t : tokenize(first, analyzer)) {
                            qs.add(new TermQuery(new Term(name, t)));
                        }
                    }
                }
                continue;
            }

            first = tokenizeAndPoll(first, analyzer);
            last = tokenizeAndPoll(last, analyzer);
            qs.add(TermRangeQuery.newStringRange(name, first, last,
                    pr.firstIncluding, pr.lastIncluding));
        }
    }

    private static String tokenizeAndPoll(String token, Analyzer analyzer){
        if (token != null) {
            List<String> tokens = tokenize(token, analyzer);
            if (!tokens.isEmpty()) {
                token = tokens.get(0);
            }
        }
        return token;
    }

    private static boolean isExcludedProperty(PropertyRestriction pr,
            IndexingRule rule) {
        String name = pr.propertyName;
        if (name.contains("/")) {
            // lucene cannot handle child-level property restrictions
            return true;
        }

        PropertyDefinition pd = rule.getConfig(name);
        // check name
        if(pd == null || !pd.index){
            return true;
        }

        // check type
        Integer type = null;
        if (pr.first != null) {
            type = pr.first.getType().tag();
        } else if (pr.last != null) {
            type = pr.last.getType().tag();
        } else if (pr.list != null && !pr.list.isEmpty()) {
            type = pr.list.get(0).getType().tag();
        }
        if (type != null) {
            if (!includePropertyType(type, rule)) {
                return true;
            }
        }
        return false;
    }

    private static boolean includePropertyType(int type, IndexingRule rule){
        if(rule.propertyTypes < 0){
            return false;
        }
        return (rule.propertyTypes & (1 << type)) != 0;
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

    private static void addNodeTypeConstraints(List<Query> qs, Filter filter) {
        BooleanQuery bq = new BooleanQuery();
        for (String type : filter.getPrimaryTypes()) {
            bq.add(new TermQuery(new Term(JCR_PRIMARYTYPE, type)), SHOULD);
        }
        for (String type : filter.getMixinTypes()) {
            bq.add(new TermQuery(new Term(JCR_MIXINTYPES, type)), SHOULD);
        }
        qs.add(bq);
    }

    static Query getFullTextQuery(FullTextExpression ft, final Analyzer analyzer, final IndexReader reader) {
        // a reference to the query, so it can be set in the visitor
        // (a "non-local return")
        final AtomicReference<Query> result = new AtomicReference<Query>();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextContains contains) {
                return contains.getBase().accept(this);
            }

            @Override
            public boolean visit(FullTextOr or) {
                BooleanQuery q = new BooleanQuery();
                for (FullTextExpression e : or.list) {
                    Query x = getFullTextQuery(e, analyzer, reader);
                    q.add(x, SHOULD);
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BooleanQuery q = new BooleanQuery();
                for (FullTextExpression e : and.list) {
                    Query x = getFullTextQuery(e, analyzer, reader);
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
                String p = propertyName;
                if (p != null && p.indexOf('/') >= 0) {
                    p = getName(p);
                }
                Query q = tokenToQuery(text, p, analyzer, reader);
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

    static Query tokenToQuery(String text, String fieldName, Analyzer analyzer, IndexReader reader) {
        if (analyzer == null) {
            return null;
        }
        List<String> tokens = tokenize(text, analyzer);

        if (tokens.isEmpty()) {
            // TODO what should be returned in the case there are no tokens?
            return new BooleanQuery();
        }
        if (tokens.size() == 1) {
            String token = tokens.iterator().next();
            if (hasFulltextToken(token)) {
                return new WildcardQuery(newFulltextTerm(token, fieldName));
            } else {
                return new TermQuery(newFulltextTerm(token, fieldName));
            }
        } else {
            if (hasFulltextToken(tokens)) {
                BooleanQuery bq = new BooleanQuery();
                for(String token: tokens){
                    if (hasFulltextToken(token)) {
                        bq.add(new WildcardQuery(newFulltextTerm(token, fieldName)), Occur.MUST);
                    } else {
                        bq.add(new TermQuery(newFulltextTerm(token, fieldName)), Occur.MUST);
                    }
                }
                return bq;
            } else {
                PhraseQuery pq = new PhraseQuery();
                for (String t : tokens) {
                    pq.add(newFulltextTerm(t, fieldName));
                }
                return pq;
            }
        }
    }

    private static boolean hasFulltextToken(List<String> tokens) {
        for (String token : tokens) {
            if (hasFulltextToken(token)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasFulltextToken(String token) {
        for (char c : fulltextTokens) {
            if (token.indexOf(c) != -1) {
                return true;
            }
        }
        return false;
    }

    private static char[] fulltextTokens = new char[] { '*', '?' };

    /**
     * Tries to merge back tokens that are split on relevant fulltext query
     * wildcards ('*' or '?')
     *
     *
     * @param text
     * @param analyzer
     * @return
     */
    static List<String> tokenize(String text, Analyzer analyzer) {
        List<String> tokens = new ArrayList<String>();
        TokenStream stream = null;
        try {
            stream = analyzer.tokenStream(FieldNames.FULLTEXT,
                    new StringReader(text));
            CharTermAttribute termAtt = stream
                    .addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAtt = stream
                    .addAttribute(OffsetAttribute.class);
            // TypeAttribute type = stream.addAttribute(TypeAttribute.class);

            stream.reset();

            int poz = 0;
            boolean hasFulltextToken = false;
            StringBuilder token = new StringBuilder();
            while (stream.incrementToken()) {
                String term = termAtt.toString();
                int start = offsetAtt.startOffset();
                int end = offsetAtt.endOffset();
                if (start > poz) {
                    for (int i = poz; i < start; i++) {
                        for (char c : fulltextTokens) {
                            if (c == text.charAt(i)) {
                                token.append(c);
                                hasFulltextToken = true;
                            }
                        }
                    }
                }
                poz = end;
                if (hasFulltextToken) {
                    token.append(term);
                    hasFulltextToken = false;
                } else {
                    if (token.length() > 0) {
                        tokens.add(token.toString());
                    }
                    token = new StringBuilder();
                    token.append(term);
                }
            }
            // consume to the end of the string
            if (poz < text.length()) {
                for (int i = poz; i < text.length(); i++) {
                    for (char c : fulltextTokens) {
                        if (c == text.charAt(i)) {
                            token.append(c);
                        }
                    }
                }
            }
            if (token.length() > 0) {
                tokens.add(token.toString());
            }
            stream.end();
        } catch (IOException e) {
            LOG.error("Building fulltext query failed", e.getMessage());
            return null;
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return tokens;
    }

    @Override
    public NodeAggregator getNodeAggregator() {
        return aggregator;
    }

    static class LuceneResultRow {
        final String path;
        final double score;
        final Iterable<String> suggestWords;
        final boolean isVirtual;
        final String excerpt;

        LuceneResultRow(String path, double score, String excerpt) {
            this.isVirtual = false;
            this.path = path;
            this.score = score;
            this.excerpt = excerpt;
            this.suggestWords = Collections.emptySet();
        }

        LuceneResultRow(Iterable<String> suggestWords) {
            this.isVirtual = true;
            this.path = "/";
            this.score = 1.0d;
            this.suggestWords = suggestWords;
            this.excerpt = null;
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
        LuceneResultRow currentRow;
        private final SizeEstimator sizeEstimator;
        private long estimatedSize;

        LucenePathCursor(final Iterator<LuceneResultRow> it, QueryLimits settings, SizeEstimator sizeEstimator, Filter filter) {
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
                        LOG.warn("Index-Traversed {} nodes with filter {}", readCount, filter);
                    }
                    return currentRow.path;
                }

                @Override
                public void remove() {
                    it.remove();
                }

            };
            pathCursor = new PathCursor(pathIterator, true, settings);
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
                    return currentRow.isVirtual;
                }

                @Override
                public String getPath() {
                    return pathRow.getPath();
                }

                @Override
                public PropertyValue getValue(String columnName) {
                    // overlay the score
                    if (QueryConstants.JCR_SCORE.equals(columnName)) {
                        return PropertyValues.newDouble(currentRow.score);
                    }
                    if (QueryConstants.REP_SPELLCHECK.equals(columnName) || QueryConstants.REP_SUGGEST.equals(columnName)) {
                        return PropertyValues.newString(Iterables.toString(currentRow.suggestWords));
                    }
                    if (QueryConstants.REP_EXCERPT.equals(columnName)) {
                        return PropertyValues.newString(currentRow.excerpt);
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
