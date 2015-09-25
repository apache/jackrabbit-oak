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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.MoreLikeThisHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SpellcheckHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors.PathCursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvanceFulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
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
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.NATIVE_SORT_ORDER;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newAncestorTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.jackrabbit.oak.query.QueryImpl.JCR_PATH;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.NativeQueryIndex;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.MUST_NOT;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Provides a QueryIndex that does lookups against a Lucene-based index
 *
 * <p>
 * To define a lucene index on a subtree you have to add an
 * <code>oak:index<code> node.
 *
 * Under it follows the index definition node that:
 * <ul>
 * <li>must be of type <code>oak:QueryIndexDefinition</code></li>
 * <li>must have the <code>type</code> property set to <b><code>lucene</code></b></li>
 * <li>must have the <code>async</code> property set to <b><code>async</code></b></li>
 * </b></li>
 * </ul>
 * </p>
 * <p>
 * Optionally you can add
 * <ul>
 * <li>what subset of property types to be included in the index via the <code>includePropertyTypes<code> property</li>
 * <li>a blacklist of property names: what property to be excluded from the index via the <code>excludePropertyNames<code> property</li>
 * <li>the <code>reindex<code> flag which when set to <code>true<code>, triggers a full content re-index.</li>
 * </ul>
 * </p>
 * <pre>
 * <code>
 * {
 *     NodeBuilder index = root.child("oak:index");
 *     index.child("lucene")
 *         .setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME)
 *         .setProperty("type", "lucene")
 *         .setProperty("async", "async")
 *         .setProperty("reindex", "true");
 * }
 * </code>
 * </pre>
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
    static final int LUCENE_QUERY_BATCH_SIZE = 50;

    protected final IndexTracker tracker;

    private final ScorerProviderFactory scorerProviderFactory;

    public LucenePropertyIndex(IndexTracker tracker) {
        this.tracker = tracker;
        this.scorerProviderFactory = ScorerProviderFactory.DEFAULT;
    }

    public LucenePropertyIndex(IndexTracker tracker, ScorerProviderFactory factory) {
        this.tracker = tracker;
        this.scorerProviderFactory = factory;
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
        IndexNode indexNode = null;
        for (String path : indexPaths) {
            try {
                indexNode = tracker.acquireIndexNode(path);

                if (indexNode != null) {
                    IndexPlan plan = new IndexPlanner(indexNode, path, filter, sortOrder).getPlan();
                    if (plan != null) {
                        plans.add(plan);
                    }
                }
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
            sb.append(getLuceneRequest(plan, null));
            if(plan.getSortOrder() != null && !plan.getSortOrder().isEmpty()){
                sb.append(" ordering:").append(plan.getSortOrder());
            }
            if (ft != null) {
                sb.append(" ft:(").append(ft).append(")");
            }
            return sb.toString();
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
        final Sort sort = getSort(plan);
        final PlanResult pr = getPlanResult(plan);
        QueryEngineSettings settings = filter.getQueryEngineSettings();
        Iterator<LuceneResultRow> itr = new AbstractIterator<LuceneResultRow>() {
            private final Deque<LuceneResultRow> queue = Queues.newArrayDeque();
            private final Set<String> seenPaths = Sets.newHashSet();
            private ScoreDoc lastDoc;
            private int nextBatchSize = LUCENE_QUERY_BATCH_SIZE;
            private boolean noDocs = false;
            private long lastSearchIndexerVersion;

            @Override
            protected LuceneResultRow computeNext() {
                while (!queue.isEmpty() || loadDocs()) {
                    return queue.remove();
                }
                return endOfData();
            }

            private LuceneResultRow convertToRow(ScoreDoc doc, IndexSearcher searcher) throws IOException {
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

                        if (path == null){
                            LOG.trace("Ignoring path {} : Transformation returned null", originalPath);
                            return null;
                        }

                        // avoid duplicate entries
                        if (seenPaths.contains(path)){
                            LOG.trace("Ignoring path {} : Duplicate post transformation", originalPath);
                            return null;
                        }
                        seenPaths.add(path);
                    }

                    LOG.trace("Matched path {}", path);
                    return new LuceneResultRow(path, doc.score);
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

                IndexNode indexNode = acquireIndexNode(plan);
                checkState(indexNode != null);
                try {
                    IndexSearcher searcher = indexNode.getSearcher();
                    LuceneRequestFacade luceneRequestFacade = getLuceneRequest(plan, searcher.getIndexReader());
                    if (luceneRequestFacade.getLuceneRequest() instanceof Query) {
                        Query query = (Query) luceneRequestFacade.getLuceneRequest();

                        CustomScoreQuery customScoreQuery = getCustomScoreQuery(plan, query);

                        if (customScoreQuery != null) {
                            query = customScoreQuery;
                        }

                        checkForIndexVersionChange(searcher);

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

                            for (ScoreDoc doc : docs.scoreDocs) {
                                LuceneResultRow row = convertToRow(doc, searcher);
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
                        SpellcheckHelper.SpellcheckQuery spellcheckQuery = (SpellcheckHelper.SpellcheckQuery) luceneRequestFacade.getLuceneRequest();
                        SuggestWord[] suggestWords = SpellcheckHelper.getSpellcheck(spellcheckQuery);

                        // ACL filter spellchecks
                        Collection<String> suggestedWords = new ArrayList<String>(suggestWords.length);
                        QueryParser qp = new QueryParser(Version.LUCENE_47, FieldNames.FULLTEXT, indexNode.getDefinition().getAnalyzer());
                        for (SuggestWord suggestion : suggestWords) {
                            Query query = qp.createPhraseQuery(FieldNames.FULLTEXT, suggestion.string);
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
                        noDocs = true;
                    } else if (luceneRequestFacade.getLuceneRequest() instanceof SuggestHelper.SuggestQuery) {
                        SuggestHelper.SuggestQuery suggestQuery = (SuggestHelper.SuggestQuery) luceneRequestFacade.getLuceneRequest();

                        List<Lookup.LookupResult> lookupResults = SuggestHelper.getSuggestions(suggestQuery);

                        // ACL filter suggestions
                        Collection<String> suggestedWords = new ArrayList<String>(lookupResults.size());
                        QueryParser qp = new QueryParser(Version.LUCENE_47, FieldNames.SUGGEST, indexNode.getDefinition().getAnalyzer());
                        for (Lookup.LookupResult suggestion : lookupResults) {
                            Query query = qp.createPhraseQuery(FieldNames.SUGGEST, suggestion.key.toString());
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
                        noDocs = true;
                    }
                } catch (IOException e) {
                    LOG.warn("query via {} failed.", LucenePropertyIndex.this, e);
                } finally {
                    indexNode.release();
                }

                if (lastDocToRecord != null) {
                    this.lastDoc = lastDocToRecord;
                }

                return !queue.isEmpty();
            }

            private void checkForIndexVersionChange(IndexSearcher searcher) {
                long currentVersion = getVersion(searcher);
                if (currentVersion != lastSearchIndexerVersion && lastDoc != null){
                    lastDoc = null;
                    LOG.debug("Change in index version detected {} => {}. Query would be performed without " +
                            "offset", currentVersion, lastSearchIndexerVersion);
                }
                this.lastSearchIndexerVersion = currentVersion;
            }
        };
        SizeEstimator sizeEstimator = new SizeEstimator() {
            @Override
            public long getSize() {
                IndexNode indexNode = acquireIndexNode(plan);
                checkState(indexNode != null);
                try {
                    IndexSearcher searcher = indexNode.getSearcher();
                    LuceneRequestFacade luceneRequestFacade = getLuceneRequest(plan, searcher.getIndexReader());
                    if (luceneRequestFacade.getLuceneRequest() instanceof Query) {
                        Query query = (Query) luceneRequestFacade.getLuceneRequest();
                        TotalHitCountCollector collector = new TotalHitCountCollector();
                        searcher.search(query, collector);
                        int totalHits =  collector.getTotalHits();
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
        return new LucenePathCursor(itr, plan, settings, sizeEstimator);
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
    public static boolean isNodePath(String fulltextTermPath){
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

        List<SortField> fieldsList = newArrayListWithCapacity(sortOrder.size());
        PlanResult planResult = getPlanResult(plan);
        for (int i = 0; i < sortOrder.size(); i++) {
            OrderEntry oe = sortOrder.get(i);
            if (!isNativeSort(oe)) {
                PropertyDefinition pd = planResult.getOrderedProperty(i);
                boolean reverse = oe.getOrder() != OrderEntry.Order.ASCENDING;
                String propName = oe.getPropertyName();
                propName = FieldNames.createDocValFieldName(propName);
                fieldsList.add(new SortField(propName, toLuceneSortType(oe, pd), reverse));
            }
        }

        if (fieldsList.isEmpty()) {
            return null;
        } else {
            return new Sort(fieldsList.toArray(new SortField[0]));
        }
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

    private static String getIndexName(IndexPlan plan){
        return PathUtils.getName(getPlanResult(plan).indexPath);
    }

    /**
     * Get the Lucene query for the given filter.
     *
     * @param plan index plan containing filter details
     * @param reader the Lucene reader
     * @return the Lucene query
     */
    private static LuceneRequestFacade getLuceneRequest(IndexPlan plan, IndexReader reader) {
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
            qs.add(getFullTextQuery(plan, ft, analyzer));
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
            List<OrderEntry> orders = plan.getSortOrder();
            for (int i = 0; i < orders.size(); i++) {
                OrderEntry oe = orders.get(i);
                if (!isNativeSort(oe)) {
                    PropertyDefinition pd = planResult.getOrderedProperty(i);
                    PropertyRestriction orderRest = new PropertyRestriction();
                    orderRest.propertyName = oe.getPropertyName();
                    Query q = createQuery(orderRest, pd);
                    if (q != null) {
                        qs.add(q);
                    }
                }
            }
        }

        if (qs.size() == 0) {
            if (reader == null){
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
        if (qs.size() == 1) {
            return new LuceneRequestFacade<Query>(qs.get(0));
        }
        BooleanQuery bq = new BooleanQuery();
        for (Query q : qs) {
            bq.add(q, MUST);
        }
        return new LuceneRequestFacade<Query>(bq);
    }

    private CustomScoreQuery getCustomScoreQuery(IndexPlan plan, Query subQuery) {
        PlanResult planResult = getPlanResult(plan);
        IndexDefinition idxDef = planResult.indexDefinition;
        String providerName = idxDef.getScorerProviderName();
        if (scorerProviderFactory != null && providerName != null) {
               return  scorerProviderFactory.getScorerProvider(providerName)
                       .createCustomScoreQuery(subQuery);
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

            if ("rep:excerpt".equals(name)) {
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

            Query q = createQuery(pr, pd);
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
            } else if (pr.list != null && !pr.list.isEmpty()){
                typeFromRestriction = pr.list.get(0).getType().tag();
            }
        }
        return getPropertyType(defn, pr.propertyName, typeFromRestriction);
    }

    private static int getPropertyType(PropertyDefinition defn, String name, int defaultVal){
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
    private static Query createQuery(PropertyRestriction pr,
                                     PropertyDefinition defn) {
        int propType = determinePropertyType(defn, pr);

        if (pr.isNullRestriction()){
            return new TermQuery(new Term(FieldNames.NULL_PROPS, defn.name));
        }

        //If notNullCheckEnabled explicitly enabled use the simple TermQuery
        //otherwise later fallback to range query
        if (pr.isNotNullRestriction() && defn.notNullCheckEnabled){
            return new TermQuery(new Term(FieldNames.NOT_NULL_PROPS, defn.name));
        }

        switch (propType) {
            case PropertyType.DATE: {
                Long first = pr.first != null ? FieldFactory.dateToLong(pr.first.getValue(Type.DATE)) : null;
                Long last = pr.last != null ? FieldFactory.dateToLong(pr.last.getValue(Type.DATE)) : null;
                if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                        && pr.lastIncluding) {
                    // [property]=[value]
                    return NumericRangeQuery.newLongRange(pr.propertyName, first, first, true, true);
                } else if (pr.first != null && pr.last != null) {
                    return NumericRangeQuery.newLongRange(pr.propertyName, first, last,
                            pr.firstIncluding, pr.lastIncluding);
                } else if (pr.first != null && pr.last == null) {
                    // '>' & '>=' use cases
                    return NumericRangeQuery.newLongRange(pr.propertyName, first, null, pr.firstIncluding, true);
                } else if (pr.last != null && !pr.last.equals(pr.first)) {
                    // '<' & '<='
                    return NumericRangeQuery.newLongRange(pr.propertyName, null, last, true, pr.lastIncluding);
                } else if (pr.list != null) {
                    BooleanQuery in = new BooleanQuery();
                    for (PropertyValue value : pr.list) {
                        Long dateVal = FieldFactory.dateToLong(value.getValue(Type.DATE));
                        in.add(NumericRangeQuery.newLongRange(pr.propertyName, dateVal, dateVal, true, true), BooleanClause.Occur.SHOULD);
                    }
                    return in;
                } else if (pr.isNotNullRestriction()) {
                    // not null. For date lower bound of zero can be used
                    return NumericRangeQuery.newLongRange(pr.propertyName, 0L, Long.MAX_VALUE, true, true);
                }

                break;
            }
            case PropertyType.DOUBLE: {
                Double first = pr.first != null ? pr.first.getValue(Type.DOUBLE) : null;
                Double last = pr.last != null ? pr.last.getValue(Type.DOUBLE) : null;
                if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                        && pr.lastIncluding) {
                    // [property]=[value]
                    return NumericRangeQuery.newDoubleRange(pr.propertyName, first, first, true, true);
                } else if (pr.first != null && pr.last != null) {
                    return NumericRangeQuery.newDoubleRange(pr.propertyName, first, last,
                            pr.firstIncluding, pr.lastIncluding);
                } else if (pr.first != null && pr.last == null) {
                    // '>' & '>=' use cases
                    return NumericRangeQuery.newDoubleRange(pr.propertyName, first, null, pr.firstIncluding, true);
                } else if (pr.last != null && !pr.last.equals(pr.first)) {
                    // '<' & '<='
                    return NumericRangeQuery.newDoubleRange(pr.propertyName, null, last, true, pr.lastIncluding);
                } else if (pr.list != null) {
                    BooleanQuery in = new BooleanQuery();
                    for (PropertyValue value : pr.list) {
                        Double doubleVal = value.getValue(Type.DOUBLE);
                        in.add(NumericRangeQuery.newDoubleRange(pr.propertyName, doubleVal, doubleVal, true, true), BooleanClause.Occur.SHOULD);
                    }
                    return in;
                } else if (pr.isNotNullRestriction()) {
                    // not null.
                    return NumericRangeQuery.newDoubleRange(pr.propertyName, Double.MIN_VALUE, Double.MAX_VALUE, true, true);
                }
                break;
            }
            case PropertyType.LONG: {
                Long first = pr.first != null ? pr.first.getValue(LONG) : null;
                Long last = pr.last != null ? pr.last.getValue(LONG) : null;
                if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                        && pr.lastIncluding) {
                    // [property]=[value]
                    return NumericRangeQuery.newLongRange(pr.propertyName, first, first, true, true);
                } else if (pr.first != null && pr.last != null) {
                    return NumericRangeQuery.newLongRange(pr.propertyName, first, last,
                            pr.firstIncluding, pr.lastIncluding);
                } else if (pr.first != null && pr.last == null) {
                    // '>' & '>=' use cases
                    return NumericRangeQuery.newLongRange(pr.propertyName, first, null, pr.firstIncluding, true);
                } else if (pr.last != null && !pr.last.equals(pr.first)) {
                    // '<' & '<='
                    return NumericRangeQuery.newLongRange(pr.propertyName, null, last, true, pr.lastIncluding);
                } else if (pr.list != null) {
                    BooleanQuery in = new BooleanQuery();
                    for (PropertyValue value : pr.list) {
                        Long longVal = value.getValue(LONG);
                        in.add(NumericRangeQuery.newLongRange(pr.propertyName, longVal, longVal, true, true), BooleanClause.Occur.SHOULD);
                    }
                    return in;
                } else if (pr.isNotNullRestriction()) {
                    // not null.
                    return NumericRangeQuery.newLongRange(pr.propertyName, Long.MIN_VALUE, Long.MAX_VALUE, true, true);
                }
                break;
            }
            default: {
                if (pr.isLike) {
                    return createLikeQuery(pr.propertyName, pr.first.getValue(STRING));
                }

                //TODO Confirm that all other types can be treated as string
                String first = pr.first != null ? pr.first.getValue(STRING) : null;
                String last = pr.last != null ? pr.last.getValue(STRING) : null;
                if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                        && pr.lastIncluding) {
                    // [property]=[value]
                    return new TermQuery(new Term(pr.propertyName, first));
                } else if (pr.first != null && pr.last != null) {
                    return TermRangeQuery.newStringRange(pr.propertyName, first, last,
                            pr.firstIncluding, pr.lastIncluding);
                } else if (pr.first != null && pr.last == null) {
                    // '>' & '>=' use cases
                    return TermRangeQuery.newStringRange(pr.propertyName, first, null, pr.firstIncluding, true);
                } else if (pr.last != null && !pr.last.equals(pr.first)) {
                    // '<' & '<='
                    return TermRangeQuery.newStringRange(pr.propertyName, null, last, true, pr.lastIncluding);
                } else if (pr.list != null) {
                    BooleanQuery in = new BooleanQuery();
                    for (PropertyValue value : pr.list) {
                        String strVal = value.getValue(STRING);
                        in.add(new TermQuery(new Term(pr.propertyName, strVal)), BooleanClause.Occur.SHOULD);
                    }
                    return in;
                } else if (pr.isNotNullRestriction()) {
                    return new TermRangeQuery(pr.propertyName, null, null, true, true);
                }
            }
        }
        throw new IllegalStateException("PropertyRestriction not handled " + pr + " for index " + defn );
    }

    static long getVersion(IndexSearcher indexSearcher){
        IndexReader reader = indexSearcher.getIndexReader();
        if (reader instanceof DirectoryReader){
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

        throw new IllegalStateException("For nodeName queries only EQUALS and LIKE are supported "+pr);
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
                                  final Analyzer analyzer) {
        final PlanResult pr = getPlanResult(plan);
        // a reference to the query, so it can be set in the visitor
        // (a "non-local return")
        final AtomicReference<Query> result = new AtomicReference<Query>();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextContains contains) {
                visitTerm(contains.getPropertyName(), contains.getRawText(), null, false);
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                BooleanQuery q = new BooleanQuery();
                for (FullTextExpression e : or.list) {
                    Query x = getFullTextQuery(plan, e, analyzer);
                    q.add(x, SHOULD);
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BooleanQuery q = new BooleanQuery();
                for (FullTextExpression e : and.list) {
                    Query x = getFullTextQuery(plan, e, analyzer);
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
                Query q = tokenToQuery(text, p, pr.indexingRule,  analyzer);
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
        if (p == null){
            return FieldNames.FULLTEXT;
        }

        if (isNodePath(p)){
            if (pr.isPathTransformed()){
                p = PathUtils.getName(p);
            } else {
                //Get rid of /* as aggregated fulltext field name is the
                //node relative path
                p = FieldNames.createFulltextFieldName(PathUtils.getParentPath(p));
            }
        } else {
            if (pr.isPathTransformed()){
                p = PathUtils.getName(p);
            }
            p = FieldNames.createAnalyzedFieldName(p);
        }

        if ("*".equals(p)){
            p = FieldNames.FULLTEXT;
        }
        return p;
    }

    private static Query tokenToQuery(String text, String fieldName, IndexingRule indexingRule, Analyzer analyzer) {
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
            return in;
        }
        return tokenToQuery(text, fieldName, analyzer);
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
     * Following logic is taken from org.apache.jackrabbit.core.query.lucene.JackrabbitQueryParser#parse(java.lang.String)
     */
    private static String rewriteQueryText(String textsearch){
        // replace escaped ' with just '
        StringBuilder rewritten = new StringBuilder();
        // the default lucene query parser recognizes 'AND' and 'NOT' as
        // keywords.
        textsearch = textsearch.replaceAll("AND", "and");
        textsearch = textsearch.replaceAll("NOT", "not");
        boolean escaped = false;
        for (int i = 0; i < textsearch.length(); i++) {
            if (textsearch.charAt(i) == '\\') {
                if (escaped) {
                    rewritten.append("\\\\");
                    escaped = false;
                } else {
                    escaped = true;
                }
            } else if (textsearch.charAt(i) == '\'') {
                if (escaped) {
                    escaped = false;
                }
                rewritten.append(textsearch.charAt(i));
            } else if (textsearch.charAt(i) == ':') {
                // fields as known in lucene are not supported
                rewritten.append("\\:");
            } else {
                if (escaped) {
                    rewritten.append('\\');
                    escaped = false;
                }
                rewritten.append(textsearch.charAt(i));
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

    static class LuceneResultRow {
        final String path;
        final double score;
        final Iterable<String> suggestWords;

        LuceneResultRow(String path, double score) {
            this.path = path;
            this.score = score;
            this.suggestWords = Collections.emptySet();
        }

        LuceneResultRow(Iterable<String> suggestWords) {
            this.path = "/";
            this.score = 1.0d;
            this.suggestWords = suggestWords;
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

        private final Cursor pathCursor;
        private final String pathPrefix;
        LuceneResultRow currentRow;
        private final SizeEstimator sizeEstimator;
        private long estimatedSize;

        LucenePathCursor(final Iterator<LuceneResultRow> it, final IndexPlan plan, QueryEngineSettings settings, SizeEstimator sizeEstimator) {
            pathPrefix = plan.getPathPrefix();
            this.sizeEstimator = sizeEstimator;
            Iterator<String> pathIterator = new Iterator<String>() {

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public String next() {
                    currentRow = it.next();
                    return currentRow.path;
                }

                @Override
                public void remove() {
                    it.remove();
                }

            };
            pathCursor = new PathCursor(pathIterator, false, settings);
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
                    return getPath() == null;
                }

                @Override
                public String getPath() {
                    String sub = pathRow.getPath();
                    if (PathUtils.isAbsolute(sub)) {
                        return pathPrefix + sub;
                    } else {
                        return PathUtils.concat(pathPrefix, sub);
                    }
                }

                @Override
                public PropertyValue getValue(String columnName) {
                    // overlay the score
                    if (QueryImpl.JCR_SCORE.equals(columnName)) {
                        return PropertyValues.newDouble(currentRow.score);
                    }
                    if (QueryImpl.REP_SPELLCHECK.equals(columnName) || QueryImpl.REP_SUGGEST.equals(columnName)) {
                        return PropertyValues.newString(Iterables.toString(currentRow.suggestWords));
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

    private static class PathStoredFieldVisitor extends StoredFieldVisitor {

        private String path;
        private boolean pathVisited;

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (PATH.equals(fieldInfo.name)) {
                return Status.YES;
            }
            return pathVisited ? Status.STOP : Status.NO;
        }

        @Override
        public void stringField(FieldInfo fieldInfo, String value)
                throws IOException {
            if (PATH.equals(fieldInfo.name)) {
                path = value;
                pathVisited = true;
            }
        }

        public String getPath() {
            return path;
        }
    }

}
