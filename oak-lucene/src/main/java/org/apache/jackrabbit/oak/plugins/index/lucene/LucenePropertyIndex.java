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

import javax.annotation.CheckForNull;
import javax.jcr.PropertyType;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.MoreLikeThisHelper;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.fulltext.FullTextAnd;
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
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getAncestorPath;
import static org.apache.jackrabbit.oak.commons.PathUtils.getDepth;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newFulltextTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.jackrabbit.oak.query.QueryImpl.JCR_PATH;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvanceFulltextQueryIndex;
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
public class LucenePropertyIndex implements AdvanceFulltextQueryIndex {

    private static final Logger LOG = LoggerFactory
            .getLogger(LucenePropertyIndex.class);
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

    protected final IndexTracker tracker;

    private final Analyzer analyzer;

    private final NodeAggregator aggregator;

    public LucenePropertyIndex(
            IndexTracker tracker, Analyzer analyzer,
            NodeAggregator aggregator) {
        this.tracker = tracker;
        this.analyzer = analyzer;
        this.aggregator = aggregator;
    }

    @Override
    public String getIndexName() {
        return "lucene";
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
            StringBuilder sb = new StringBuilder("lucene:");
            String path = (String) plan.getAttribute(ATTR_INDEX_PATH);
            sb.append(getIndexName(plan))
                    .append("(")
                    .append(path)
                    .append(") ");
            sb.append(getQuery(plan, null, nonFullTextConstraints, analyzer, index.getDefinition()));
            if(plan.getSortOrder() != null && !plan.getSortOrder().isEmpty()){
                sb.append(" ordering:").append(plan.getSortOrder());
            }
            if (ft != null) {
                sb.append(" ft:(").append(ft).append(")");
            }
            if (!parent.isEmpty()) {
                sb.append(" parent:").append(parent);
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
        final Sort sort = getSort(plan.getSortOrder(), plan);
        FullTextExpression ft = filter.getFullTextConstraint();
        Set<String> relPaths = getRelativePaths(ft);
        if (relPaths.size() > 1) {
            return new MultiLuceneIndex(filter, rootState, relPaths).query();
        }

        final String parent = relPaths.size() == 0 ? "" : relPaths.iterator().next();
        // we only restrict non-full-text conditions if there is
        // no relative property in the full-text constraint
        final boolean nonFullTextConstraints = parent.isEmpty();
        final int parentDepth = getDepth(parent);
        QueryEngineSettings settings = filter.getQueryEngineSettings();
        Iterator<LuceneResultRow> itr = new AbstractIterator<LuceneResultRow>() {
            private final Deque<LuceneResultRow> queue = Queues.newArrayDeque();
            private final Set<String> seenPaths = Sets.newHashSet();
            private ScoreDoc lastDoc;
            private int nextBatchSize = LUCENE_QUERY_BATCH_SIZE;

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

                    return new LuceneResultRow(path, doc.score);
                }
                return null;
            }

            /**
             * Loads the lucene documents in batches
             * @return true if any document is loaded
             */
            private boolean loadDocs() {
                ScoreDoc lastDocToRecord = null;

                IndexNode indexNode = acquireIndexNode(plan);
                checkState(indexNode != null);
                try {
                    IndexSearcher searcher = indexNode.getSearcher();
                    Query query = getQuery(plan, searcher.getIndexReader(),
                            nonFullTextConstraints, analyzer, indexNode.getDefinition());
                    TopDocs docs;
                    long time = System.currentTimeMillis();
                    if (lastDoc != null) {
                        LOG.debug("loading the next {} entries for query {}", nextBatchSize, query);
                        if (sort == null) {
                            docs = searcher.searchAfter(lastDoc, query, nextBatchSize);
                        } else {
                            docs = searcher.searchAfter(lastDoc, query, LUCENE_QUERY_BATCH_SIZE, sort);
                        }
                    } else {
                        LOG.debug("loading the first {} entries for query {}", nextBatchSize, query);
                        if (sort == null) {
                            docs = searcher.search(query, nextBatchSize);
                        } else {
                            docs = searcher.search(query, LUCENE_QUERY_BATCH_SIZE, sort);
                        }
                    }
                    time = System.currentTimeMillis() - time;
                    LOG.debug("... took {} ms", time);
                    nextBatchSize = (int) Math.min(nextBatchSize * 2L, 100000);

                    for (ScoreDoc doc : docs.scoreDocs) {
                        LuceneResultRow row = convertToRow(doc, searcher);
                        if(row != null) {
                            queue.add(row);
                        }
                        lastDocToRecord = doc;
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
        };
        return new LucenePathCursor(itr, plan, settings);
    }

    private IndexNode acquireIndexNode(IndexPlan plan) {
        return tracker.acquireIndexNode((String) plan.getAttribute(ATTR_INDEX_PATH));
    }

    private Sort getSort(List<OrderEntry> sortOrder, IndexPlan plan) {
        if (sortOrder == null || sortOrder.isEmpty()) {
            return null;
        }
        IndexNode indexNode = acquireIndexNode(plan);
        try {
            IndexDefinition defn = indexNode.getDefinition();
            SortField[] fields = new SortField[sortOrder.size()];
            for (int i = 0; i < sortOrder.size(); i++) {
                OrderEntry oe = sortOrder.get(i);
                boolean reverse = oe.getOrder() != OrderEntry.Order.ASCENDING;
                String propName = oe.getPropertyName();
                if (defn.isOrdered(propName)){
                    propName = FieldNames.createDocValFieldName(propName);
                }
                fields[i] = new SortField(propName, toLuceneSortType(oe, defn), reverse);
            }
            return new Sort(fields);
        } finally {
            indexNode.release();
        }
    }

    private static SortField.Type toLuceneSortType(OrderEntry oe, IndexDefinition defn) {
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
        return PathUtils.getName((String) plan.getAttribute(ATTR_INDEX_PATH));
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
     * @param plan index plan containing filter details
     * @param reader the Lucene reader
     * @param nonFullTextConstraints whether non-full-text constraints (such a
     *            path, node type, and so on) should be added to the Lucene
     *            query
     * @param analyzer the Lucene analyzer used for building the fulltext query
     * @param defn nodestate that contains the index definition
     * @return the Lucene query
     */
    private static Query getQuery(IndexPlan plan, IndexReader reader,
            boolean nonFullTextConstraints, Analyzer analyzer, IndexDefinition defn) {
        List<Query> qs = new ArrayList<Query>();
        Filter filter = plan.getFilter();
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            // there might be no full-text constraint
            // when using the LowCostLuceneIndexProvider
            // which is used for testing
        } else {
            qs.add(getFullTextQuery(ft, analyzer, reader));
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
            }
            else {
                try {
                    qs.add(queryParser.parse(query));
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        } else if (nonFullTextConstraints) {
            addNonFullTextConstraints(qs, filter, reader, analyzer,
                    defn);
        }
        if (qs.size() == 0) {
            if (!defn.isFullTextEnabled()) {
                throw new IllegalStateException("No query created for filter " + filter);
            }
            return new MatchAllDocsQuery();
        }
        if (qs.size() == 1) {
            return qs.get(0);
        }
        BooleanQuery bq = new BooleanQuery();
        for (Query q : qs) {
            bq.add(q, MUST);
        }
        return bq;
    }

    private static void addNonFullTextConstraints(List<Query> qs,
            Filter filter, IndexReader reader, Analyzer analyzer, IndexDefinition indexDefinition) {
        if (!filter.matchesAllTypes()) {
            addNodeTypeConstraints(indexDefinition, qs, filter);
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

        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {

            if (pr.first == null && pr.last == null && pr.list == null) {
                // ignore property existence checks, Lucene can't to 'property
                // is not null' queries (OAK-1208)

                //TODO May be this can be relaxed if we have an index which
                //maintains the list of propertyName present in a node say
                //against :propNames. Only configured set of properties would
                //be

                continue;
            }

            // check excluded properties and types
            if (isExcludedProperty(pr, indexDefinition)) {
                continue;
            }

            String name = pr.propertyName;
            if ("rep:excerpt".equals(name)) {
                continue;
            }
            if (JCR_PRIMARYTYPE.equals(name)) {
                continue;
            }

            if (indexDefinition.skipTokenization(name)
                    && indexDefinition.includeProperty(pr.propertyName)) {
                Query q = createQuery(pr, indexDefinition);
                if(q != null) {
                    qs.add(q);
                }
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

    private static int getPropertyType(IndexDefinition defn, String name, int defaultVal){
        if (defn.hasPropertyDefinition(name)) {
            return defn.getPropDefn(name).getPropertyType();
        }
        return defaultVal;
    }

    @CheckForNull
    private static Query createQuery(PropertyRestriction pr,
                                     IndexDefinition defn) {
        int propType = getPropertyType(defn, pr.propertyName, pr.propertyType);
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
                }
                break;
            }
            default: {
                //TODO Support for pr.like

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
                }
            }
        }
        throw new IllegalStateException("PropertyRestriction not handled " + pr + " for index " + defn );
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
            IndexDefinition definition) {
        String name = pr.propertyName;
        if (name.contains("/")) {
            // lucene cannot handle child-level property restrictions
            return true;
        }

        boolean includeProperty = definition.includeProperty(name);
        // check name
        if (!includeProperty) {
            return true;
        } else if (!definition.isFullTextEnabled()) {
            //For property index do not filter on type. If property
            //is included then that is sufficient
           return false;
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
            if (!definition.includePropertyType(type)) {
                return true;
            }
        }
        return false;
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

    private static void addNodeTypeConstraints(IndexDefinition defn, List<Query> qs, Filter filter) {
        BooleanQuery bq = new BooleanQuery();
        //TODO OAK-2198 Add proper nodeType query support
        if (defn.includeProperty(JCR_PRIMARYTYPE)) {
            for (String type : filter.getPrimaryTypes()) {
                bq.add(new TermQuery(new Term(JCR_PRIMARYTYPE, type)), SHOULD);
            }
        }

        if (defn.includeProperty(JCR_MIXINTYPES)) {
            for (String type : filter.getMixinTypes()) {
                bq.add(new TermQuery(new Term(JCR_MIXINTYPES, type)), SHOULD);
            }
        }

        if (bq.clauses().size() != 0) {
            qs.add(bq);
        }
    }

    static Query getFullTextQuery(FullTextExpression ft, final Analyzer analyzer, final IndexReader reader) {
        // a reference to the query, so it can be set in the visitor
        // (a "non-local return")
        final AtomicReference<Query> result = new AtomicReference<Query>();
        ft.accept(new FullTextVisitor() {

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
                    // Lucene can't deal with "must(must_not(x))"
                    if (x instanceof BooleanQuery) {
                        BooleanQuery bq = (BooleanQuery) x;
                        for (BooleanClause c : bq.clauses()) {
                            q.add(c);
                        }
                    } else {
                        q.add(x, MUST);
                    }
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                String p = term.getPropertyName();
                if (p != null && p.indexOf('/') >= 0) {
                    p = getName(p);
                }
                Query q = tokenToQuery(term.getText(), p, analyzer, reader);
                if (q == null) {
                    return false;
                }
                String boost = term.getBoost();
                if (boost != null) {
                    q.setBoost(Float.parseFloat(boost));
                }
                if (term.isNot()) {
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
                MultiPhraseQuery mpq = new MultiPhraseQuery();
                for(String token: tokens){
                    if (hasFulltextToken(token)) {
                        Term[] terms = extractMatchingTokens(reader, fieldName, token);
                        if (terms != null && terms.length > 0) {
                            mpq.add(terms);
                        }
                    } else {
                        mpq.add(newFulltextTerm(token, fieldName));
                    }
                }
                return mpq;
            } else {
                PhraseQuery pq = new PhraseQuery();
                for (String t : tokens) {
                    pq.add(newFulltextTerm(t, fieldName));
                }
                return pq;
            }
        }
    }

    private static Term[] extractMatchingTokens(IndexReader reader, String fieldName, String token) {
        if (reader == null) {
            // getPlan call
            return null;
        }

        try {
            List<Term> terms = new ArrayList<Term>();
            Term onTerm = newFulltextTerm(token, fieldName);
            Terms t = MultiFields.getTerms(reader, onTerm.field());
            Automaton a = WildcardQuery.toAutomaton(onTerm);
            CompiledAutomaton ca = new CompiledAutomaton(a);
            TermsEnum te = ca.getTermsEnum(t);
            BytesRef text;
            while ((text = te.next()) != null) {
                terms.add(newFulltextTerm(text.utf8ToString(), fieldName));
            }
            return terms.toArray(new Term[terms.size()]);
        } catch (IOException e) {
            LOG.error("Building fulltext query failed", e.getMessage());
            return null;
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

        LuceneResultRow(String path, double score) {
            this.path = path;
            this.score = score;
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
        
        LucenePathCursor(final Iterator<LuceneResultRow> it, final IndexPlan plan, QueryEngineSettings settings) {
            pathPrefix = plan.getPathPrefix();
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
                    return pathRow.getValue(columnName);
                }
                
            };
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
