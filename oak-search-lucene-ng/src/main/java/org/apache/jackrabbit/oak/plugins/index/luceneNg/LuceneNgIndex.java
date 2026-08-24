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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.cursor.Cursors;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgCursor;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexNode;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgSecureSortedSetDocValuesFacetCounts;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgStatisticalSortedSetDocValuesFacetCounts;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.NodeAggregator;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Lucene 9 query index implementation.
 * Executes queries against Lucene 9 indexes.
 */
public class LuceneNgIndex implements QueryIndex.AdvanceFulltextQueryIndex {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndex.class);
    // Must equal FacetHelper.ATTR_FACET_FIELDS — shared via plan attribute
    private static final String ATTR_FACET_FIELDS = "oak.facet.fields";

    private final LuceneNgIndexTracker tracker;
    private final String indexPath;

    public LuceneNgIndex(LuceneNgIndexTracker tracker, String indexPath) {
        this.tracker = tracker;
        this.indexPath = indexPath;
    }

    @Override
    public double getMinimumCost() {
        return 2.0; // Better than traversal (1000+) but not as good as unique lookup (1.0)
    }

    @Override
    public String getIndexName() {
        return "luceneNg";
    }

    /**
     * Returns the index definition path (per {@link QueryIndex#getIndexName(Filter, NodeState)})
     * so callers can distinguish this LuceneNg index instance from others.
     */
    @Override
    public String getIndexName(Filter filter, NodeState rootState) {
        return indexPath;
    }

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        FullTextExpression ft = filter.getFullTextConstraint();
        List<Filter.PropertyRestriction> propRestrictions = filter.getPropertyRestrictions()
                .stream()
                .filter(pr -> pr.propertyName != null)
                .filter(pr -> !pr.propertyName.startsWith("rep:"))
                .filter(pr -> !pr.propertyName.startsWith("oak:"))
                .filter(pr -> !pr.propertyName.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX))
                .collect(Collectors.toList());

        // If we have both full-text and property restrictions, lower cost
        if (ft != null && !propRestrictions.isEmpty()) {
            return 1.5; // Very selective
        }

        // Full-text only
        if (ft != null) {
            return 2.0;
        }

        // Check for property restrictions we can handle
        int supportedRestrictions = 0;
        for (Filter.PropertyRestriction pr : propRestrictions) {
            if (canHandleRestriction(pr)) {
                supportedRestrictions++;
            }
        }

        if (supportedRestrictions > 0) {
            // More restrictions = more selective = lower cost
            return 2.0 / Math.sqrt(supportedRestrictions);
        }

        // Node-type-only query: only return a finite cost when the tracker confirms the
        // index has a rule for the queried type (same guard used in getPlans).
        if (!filter.matchesAllTypes()) {
            String nodeType = filter.getNodeType();
            LuceneNgIndexNode node = tracker.acquireIndexNode(indexPath);
            if (node != null) {
                try {
                    if (nodeType != null
                            && node.getDefinition().getApplicableIndexingRule(nodeType) != null) {
                        return 10.0;
                    }
                } finally {
                    node.release();
                }
            }
        }

        return Double.POSITIVE_INFINITY;
    }

    private boolean canHandleRestriction(Filter.PropertyRestriction pr) {
        // Skip special properties (rep:facet, rep:excerpt, etc.) — they are not
        // regular property restrictions and are handled separately as facet fields
        if (pr.propertyName.startsWith("rep:") || pr.propertyName.startsWith("oak:")) {
            return false;
        }
        // Can handle equality, range, NOT NULL, NULL, NOT, and IN queries
        return pr.first != null || pr.last != null || pr.not != null || pr.list != null
            || pr.isNotNullRestriction() || pr.isNullRestriction();
    }

    @Override
    public String getPlan(Filter filter, NodeState rootState) {
        return "lucene9:" + indexPath + " ft=" + filter.getFullTextConstraint();
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        // Build the Lucene query up front; row iteration acquires the index node per batch
        // inside the cursor rather than holding it open for the cursor's whole lifetime.
        // This overload supports neither sort, facets, nor fulltext excerpts.
        Query query = buildQuery(filter);
        LOG.debug("Executing query: {}", query);
        return new LuceneNgCursor(tracker, indexPath, query, null,
                Collections.emptyMap(), false, null);
    }

    private Query buildQuery(Filter filter) {
        FullTextExpression ft = filter.getFullTextConstraint();

        // Strip rep:facet pseudo-restrictions and function restrictions we don't index.
        // Function restrictions (e.g. "function*@:localname") are paired with their dedicated
        // equivalents (e.g. ":localname") and are handled by createPropertyQuery(); including
        // them as separate clauses would produce a term query on a non-existent field.
        List<Filter.PropertyRestriction> propRestrictions = filter.getPropertyRestrictions()
            .stream()
            .filter(pr -> !QueryConstants.REP_FACET.equals(pr.propertyName))
            .filter(pr -> pr.propertyName == null
                    || !pr.propertyName.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX))
            .collect(Collectors.toList());

        Query pathQuery = buildPathQuery(filter);

        // Build content query (fulltext and/or property constraints)
        Query contentQuery;
        if (ft == null && propRestrictions.isEmpty()) {
            contentQuery = new MatchAllDocsQuery();
        } else if (ft != null) {
            try (Analyzer analyzer = new StandardAnalyzer()) {
                Query ftQuery = getFullTextQuery(ft, analyzer);
                LOG.debug("Building full-text query: {}", ftQuery);
                if (!propRestrictions.isEmpty()) {
                    BooleanQuery.Builder bq = new BooleanQuery.Builder();
                    bq.add(ftQuery, Occur.MUST);
                    for (Filter.PropertyRestriction pr : propRestrictions) {
                        Query propQuery = createPropertyQuery(pr);
                        if (propQuery != null) {
                            bq.add(propQuery, Occur.MUST);
                        }
                    }
                    contentQuery = bq.build();
                } else {
                    contentQuery = ftQuery;
                }
            }
        } else if (propRestrictions.size() == 1) {
            Query q = createPropertyQuery(propRestrictions.get(0));
            contentQuery = q != null ? q : new MatchAllDocsQuery();
        } else {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            for (Filter.PropertyRestriction pr : propRestrictions) {
                Query propQuery = createPropertyQuery(pr);
                if (propQuery != null) {
                    bq.add(propQuery, Occur.MUST);
                }
            }
            contentQuery = bq.build();
        }

        if (pathQuery == null) {
            return contentQuery;
        }
        BooleanQuery.Builder combined = new BooleanQuery.Builder();
        combined.add(contentQuery, Occur.MUST);
        combined.add(pathQuery, Occur.FILTER);
        return combined.build();
    }

    /**
     * Translates the Oak PathRestriction to a Lucene query clause,
     * or returns null for NO_RESTRICTION (no clause added).
     */
    @org.jetbrains.annotations.Nullable
    private Query buildPathQuery(Filter filter) {
        Filter.PathRestriction restriction = filter.getPathRestriction();
        if (restriction == null) {
            return null;
        }
        String path = filter.getPath();
        switch (restriction) {
            case ALL_CHILDREN:
                if ("/".equals(path)) {
                    return null; // matches everything
                }
                return new PrefixQuery(new Term(FieldNames.PATH, path + "/"));
            case DIRECT_CHILDREN:
                return new TermQuery(new Term(LuceneNgIndexConstants.FIELD_PARENT_PATH, path));
            case EXACT:
                return new TermQuery(new Term(FieldNames.PATH, path));
            case PARENT:
                if ("/".equals(path)) {
                    // root has no parent — match nothing
                    return new TermQuery(new Term(FieldNames.PATH, "\u0000"));
                }
                int lastSlash = path.lastIndexOf('/');
                String parentPath = lastSlash == 0 ? "/" : path.substring(0, lastSlash);
                return new TermQuery(new Term(FieldNames.PATH, parentPath));
            case NO_RESTRICTION:
            default:
                return null;
        }
    }

    /**
     * Creates a Lucene Query for a property restriction.
     * Handles equality, range, NOT NULL, NULL, NOT, and IN queries.
     * Based on legacy LuceneIndex pattern.
     */
    private Query createPropertyQuery(Filter.PropertyRestriction pr) {
        String propertyName = pr.propertyName;

        // localname() restriction — maps to the NODE_NAME StringField
        if (QueryConstants.RESTRICTION_LOCAL_NAME.equals(propertyName)) {
            return createLocalNameQuery(pr);
        }

        // Function restrictions (e.g. "function*@:localname", "function*lower*@name") are
        // only supported when the index has an explicit function property definition.
        // We don't support that yet, so skip these to avoid false negatives.
        if (propertyName.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX)) {
            return null;
        }

        // Skip special properties (rep:facet etc.)
        if (propertyName.startsWith("rep:") || propertyName.startsWith("oak:")) {
            return null;
        }

        // Handle IS NOT NULL: matches all documents that have the property indexed
        if (pr.isNotNullRestriction()) {
            return new TermRangeQuery(propertyName, null, null, true, true);
        }

        // Handle IS NULL: currently not efficiently supportable; return MatchAllDocs
        // (Oak will post-filter)
        if (pr.isNullRestriction()) {
            return new MatchAllDocsQuery();
        }

        // Determine property type from first/last/not value
        int propertyType = determinePropertyType(pr);

        switch (propertyType) {
            case javax.jcr.PropertyType.LONG:
                return createLongQuery(propertyName, pr);
            case javax.jcr.PropertyType.DOUBLE:
                return createDoubleQuery(propertyName, pr);
            case javax.jcr.PropertyType.DATE:
                return createDateQuery(propertyName, pr);
            case javax.jcr.PropertyType.BOOLEAN:
                return createBooleanQuery(propertyName, pr);
            default:
                return createStringQuery(propertyName, pr);
        }
    }

    private int determinePropertyType(Filter.PropertyRestriction pr) {
        org.apache.jackrabbit.oak.api.PropertyValue value = pr.first != null ? pr.first :
                          (pr.last != null ? pr.last : pr.not);
        if (value == null) {
            return javax.jcr.PropertyType.STRING;
        }
        return value.getType().tag();
    }

    // Abstracts the type-specific operations needed for numeric Point queries (Long and Double).
    private interface NumericPoint<T extends Number> {
        T convert(org.apache.jackrabbit.oak.api.PropertyValue pv);
        T nextAbove(T val);
        T nextBelow(T val);
        T min();
        T max();
        Query exact(String field, T val);
        Query range(String field, T lo, T hi);
        Query set(String field, List<org.apache.jackrabbit.oak.api.PropertyValue> list);
    }

    private static final NumericPoint<Long> LONG_POINT = new NumericPoint<Long>() {
        public Long convert(org.apache.jackrabbit.oak.api.PropertyValue pv) { return pv.getValue(org.apache.jackrabbit.oak.api.Type.LONG); }
        public Long nextAbove(Long v) { return v == Long.MAX_VALUE ? v : v + 1; }
        public Long nextBelow(Long v) { return v == Long.MIN_VALUE ? v : v - 1; }
        public Long min() { return Long.MIN_VALUE; }
        public Long max() { return Long.MAX_VALUE; }
        public Query exact(String f, Long v) { return org.apache.lucene.document.LongPoint.newExactQuery(f, v); }
        public Query range(String f, Long lo, Long hi) { return org.apache.lucene.document.LongPoint.newRangeQuery(f, lo, hi); }
        public Query set(String f, List<org.apache.jackrabbit.oak.api.PropertyValue> list) {
            long[] vals = list.stream().mapToLong(pv -> pv.getValue(org.apache.jackrabbit.oak.api.Type.LONG)).toArray();
            return org.apache.lucene.document.LongPoint.newSetQuery(f, vals);
        }
    };

    private static final NumericPoint<Double> DOUBLE_POINT = new NumericPoint<Double>() {
        public Double convert(org.apache.jackrabbit.oak.api.PropertyValue pv) { return pv.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE); }
        public Double nextAbove(Double v) { return Math.nextUp(v); }
        public Double nextBelow(Double v) { return Math.nextDown(v); }
        public Double min() { return -Double.MAX_VALUE; }
        public Double max() { return Double.MAX_VALUE; }
        public Query exact(String f, Double v) { return org.apache.lucene.document.DoublePoint.newExactQuery(f, v); }
        public Query range(String f, Double lo, Double hi) { return org.apache.lucene.document.DoublePoint.newRangeQuery(f, lo, hi); }
        public Query set(String f, List<org.apache.jackrabbit.oak.api.PropertyValue> list) {
            double[] vals = list.stream().mapToDouble(pv -> pv.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE)).toArray();
            return org.apache.lucene.document.DoublePoint.newSetQuery(f, vals);
        }
    };

    private <T extends Number> Query createNumericQuery(String propertyName,
            Filter.PropertyRestriction pr, NumericPoint<T> np) {
        T first = pr.first != null ? np.convert(pr.first) : null;
        T last  = pr.last  != null ? np.convert(pr.last)  : null;
        T not   = pr.not   != null ? np.convert(pr.not)   : null;

        if (first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            return np.exact(propertyName, first);
        } else if (first != null && last != null) {
            T lo = pr.firstIncluding ? first : np.nextAbove(first);
            T hi = pr.lastIncluding  ? last  : np.nextBelow(last);
            return np.range(propertyName, lo, hi);
        } else if (first != null) {
            T lo = pr.firstIncluding ? first : np.nextAbove(first);
            return np.range(propertyName, lo, np.max());
        } else if (last != null) {
            T hi = pr.lastIncluding ? last : np.nextBelow(last);
            return np.range(propertyName, np.min(), hi);
        } else if (pr.list != null) {
            return np.set(propertyName, pr.list);
        } else if (pr.isNot && not != null) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(new MatchAllDocsQuery(), Occur.MUST);
            bq.add(np.exact(propertyName, not), Occur.MUST_NOT);
            return bq.build();
        }
        throw new IllegalArgumentException("Unsupported property restriction: " + pr);
    }

    private Query createLongQuery(String propertyName, Filter.PropertyRestriction pr) {
        return createNumericQuery(propertyName, pr, LONG_POINT);
    }

    private Query createDoubleQuery(String propertyName, Filter.PropertyRestriction pr) {
        return createNumericQuery(propertyName, pr, DOUBLE_POINT);
    }

    private Query createDateQuery(String propertyName, Filter.PropertyRestriction pr) {
        // Dates are stored as Long (milliseconds since epoch)
        Long first = pr.first != null ? parseDateToMillis(pr.first) : null;
        Long last = pr.last != null ? parseDateToMillis(pr.last) : null;
        Long not = pr.not != null ? parseDateToMillis(pr.not) : null;

        Filter.PropertyRestriction longPr = new Filter.PropertyRestriction();
        longPr.propertyName = propertyName;
        longPr.first = first != null ? org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newLong(first) : null;
        longPr.last = last != null ? org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newLong(last) : null;
        longPr.not = not != null ? org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newLong(not) : null;
        longPr.firstIncluding = pr.firstIncluding;
        longPr.lastIncluding = pr.lastIncluding;
        longPr.isNot = pr.isNot;
        longPr.list = pr.list != null ?
            pr.list.stream().map(this::parseDateToMillis)
                .map(org.apache.jackrabbit.oak.plugins.memory.PropertyValues::newLong).collect(java.util.stream.Collectors.toList()) : null;

        return createLongQuery(propertyName, longPr);
    }

    private Long parseDateToMillis(org.apache.jackrabbit.oak.api.PropertyValue pv) {
        String dateStr = pv.getValue(org.apache.jackrabbit.oak.api.Type.DATE);
        try {
            return org.apache.jackrabbit.util.ISO8601.parse(dateStr).getTimeInMillis();
        } catch (Exception e) {
            LOG.error("Failed to parse date: " + dateStr, e);
            return 0L;
        }
    }

    private Query createBooleanQuery(String propertyName, Filter.PropertyRestriction pr) {
        Boolean first = pr.first != null ? pr.first.getValue(org.apache.jackrabbit.oak.api.Type.BOOLEAN) : null;
        Boolean not = pr.not != null ? pr.not.getValue(org.apache.jackrabbit.oak.api.Type.BOOLEAN) : null;

        if (pr.first != null && pr.first.equals(pr.last)) {
            // Equality: isActive = true
            String value = first.toString();
            return new TermQuery(new Term(propertyName, value));
        } else if (pr.isNot && not != null) {
            // NOT equal: isActive != true
            String value = not.toString();
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(new MatchAllDocsQuery(), Occur.MUST);
            bq.add(new TermQuery(new Term(propertyName, value)), Occur.MUST_NOT);
            return bq.build();
        }

        throw new IllegalArgumentException("Unsupported boolean restriction: " + pr);
    }

    private Query createStringQuery(String propertyName, Filter.PropertyRestriction pr) {
        String first = pr.first != null ? pr.first.getValue(org.apache.jackrabbit.oak.api.Type.STRING) : null;
        String last = pr.last != null ? pr.last.getValue(org.apache.jackrabbit.oak.api.Type.STRING) : null;
        String not = pr.not != null ? pr.not.getValue(org.apache.jackrabbit.oak.api.Type.STRING) : null;

        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            // Equality: title = 'Oak'
            return new TermQuery(new Term(propertyName, first));
        } else if (pr.first != null && pr.last != null) {
            // String range (lexicographic): title BETWEEN 'A' AND 'Z'
            return new TermRangeQuery(propertyName,
                new org.apache.lucene.util.BytesRef(first), new org.apache.lucene.util.BytesRef(last),
                pr.firstIncluding, pr.lastIncluding);
        } else if (pr.first != null) {
            // Lower bound: title >= 'M'
            return new TermRangeQuery(propertyName,
                new org.apache.lucene.util.BytesRef(first), null, pr.firstIncluding, true);
        } else if (pr.last != null) {
            // Upper bound: title <= 'Z'
            return new TermRangeQuery(propertyName,
                null, new org.apache.lucene.util.BytesRef(last), true, pr.lastIncluding);
        } else if (pr.list != null) {
            // IN query: title IN ('Oak', 'Pine', 'Elm')
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            for (org.apache.jackrabbit.oak.api.PropertyValue pv : pr.list) {
                String value = pv.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
                bq.add(new TermQuery(new Term(propertyName, value)), Occur.SHOULD);
            }
            return bq.build();
        } else if (pr.isNot && not != null) {
            // NOT equal: title != 'Draft'
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(new MatchAllDocsQuery(), Occur.MUST);
            bq.add(new TermQuery(new Term(propertyName, not)), Occur.MUST_NOT);
            return bq.build();
        }

        throw new IllegalArgumentException("Unsupported string restriction: " + pr);
    }

    /**
     * Handles localname() restrictions. Equality maps to a TermQuery; LIKE maps to
     * a WildcardQuery — both on the NODE_NAME StringField (namespace-stripped local name).
     * Mirrors LucenePropertyIndex.createNodeNameQuery().
     */
    private static Query createLocalNameQuery(Filter.PropertyRestriction pr) {
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            return new TermQuery(new Term(FieldNames.NODE_NAME,
                    pr.first.getValue(Type.STRING)));
        }
        if (pr.isLike && pr.first != null) {
            String like = pr.first.getValue(Type.STRING);
            // Convert SQL LIKE wildcards (% → *, _ → ?) to Lucene wildcard syntax
            String luceneWild = like.replace("%", "*").replace("_", "?");
            return new WildcardQuery(new Term(FieldNames.NODE_NAME, luceneWild));
        }
        return null;
    }

    /**
     * Converts a FullTextExpression to a Lucene Query using visitor pattern.
     * Based on legacy LuceneIndex implementation.
     */
    private static Query getFullTextQuery(FullTextExpression ft, final Analyzer analyzer) {
        final AtomicReference<Query> result = new AtomicReference<>();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextContains contains) {
                return contains.getBase().accept(this);
            }

            @Override
            public boolean visit(FullTextOr or) {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                for (FullTextExpression e : or.list) {
                    Query x = getFullTextQuery(e, analyzer);
                    bq.add(x, Occur.SHOULD);
                }
                result.set(bq.build());
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                for (FullTextExpression e : and.list) {
                    Query x = getFullTextQuery(e, analyzer);
                    bq.add(x, Occur.MUST);
                }
                result.set(bq.build());
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                String propertyName = term.getPropertyName();
                String text = term.getText();
                Query q = tokenToQuery(text, propertyName, analyzer);
                if (q == null) {
                    return true;
                }
                if (term.isNot()) {
                    BooleanQuery.Builder bq = new BooleanQuery.Builder();
                    bq.add(new MatchAllDocsQuery(), Occur.MUST);
                    bq.add(q, Occur.MUST_NOT);
                    q = bq.build();
                }
                String boostStr = term.getBoost();
                if (boostStr != null) {
                    try {
                        q = new BoostQuery(q, Float.parseFloat(boostStr));
                    } catch (NumberFormatException e) {
                        LOG.warn("Ignoring unparseable boost value '{}' on fulltext term", boostStr);
                    }
                }
                result.set(q);
                return true;
            }
        });
        return result.get();
    }

    /**
     * Tokenizes text and builds appropriate Lucene query (TermQuery, PhraseQuery,
     * PrefixQuery, or WildcardQuery). Wildcard terms bypass tokenization.
     */
    private static Query tokenToQuery(String text, String fieldName, Analyzer analyzer) {
        String field = (fieldName == null || "*".equals(fieldName))
            ? FieldNames.FULLTEXT
            : fieldName;

        // Wildcard/prefix: bypass tokenization to preserve wildcard characters
        if (text.contains("*") || text.contains("?")) {
            String lower = text.toLowerCase(Locale.ENGLISH);
            // Pure trailing-star prefix (no other wildcards): use PrefixQuery
            if (lower.endsWith("*")
                    && lower.indexOf('*') == lower.length() - 1
                    && !lower.contains("?")) {
                return new PrefixQuery(new Term(field, lower.substring(0, lower.length() - 1)));
            }
            return new WildcardQuery(new Term(field, lower));
        }

        List<String> tokens = tokenize(text, analyzer);
        if (tokens.isEmpty()) {
            return new BooleanQuery.Builder().build();
        }
        if (tokens.size() == 1) {
            return new TermQuery(new Term(field, tokens.get(0)));
        }
        PhraseQuery.Builder pq = new PhraseQuery.Builder();
        for (String token : tokens) {
            pq.add(new Term(field, token));
        }
        return pq.build();
    }

    /**
     * Tokenizes text using the analyzer.
     * Based on legacy LuceneIndex implementation.
     */
    private static List<String> tokenize(String text, Analyzer analyzer) {
        List<String> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(FieldNames.FULLTEXT, new StringReader(text))) {
            CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                tokens.add(termAtt.toString());
            }
            stream.end();
        } catch (IOException e) {
            LOG.error("Failed to tokenize text: " + text, e);
        }
        return tokens;
    }

    // ===== AdvancedQueryIndex methods =====

    @Override
    @org.jetbrains.annotations.Nullable
    public NodeAggregator getNodeAggregator() {
        // No aggregation support yet
        return null;
    }

    @Override
    public List<QueryIndex.IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
        // Don't offer a plan when the index has not yet been populated (no data)
        LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
        if (indexNode == null) {
            return Collections.emptyList();
        }
        try {
        return getPlansInternal(filter, sortOrder, rootState, indexNode);
        } finally {
            indexNode.release();
        }
    }

    private List<QueryIndex.IndexPlan> getPlansInternal(Filter filter, List<OrderEntry> sortOrder,
            NodeState rootState, LuceneNgIndexNode indexNode) {
        // Check if we can handle this query
        FullTextExpression ft = filter.getFullTextConstraint();
        List<Filter.PropertyRestriction> propRestrictions = new ArrayList<>(filter.getPropertyRestrictions());

        // Remove function restrictions (e.g. "function*@:localname") — we don't support
        // function-based indexes yet; these restrictions are never satisfied by our index
        // and must not be counted as "supported" constraints or included in the Lucene query.
        propRestrictions.removeIf(pr -> pr.propertyName != null
                && pr.propertyName.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX));

        // localname() restriction: only offer a plan when the indexing rule declares
        // indexNodeName=true (mirrors FulltextIndexPlanner.canEvalNodeNameRestriction).
        Filter.PropertyRestriction localNamePr = filter.getPropertyRestriction(QueryConstants.RESTRICTION_LOCAL_NAME);
        if (localNamePr != null) {
            String nodeType = filter.getNodeType();
            IndexingRule rule = nodeType != null
                    ? indexNode.getDefinition().getApplicableIndexingRule(nodeType) : null;
            if (rule == null || !rule.isNodeNameIndexed()) {
                return Collections.emptyList();
            }
            // Remove from the generic list — it is handled as a special case
            propRestrictions.removeIf(pr -> QueryConstants.RESTRICTION_LOCAL_NAME.equals(pr.propertyName));
        }

        // Extract facet fields before the early-exit guard so facet-only queries are handled
        List<String> facetFields = extractFacetFields(filter);

        // Offer a plan when there is at least one constraint we can evaluate:
        // fulltext, property restriction, facet, localname(), or a declared node-type
        // restriction that the index actually covers.
        boolean hasLocalNameConstraint = localNamePr != null;
        boolean noContentConstraints = ft == null && propRestrictions.isEmpty()
                && facetFields.isEmpty() && !hasLocalNameConstraint;
        if (noContentConstraints) {
            if (filter.matchesAllTypes()) {
                // No constraints at all — skip
                return Collections.emptyList();
            }
            // Node-type-only query: only offer a plan when the index has a rule for
            // the queried type. This prevents us from winning queries like
            // SELECT * FROM [cq:Page]... when the index only covers dam:Asset nodes.
            String nodeType = filter.getNodeType();
            if (nodeType == null
                    || indexNode.getDefinition().getApplicableIndexingRule(nodeType) == null) {
                return Collections.emptyList();
            }
        }

        // Calculate cost
        double cost = getCost(filter, rootState);
        if (cost == Double.POSITIVE_INFINITY) {
            return Collections.emptyList();
        }

        // Create index plan
        QueryIndex.IndexPlan.Builder builder = new QueryIndex.IndexPlan.Builder();
        builder.setCostPerExecution(cost);
        builder.setCostPerEntry(0.1); // Low per-entry cost
        builder.setEstimatedEntryCount(100); // Estimate
        builder.setFilter(filter);
        builder.setDelayed(false); // Synchronous index
        // Facet columns are served by the fulltext index path even without jcr:contains.
        builder.setFulltextIndex(ft != null || !facetFields.isEmpty());
        if (!facetFields.isEmpty()) {
            builder.setAttribute(ATTR_FACET_FIELDS, facetFields);
            LOG.debug("Facet fields requested: {}", facetFields);
        }

        // Set sort order if we can support it
        if (sortOrder != null && !sortOrder.isEmpty()) {
            builder.setSortOrder(sortOrder);
        }

        builder.setDefinition(getDefinitionBuilder(rootState, indexPath).getNodeState());
        builder.setPathPrefix(indexPath);
        builder.setPlanName(indexPath);

        return Collections.singletonList(builder.build());
    }

    @Override
    public String getPlanDescription(QueryIndex.IndexPlan plan, NodeState root) {
        // First line must start with "lucene:" so tooling that only matches legacy FulltextIndex
        // plans (e.g. AEM ExplainQueryServlet LUCENE_INDEX_PATTERN: "/\* lucene:…") still detects an
        // index. "@v9" suffix marks Lucene 9 / Oak type lucene9 in the captured index label;
        // "lucene9:" on the next line keeps the engine explicit for logs and tests.
        String shortName = PathUtils.getName(indexPath);
        StringBuilder sb = new StringBuilder("lucene:");
        sb.append(shortName).append("@v9\n");
        sb.append("lucene9:").append(shortName).append("\n");
        sb.append("    indexDefinition: ").append(indexPath).append("\n");
        sb.append("    estimatedEntries: ").append(plan.getEstimatedEntryCount()).append("\n");

        Filter filter = plan.getFilter();
        if (filter != null) {
            sb.append("    luceneQuery: ").append(buildQuery(filter).toString()).append("\n");
            List<OrderEntry> sortOrder = plan.getSortOrder();
            if (sortOrder != null && !sortOrder.isEmpty()) {
                sb.append("    sortOrder: ").append(sortOrder).append("\n");
            }
            FullTextExpression ft = filter.getFullTextConstraint();
            if (ft != null) {
                sb.append("    fulltextCondition: ").append(ft).append("\n");
            }
            List<Filter.PropertyRestriction> propRestrictions = new ArrayList<>(filter.getPropertyRestrictions());
            if (!propRestrictions.isEmpty()) {
                sb.append("    propertyRestrictions: ").append(propRestrictions.size()).append("\n");
            }
        }

        return sb.toString();
    }

    @Override
    public Cursor query(QueryIndex.IndexPlan plan, NodeState rootState) {
        // Extract filter and sort order from plan
        Filter filter = plan.getFilter();
        List<OrderEntry> sortOrder = plan.getSortOrder();

        @SuppressWarnings("unchecked")
        List<String> facetFields = (List<String>) plan.getAttribute(ATTR_FACET_FIELDS);

        Query query = buildQuery(filter);
        LOG.debug("Executing query: {}", query);

        Sort sort = null;
        Map<String, String> facetColumns = Collections.emptyMap();
        boolean needsExcerpts = filter.getFullTextConstraint() != null;

        // Facets (and the sort they may need) are computed once in a bounded, self-contained
        // acquire — this does NOT leak the index node into row iteration, which pages
        // independently inside the cursor. Sort-only queries acquire once just to build the Sort.
        if (facetFields != null && !facetFields.isEmpty()) {
            LuceneNgIndexNode facetNode = tracker.acquireIndexNode(indexPath);
            if (facetNode == null) {
                LOG.warn("Index node not found or not yet populated: {}", indexPath);
                return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
            }
            try {
                IndexSearcher facetSearcher = facetNode.getSearcher();
                LuceneNgIndexDefinition definition = facetNode.getDefinition();
                SecureFacetConfiguration secureFacetConfiguration = definition.getSecureFacetConfiguration();
                if (sortOrder != null && !sortOrder.isEmpty()) {
                    sort = createSort(sortOrder, definition, facetSearcher.getIndexReader());
                }
                FacetsCollector fc = new FacetsCollector();
                // limit=1: we only need FacetsCollector's side effect (it aggregates over every
                // matching doc during the search regardless of this number); the returned TopDocs
                // is discarded — row iteration re-runs its own bounded, batched search independently.
                if (sort == null) {
                    FacetsCollector.search(facetSearcher, query, 1, fc);
                } else {
                    FacetsCollector.search(facetSearcher, query, 1, sort, fc);
                }
                Map<String, Facets> facetsMap = new HashMap<>();
                for (String facetField : facetFields) {
                    try {
                        String luceneFieldName = FieldNames.createFacetFieldName(facetField);
                        DefaultSortedSetDocValuesReaderState state =
                            facetNode.getFacetReaderState(luceneFieldName);
                        Facets facetsImpl;
                        switch (secureFacetConfiguration.getMode()) {
                            case INSECURE:
                                facetsImpl = new SortedSetDocValuesFacetCounts(state, fc);
                                break;
                            case STATISTICAL:
                                facetsImpl = new LuceneNgStatisticalSortedSetDocValuesFacetCounts(
                                        state, fc, filter, secureFacetConfiguration);
                                break;
                            case SECURE:
                            default:
                                facetsImpl = new LuceneNgSecureSortedSetDocValuesFacetCounts(state, fc, filter);
                                break;
                        }
                        facetsMap.put(facetField, facetsImpl);
                    } catch (IllegalArgumentException e) {
                        LOG.debug("Facet field not indexed: {}", facetField);
                    }
                }
                facetColumns = buildFacetColumnsEagerly(facetsMap, definition.getNumberOfTopFacets());
            } catch (IOException e) {
                LOG.error("Error computing facets on index: " + indexPath, e);
            } finally {
                facetNode.release();
            }
        } else if (sortOrder != null && !sortOrder.isEmpty()) {
            LuceneNgIndexNode sortNode = tracker.acquireIndexNode(indexPath);
            if (sortNode != null) {
                try {
                    sort = createSort(sortOrder, sortNode.getDefinition(),
                            sortNode.getSearcher().getIndexReader());
                } finally {
                    sortNode.release();
                }
            }
        }

        // Excerpts are generated per batch inside the cursor; the analyzer is owned and closed
        // by the cursor. StandardAnalyzer mirrors the previous eager excerpt generation.
        Analyzer excerptAnalyzer = needsExcerpts ? new StandardAnalyzer() : null;
        return new LuceneNgCursor(tracker, indexPath, query, sort, facetColumns, needsExcerpts, excerptAnalyzer);
    }

    /**
     * Builds the {@code rep:facet(dim) -> JSON} column map from a computed {@link Facets} per
     * dimension. Mirrors {@code LuceneNgCursor.buildFacetColumns}; extracted here because the lazy
     * cursor now receives the already-built column map rather than live {@link Facets} objects
     * (which reference a searcher that is released before row iteration begins).
     */
    private static Map<String, String> buildFacetColumnsEagerly(Map<String, Facets> facetsMap, int topChildren) {
        if (facetsMap == null || facetsMap.isEmpty()) {
            return Collections.emptyMap();
        }
        int facetTopChildren = Math.max(1, topChildren);
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, Facets> entry : facetsMap.entrySet()) {
            String dimension = entry.getKey();
            try {
                String luceneFieldName = FieldNames.createFacetFieldName(dimension);
                org.apache.lucene.facet.FacetResult fr = entry.getValue().getTopChildren(facetTopChildren, dimension);
                if (fr == null || fr.labelValues == null) {
                    fr = entry.getValue().getTopChildren(facetTopChildren, luceneFieldName);
                }
                if (fr != null && fr.labelValues != null) {
                    org.apache.jackrabbit.oak.commons.json.JsopBuilder json =
                            new org.apache.jackrabbit.oak.commons.json.JsopBuilder();
                    json.object();
                    for (org.apache.lucene.facet.LabelAndValue lv : fr.labelValues) {
                        json.key(lv.label);
                        json.value(lv.value.intValue());
                    }
                    json.endObject();
                    result.put(QueryConstants.REP_FACET + "(" + dimension + ")", json.toString());
                }
            } catch (IOException e) {
                LOG.error("Failed to build facets for {}: {}", dimension, e.getMessage());
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Creates Lucene Sort from Oak OrderEntry list.
     * Based on legacy LuceneIndex implementation.
     */
    private Sort createSort(List<OrderEntry> sortOrder, LuceneNgIndexDefinition definition, IndexReader reader) {
        if (sortOrder == null || sortOrder.isEmpty()) {
            return null;
        }

        List<SortField> fields = new ArrayList<>();
        for (OrderEntry order : sortOrder) {
            SortField sf = createSortField(order, definition, reader);
            if (sf != null) {
                fields.add(sf);
            }
        }

        return new Sort(fields.toArray(new SortField[0]));
    }

    private SortField createSortField(OrderEntry order, LuceneNgIndexDefinition definition, IndexReader reader) {
        String propertyName = order.getPropertyName();

        // Special case: sort by relevance score
        if ("jcr:score".equals(propertyName)) {
            return SortField.FIELD_SCORE;
        }

        // Look up property type from index definition
        int propertyType = getPropertyTypeFromDefinition(definition, propertyName, order.getPropertyType().tag());

        // Determine sort field type based on property type
        SortField.Type fieldType = getSortFieldType(propertyType);

        // Create sort field (reverse = descending order)
        boolean reverse = (order.getOrder() == OrderEntry.Order.DESCENDING);

        // Whether a property is single- or multi-valued is a per-document, data-level fact
        // (PropertyState.isArray() on the write side), not something declared statically in
        // the index config's PropertyDefinition -- that class has no multi-valuedness flag.
        // So instead of asking the config, ask the index itself: a multi-valued string/boolean
        // property is written (see LuceneNgIndexEditor) as a SortedSetDocValuesField, which
        // requires a SortedSetSortField to sort on (a plain SortField only works against
        // SORTED doc-values and throws IllegalStateException against SORTED_SET).
        if (fieldType == SortField.Type.STRING && isMultiValuedDocValuesField(reader, propertyName)) {
            return new SortedSetSortField(propertyName, reverse);
        }

        return new SortField(propertyName, fieldType, reverse);
    }

    /**
     * Determines whether {@code propertyName} was indexed with {@code SORTED_SET} doc-values
     * (i.e. as a {@code SortedSetDocValuesField}, used for multi-valued properties) rather than
     * plain {@code SORTED} doc-values (single-valued). Returns {@code false} when the field has
     * no doc-values at all (e.g. not yet indexed, or not ordered).
     */
    private boolean isMultiValuedDocValuesField(IndexReader reader, String propertyName) {
        if (reader == null) {
            return false;
        }
        FieldInfo fieldInfo = FieldInfos.getMergedFieldInfos(reader).fieldInfo(propertyName);
        return fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET;
    }

    /**
     * Gets the property type from the index definition, falling back to the provided type.
     * Based on legacy LucenePropertyIndex.getPropertyType.
     */
    private int getPropertyTypeFromDefinition(LuceneNgIndexDefinition definition, String propertyName, int fallbackType) {
        // Try to find property definition in index rules
        for (IndexingRule rule : definition.getDefinedRules()) {
            org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition propDef = rule.getConfig(propertyName);
            if (propDef != null && propDef.index) {
                return propDef.getType();
            }
        }
        // Fall back to type from OrderEntry
        return fallbackType;
    }

    private SortField.Type getSortFieldType(int propertyType) {
        switch (propertyType) {
            case PropertyType.LONG:
            case PropertyType.DATE:
                return SortField.Type.LONG;
            case PropertyType.DOUBLE:
                return SortField.Type.DOUBLE;
            case PropertyType.BOOLEAN:
            case PropertyType.STRING:
            default:
                return SortField.Type.STRING;
        }
    }

    /**
     * Navigates to the index definition node from the root state.
     * Example: indexPath="/oak:index/myIndex" returns builder for that node.
     */
    private NodeBuilder getDefinitionBuilder(NodeState rootState, String indexPath) {
        NodeBuilder builder = rootState.builder();

        // Remove leading slash if present
        String path = indexPath.startsWith("/") ? indexPath.substring(1) : indexPath;

        // Navigate through path segments
        String[] segments = path.split("/");
        for (String segment : segments) {
            builder = builder.child(segment);
        }

        return builder;
    }

    /**
     * Extracts facet property names from Filter.
     * Oak can expose facet requests either as {@code rep:facet -> rep:facet(x)} pseudo
     * restrictions or directly as a property name shaped like {@code rep:facet(x)}.
     */
    private List<String> extractFacetFields(Filter filter) {
        List<String> facetFields = new ArrayList<>();
        for (Filter.PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String propName = pr.propertyName;
            addFacetFieldIfPresent(facetFields, propName);

            if (QueryConstants.REP_FACET.equals(propName)) {
                if (pr.first != null) {
                    addFacetFieldIfPresent(facetFields, pr.first.getValue(org.apache.jackrabbit.oak.api.Type.STRING));
                }
                if (pr.last != null) {
                    addFacetFieldIfPresent(facetFields, pr.last.getValue(org.apache.jackrabbit.oak.api.Type.STRING));
                }
                if (pr.list != null) {
                    for (PropertyValue candidate : pr.list) {
                        if (candidate != null) {
                            addFacetFieldIfPresent(facetFields, candidate.getValue(org.apache.jackrabbit.oak.api.Type.STRING));
                        }
                    }
                }
            }
        }
        // SQL2/XPath parsers may not always expose rep:facet(...) as a property restriction.
        addFacetFieldsFromQueryStatement(facetFields, filter.getQueryStatement());
        return facetFields;
    }

    private static void addFacetFieldIfPresent(List<String> facetFields, String expression) {
        if (expression == null) {
            return;
        }
        String prefix = QueryConstants.REP_FACET + "(";
        if (!expression.startsWith(prefix) || !expression.endsWith(")")) {
            return;
        }
        String facetField = expression.substring(prefix.length(), expression.length() - 1).trim();
        if (!facetField.isEmpty() && !facetFields.contains(facetField)) {
            facetFields.add(facetField);
        }
    }

    private static void addFacetFieldsFromQueryStatement(List<String> facetFields, String statement) {
        if (statement == null || statement.isEmpty()) {
            return;
        }
        String token = QueryConstants.REP_FACET + "(";
        int from = 0;
        while (from < statement.length()) {
            int start = statement.indexOf(token, from);
            if (start < 0) {
                return;
            }
            int end = statement.indexOf(')', start + token.length());
            if (end < 0) {
                return;
            }
            String field = statement.substring(start + token.length(), end).trim();
            if (!field.isEmpty() && !facetFields.contains(field)) {
                facetFields.add(field);
            }
            from = end + 1;
        }
    }
}
