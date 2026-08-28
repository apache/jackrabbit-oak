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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.cursor.Cursors;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.SizeEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgCursor;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexNode;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgSecureSortedSetDocValuesFacetCounts;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgStatisticalSortedSetDocValuesFacetCounts;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
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
import org.apache.lucene.search.TotalHitCountCollector;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Lucene 9 query index implementation.
 * Executes queries against Lucene 9 indexes.
 */
public class LuceneNgIndex extends FulltextIndex {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndex.class);
    // Must equal FulltextIndexPlanner.ATTR_FACET_FIELDS — the inherited FulltextIndexPlanner
    // sets facet fields on the plan under this key; query(IndexPlan) reads them back.
    private static final String ATTR_FACET_FIELDS = "oak.facet.fields";

    private final LuceneNgIndexTracker tracker;
    private final String indexPath;

    public LuceneNgIndex(LuceneNgIndexTracker tracker, String indexPath) {
        this.tracker = tracker;
        this.indexPath = indexPath;
    }

    // ===== FulltextIndex abstract hooks =====
    // Cost estimation and plan building come from the inherited FulltextIndexPlanner, which
    // only offers a plan for properties this index actually declares, matching
    // LucenePropertyIndex and ElasticIndex. getCost(Filter,...), getPlan(Filter,...) and
    // query(Filter,...) are unsupported here (inherited default throws).

    @Override
    protected LuceneNgIndexNode acquireIndexNode(String indexPath) {
        return tracker.acquireIndexNode(indexPath);
    }

    @Override
    protected LuceneNgIndexNode acquireIndexNode(IndexPlan plan) {
        return (LuceneNgIndexNode) super.acquireIndexNode(plan);
    }

    @Override
    protected String getType() {
        return LuceneNgIndexConstants.TYPE_LUCENE9;
    }

    @Override
    public String getIndexName() {
        return LuceneNgIndexConstants.TYPE_LUCENE9;
    }

    @Override
    protected SizeEstimator getSizeEstimator(IndexPlan plan) {
        // Port of LucenePropertyIndex.getSizeEstimator: a bounded count-only search over the
        // plan's built query. Builds the query via buildQuery(plan.getFilter(), getPlanResult(plan)),
        // the same PlanResult-driven construction the executed query uses. Note: LuceneNg's
        // query(IndexPlan,...) returns its own LuceneNgCursor, which supplies its own size, so this
        // estimator is not on the hot path today — but the hook is abstract and must be implemented
        // correctly.
        return () -> {
            LuceneNgIndexNode indexNode = acquireIndexNode(plan);
            if (indexNode == null) {
                return -1L;
            }
            try {
                IndexSearcher searcher = indexNode.getSearcher();
                if (searcher == null) {
                    return -1L;
                }
                Query query = buildQuery(plan.getFilter(), getPlanResult(plan));
                TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(query, collector);
                int totalHits = collector.getTotalHits();
                LOG.debug("Estimated size for query {} is {}", query, totalHits);
                return (long) totalHits;
            } catch (IOException e) {
                LOG.warn("Size-estimate query failed on index {}", indexPath, e);
                return -1L;
            } finally {
                indexNode.release();
            }
        };
    }

    @Override
    protected Predicate<NodeState> getIndexDefinitionPredicate() {
        return state -> LuceneNgIndexConstants.TYPE_LUCENE9.equals(
                state.getString(IndexConstants.TYPE_PROPERTY_NAME));
    }

    @Override
    protected String getFulltextRequestString(IndexPlan plan, IndexNode indexNode, NodeState rootState) {
        // The diagnostic representation of the query this plan would run — the same Lucene
        // Query buildQuery(...) constructs for execution.
        return buildQuery(plan.getFilter(), getPlanResult(plan)).toString();
    }

    @Override
    protected boolean filterReplacedIndexes() {
        return false; // matches this module's current behavior — no blue/green mount-info concept yet
    }

    @Override
    protected boolean runIsActiveIndexCheck() {
        return false; // matches ElasticIndex's choice; LuceneNg has no active-index-check concept yet
    }

    private Query buildQuery(Filter filter, FulltextIndexPlanner.PlanResult planResult) {
        FullTextExpression ft = filter.getFullTextConstraint();

        // Strip rep:facet pseudo-restrictions and function restrictions we don't index.
        // Function restrictions (e.g. "function*@:localname") are paired with their dedicated
        // equivalents (e.g. ":localname") and are handled by createPropertyQuery(); including
        // them as separate clauses would produce a term query on a non-existent field.
        //
        // A property restriction only becomes a Lucene clause when the planner validated it —
        // matching LucenePropertyIndex.addNonFullTextConstraints, which skips any restriction
        // whose planResult.getPropDefn(pr) is null (undeclared/unindexed property) and leaves it
        // for the query engine to post-filter instead. The localname() pseudo-restriction has no
        // declared PropertyDefinition, so it is gated on evaluateNodeNameRestriction() instead,
        // exactly as legacy does.
        List<Filter.PropertyRestriction> propRestrictions = filter.getPropertyRestrictions()
            .stream()
            .filter(pr -> !QueryConstants.REP_FACET.equals(pr.propertyName))
            .filter(pr -> pr.propertyName == null
                    || !pr.propertyName.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX))
            .filter(pr -> isPlannerValidated(pr, planResult))
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
     * Decides whether a property restriction may be turned into a Lucene query clause, driven by
     * the {@link FulltextIndexPlanner.PlanResult} the planner already built and attached to the plan
     * (rather than re-deciding from the raw {@link Filter}). Mirrors
     * {@code LucenePropertyIndex.addNonFullTextConstraints}:
     * <ul>
     *   <li>the {@code localname()} pseudo-restriction is retained only when the planner marked the
     *       node-name restriction as evaluable ({@link FulltextIndexPlanner.PlanResult#evaluateNodeNameRestriction()});</li>
     *   <li>every other property restriction is retained only when the planner matched it to a
     *       declared/indexed property ({@link FulltextIndexPlanner.PlanResult#getPropDefn} is non-null) —
     *       restrictions on undeclared properties are dropped here and left for the query engine to
     *       post-filter, exactly as legacy does.</li>
     * </ul>
     */
    private static boolean isPlannerValidated(Filter.PropertyRestriction pr,
                                              FulltextIndexPlanner.PlanResult planResult) {
        // In real query execution the plan is always built by the inherited FulltextIndexPlanner,
        // so getPlanResult(plan) is non-null (the same assumption LucenePropertyIndex makes). A null
        // PlanResult only arises for lower-level building-block callers that construct a plan without
        // going through the planner (e.g. mock-plan unit tests). With no planner decision to be
        // consistent with, there is nothing to gate on, so we retain the restriction — i.e. fall back
        // to the pre-D3 "derive every constraint from the raw Filter" behavior.
        if (planResult == null) {
            return true;
        }
        if (QueryConstants.RESTRICTION_LOCAL_NAME.equals(pr.propertyName)) {
            return planResult.evaluateNodeNameRestriction();
        }
        return planResult.getPropDefn(pr) != null;
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
        // Property-scoped fulltext (CONTAINS(propertyName, ...)) resolves to the analyzed field
        // written by LuceneNgDocumentMaker#indexAnalyzedProperty for that property, not to the
        // raw property name (nothing is ever indexed under the literal property name here).
        String field = (fieldName == null || "*".equals(fieldName))
            ? FieldNames.FULLTEXT
            : FieldNames.createAnalyzedFieldName(fieldName);

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
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        // Kept as an override (rather than inheriting FulltextIndex.getPlanDescription) purely for
        // output-format compatibility that LuceneNgIndexComparisonTest.testLuceneNgIndexIsUsed pins:
        //  - the first line must start with "lucene:" so tooling that only matches legacy
        //    FulltextIndex plans (e.g. AEM ExplainQueryServlet LUCENE_INDEX_PATTERN: "/\* lucene:…")
        //    still detects an index; the "@v9" suffix marks Lucene 9 / Oak type lucene9;
        //  - the "lucene9:" line keeps the engine explicit for logs/tests;
        //  - the query label is "luceneQuery:" (not the base's "<type>Query:" = "lucene9Query:").
        // The path is now taken from the plan's PlanResult (built by the inherited
        // FulltextIndexPlanner) rather than a per-instance field, so it is correct even if this
        // instance was allocated for a different index path.
        String path = getPlanResult(plan).indexPath;
        String shortName = PathUtils.getName(path);
        StringBuilder sb = new StringBuilder("lucene:");
        sb.append(shortName).append("@v9\n");
        sb.append("lucene9:").append(shortName).append("\n");
        sb.append("    indexDefinition: ").append(path).append("\n");
        sb.append("    estimatedEntries: ").append(plan.getEstimatedEntryCount()).append("\n");

        Filter filter = plan.getFilter();
        if (filter != null) {
            sb.append("    luceneQuery: ").append(buildQuery(filter, getPlanResult(plan)).toString()).append("\n");
            List<OrderEntry> sortOrder = plan.getSortOrder();
            if (sortOrder != null && !sortOrder.isEmpty()) {
                sb.append("    sortOrder: ").append(sortOrder).append("\n");
            }
            FullTextExpression ft = filter.getFullTextConstraint();
            if (ft != null) {
                sb.append("    fulltextCondition: ").append(ft).append("\n");
            }
            int propRestrictionCount = filter.getPropertyRestrictions().size();
            if (propRestrictionCount > 0) {
                sb.append("    propertyRestrictions: ").append(propRestrictionCount).append("\n");
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

        Query query = buildQuery(filter, getPlanResult(plan));
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
        // by the cursor.
        Analyzer excerptAnalyzer = needsExcerpts ? new StandardAnalyzer() : null;
        return new LuceneNgCursor(tracker, indexPath, query, sort, facetColumns, needsExcerpts, excerptAnalyzer);
    }

    /**
     * Builds the {@code rep:facet(dim) -> JSON} column map from a computed {@link Facets} per
     * dimension. Built here, before the cursor is constructed, because the underlying
     * {@link Facets} reference a searcher that is released before row iteration begins.
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

}
