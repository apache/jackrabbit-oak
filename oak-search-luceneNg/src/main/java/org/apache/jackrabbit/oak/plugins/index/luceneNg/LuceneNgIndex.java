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
import org.apache.jackrabbit.oak.plugins.index.cursor.Cursors;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
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

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        FullTextExpression ft = filter.getFullTextConstraint();
        List<Filter.PropertyRestriction> propRestrictions = new ArrayList<>(filter.getPropertyRestrictions());

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
        try {
            LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
            if (indexNode == null) {
                LOG.warn("Index node not found: {}", indexPath);
                return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
            }

            // Get definition builder from rootState for reading index data
            // Navigate to the index definition node (e.g., /oak:index/luceneNgTestIndex)
            NodeBuilder definitionBuilder = getDefinitionBuilder(rootState, indexPath);

            // Get searcher - pass definition builder so OakDirectory can access :data child node
            IndexSearcherHolder holder = new IndexSearcherHolder(
                definitionBuilder,
                indexNode.getDefinition().getIndexName()
            );
            IndexSearcher searcher = holder.getSearcher();

            // Build Lucene query from filter
            Query query = buildQuery(filter);
            LOG.debug("Executing query: {}", query);

            // Execute query
            TopDocs docs = searcher.search(query, 100); // Limit to 100 for now
            LOG.debug("Found {} hits", docs.totalHits);

            // Return cursor
            return new LuceneNgCursor(docs, searcher, holder);

        } catch (IOException e) {
            LOG.error("Error executing query on index: " + indexPath, e);
            return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
        }
    }

    private Query buildQuery(Filter filter) {
        FullTextExpression ft = filter.getFullTextConstraint();

        // Strip rep:facet pseudo-restrictions — they are not real query constraints
        List<Filter.PropertyRestriction> propRestrictions = filter.getPropertyRestrictions()
            .stream()
            .filter(pr -> !QueryConstants.REP_FACET.equals(pr.propertyName))
            .collect(Collectors.toList());

        // If there are no real constraints, match all documents
        if (ft == null && propRestrictions.isEmpty()) {
            return new MatchAllDocsQuery();
        }

        // Handle full-text queries
        if (ft != null) {
            Analyzer analyzer = new StandardAnalyzer();
            Query ftQuery = getFullTextQuery(ft, analyzer);
            LOG.debug("Building full-text query: {}", ftQuery);

            // Combine with property restrictions if present
            if (!propRestrictions.isEmpty()) {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                bq.add(ftQuery, Occur.MUST);
                for (Filter.PropertyRestriction pr : propRestrictions) {
                    Query propQuery = createPropertyQuery(pr);
                    if (propQuery != null) {
                        bq.add(propQuery, Occur.MUST);
                    }
                }
                return bq.build();
            }
            return ftQuery;
        }

        // Handle property restriction queries only
        if (propRestrictions.size() == 1) {
            Query q = createPropertyQuery(propRestrictions.get(0));
            return q != null ? q : new MatchAllDocsQuery();
        }

        // Multiple property restrictions - combine with AND
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (Filter.PropertyRestriction pr : propRestrictions) {
            Query propQuery = createPropertyQuery(pr);
            if (propQuery != null) {
                bq.add(propQuery, Occur.MUST);
            }
        }
        return bq.build();
    }

    /**
     * Creates a Lucene Query for a property restriction.
     * Handles equality, range, NOT NULL, NULL, NOT, and IN queries.
     * Based on legacy LuceneIndex pattern.
     */
    private Query createPropertyQuery(Filter.PropertyRestriction pr) {
        String propertyName = pr.propertyName;

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

    private Query createLongQuery(String propertyName, Filter.PropertyRestriction pr) {
        Long first = pr.first != null ? pr.first.getValue(org.apache.jackrabbit.oak.api.Type.LONG) : null;
        Long last = pr.last != null ? pr.last.getValue(org.apache.jackrabbit.oak.api.Type.LONG) : null;
        Long not = pr.not != null ? pr.not.getValue(org.apache.jackrabbit.oak.api.Type.LONG) : null;

        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            // Equality: age = 25
            return org.apache.lucene.document.LongPoint.newExactQuery(propertyName, first);
        } else if (pr.first != null && pr.last != null) {
            // Range with both bounds: age BETWEEN 10 AND 100
            long lowerValue = pr.firstIncluding ? first : Math.addExact(first, 1);
            long upperValue = pr.lastIncluding ? last : Math.addExact(last, -1);
            return org.apache.lucene.document.LongPoint.newRangeQuery(propertyName, lowerValue, upperValue);
        } else if (pr.first != null) {
            // Lower bound only: age >= 25 or age > 25
            long lowerValue = pr.firstIncluding ? first : Math.addExact(first, 1);
            return org.apache.lucene.document.LongPoint.newRangeQuery(propertyName, lowerValue, Long.MAX_VALUE);
        } else if (pr.last != null) {
            // Upper bound only: age <= 50 or age < 50
            long upperValue = pr.lastIncluding ? last : Math.addExact(last, -1);
            return org.apache.lucene.document.LongPoint.newRangeQuery(propertyName, Long.MIN_VALUE, upperValue);
        } else if (pr.list != null) {
            // IN query: age IN (10, 20, 30)
            long[] values = pr.list.stream()
                .map(pv -> pv.getValue(org.apache.jackrabbit.oak.api.Type.LONG))
                .mapToLong(Long::longValue)
                .toArray();
            return org.apache.lucene.document.LongPoint.newSetQuery(propertyName, values);
        } else if (pr.isNot && not != null) {
            // NOT equal: age != 25
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(new MatchAllDocsQuery(), Occur.MUST);
            bq.add(org.apache.lucene.document.LongPoint.newExactQuery(propertyName, not), Occur.MUST_NOT);
            return bq.build();
        }

        throw new IllegalArgumentException("Unsupported property restriction: " + pr);
    }

    private Query createDoubleQuery(String propertyName, Filter.PropertyRestriction pr) {
        Double first = pr.first != null ? pr.first.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE) : null;
        Double last = pr.last != null ? pr.last.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE) : null;
        Double not = pr.not != null ? pr.not.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE) : null;

        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            return org.apache.lucene.document.DoublePoint.newExactQuery(propertyName, first);
        } else if (pr.first != null && pr.last != null) {
            double lowerValue = pr.firstIncluding ? first : Math.nextUp(first);
            double upperValue = pr.lastIncluding ? last : Math.nextDown(last);
            return org.apache.lucene.document.DoublePoint.newRangeQuery(propertyName, lowerValue, upperValue);
        } else if (pr.first != null) {
            double lowerValue = pr.firstIncluding ? first : Math.nextUp(first);
            return org.apache.lucene.document.DoublePoint.newRangeQuery(propertyName, lowerValue, Double.MAX_VALUE);
        } else if (pr.last != null) {
            double upperValue = pr.lastIncluding ? last : Math.nextDown(last);
            return org.apache.lucene.document.DoublePoint.newRangeQuery(propertyName, -Double.MAX_VALUE, upperValue);
        } else if (pr.list != null) {
            double[] values = pr.list.stream()
                .map(pv -> pv.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE))
                .mapToDouble(Double::doubleValue)
                .toArray();
            return org.apache.lucene.document.DoublePoint.newSetQuery(propertyName, values);
        } else if (pr.isNot && not != null) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(new MatchAllDocsQuery(), Occur.MUST);
            bq.add(org.apache.lucene.document.DoublePoint.newExactQuery(propertyName, not), Occur.MUST_NOT);
            return bq.build();
        }

        throw new IllegalArgumentException("Unsupported property restriction: " + pr);
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
                if (q != null) {
                    result.set(q);
                }
                return true;
            }
        });
        return result.get();
    }

    /**
     * Tokenizes text and builds appropriate Lucene query (TermQuery or PhraseQuery).
     * Based on legacy LuceneIndex implementation.
     */
    private static Query tokenToQuery(String text, String fieldName, Analyzer analyzer) {
        List<String> tokens = tokenize(text, analyzer);

        if (tokens.isEmpty()) {
            return new BooleanQuery.Builder().build();
        }

        // Use FieldNames.FULLTEXT if no specific field
        String field = (fieldName == null || "*".equals(fieldName))
            ? FieldNames.FULLTEXT
            : fieldName;

        if (tokens.size() == 1) {
            // Single token - use TermQuery
            return new TermQuery(new Term(field, tokens.get(0)));
        } else {
            // Multiple tokens - use PhraseQuery
            PhraseQuery.Builder pq = new PhraseQuery.Builder();
            for (String token : tokens) {
                pq.add(new Term(field, token));
            }
            return pq.build();
        }
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
        // Check if we can handle this query
        FullTextExpression ft = filter.getFullTextConstraint();
        List<Filter.PropertyRestriction> propRestrictions = new ArrayList<>(filter.getPropertyRestrictions());

        // Extract facet fields before the early-exit guard so facet-only queries are handled
        List<String> facetFields = extractFacetFields(filter);

        // We can handle full-text queries, property restrictions, and/or facet requests
        if (ft == null && propRestrictions.isEmpty() && facetFields.isEmpty()) {
            return Collections.emptyList();
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
        builder.setFulltextIndex(ft != null); // Full-text if ft constraint present
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

        return Collections.singletonList(builder.build());
    }

    @Override
    public String getPlanDescription(QueryIndex.IndexPlan plan, NodeState root) {
        StringBuilder sb = new StringBuilder("lucene9:");
        sb.append(indexPath);

        Filter filter = plan.getFilter();
        if (filter != null) {
            FullTextExpression ft = filter.getFullTextConstraint();
            if (ft != null) {
                sb.append(" ft=").append(ft);
            }

            List<Filter.PropertyRestriction> propRestrictions = new ArrayList<>(filter.getPropertyRestrictions());
            if (!propRestrictions.isEmpty()) {
                sb.append(" props=").append(propRestrictions.size());
            }
        }

        List<OrderEntry> sortOrder = plan.getSortOrder();
        if (sortOrder != null && !sortOrder.isEmpty()) {
            sb.append(" sort=").append(sortOrder.size()).append(" fields");
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

        try {
            // Get index node
            LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
            if (indexNode == null) {
                LOG.warn("Index node not found: {}", indexPath);
                return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
            }

            // Get searcher
            NodeBuilder definitionBuilder = getDefinitionBuilder(rootState, indexPath);
            IndexSearcherHolder holder = new IndexSearcherHolder(
                definitionBuilder,
                indexNode.getDefinition().getIndexName()
            );
            IndexSearcher searcher = holder.getSearcher();

            // Build Lucene query
            Query query = buildQuery(filter);
            LOG.debug("Executing query: {}", query);

            // Execute query with facet collection if requested, otherwise plain search
            TopDocs docs;
            Map<String, Facets> facetsMap = new HashMap<>();

            if (facetFields != null && !facetFields.isEmpty()) {
                FacetsCollector fc = new FacetsCollector();
                if (sortOrder == null || sortOrder.isEmpty()) {
                    docs = FacetsCollector.search(searcher, query, 100, fc);
                } else {
                    Sort sort = createSort(sortOrder, indexNode.getDefinition());
                    LOG.debug("Sorting by: {}", sort);
                    docs = FacetsCollector.search(searcher, query, 100, sort, fc);
                }

                for (String facetField : facetFields) {
                    try {
                        String luceneFieldName = FieldNames.createFacetFieldName(facetField);
                        DefaultSortedSetDocValuesReaderState state =
                            new DefaultSortedSetDocValuesReaderState(searcher.getIndexReader(), luceneFieldName);
                        facetsMap.put(facetField, new SortedSetDocValuesFacetCounts(state, fc));
                    } catch (IllegalArgumentException e) {
                        LOG.debug("Facet field not indexed: {}", facetField);
                    }
                }
            } else {
                if (sortOrder == null || sortOrder.isEmpty()) {
                    docs = searcher.search(query, 100);
                } else {
                    Sort sort = createSort(sortOrder, indexNode.getDefinition());
                    LOG.debug("Sorting by: {}", sort);
                    docs = searcher.search(query, 100, sort);
                }
            }

            LOG.debug("Found {} hits", docs.totalHits);

            // Return cursor
            return new LuceneNgCursor(docs, searcher, holder, facetsMap);

        } catch (IOException e) {
            LOG.error("Error executing query on index: " + indexPath, e);
            return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
        }
    }

    /**
     * Creates Lucene Sort from Oak OrderEntry list.
     * Based on legacy LuceneIndex implementation.
     */
    private Sort createSort(List<OrderEntry> sortOrder, LuceneNgIndexDefinition definition) {
        if (sortOrder == null || sortOrder.isEmpty()) {
            return null;
        }

        List<SortField> fields = new ArrayList<>();
        for (OrderEntry order : sortOrder) {
            SortField sf = createSortField(order, definition);
            if (sf != null) {
                fields.add(sf);
            }
        }

        return new Sort(fields.toArray(new SortField[0]));
    }

    private SortField createSortField(OrderEntry order, LuceneNgIndexDefinition definition) {
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

        return new SortField(propertyName, fieldType, reverse);
    }

    /**
     * Gets the property type from the index definition, falling back to the provided type.
     * Based on legacy LucenePropertyIndex.getPropertyType.
     */
    private int getPropertyTypeFromDefinition(LuceneNgIndexDefinition definition, String propertyName, int fallbackType) {
        // Try to find property definition in index rules
        for (org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule rule : definition.getDefinedRules()) {
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
     * In Oak, facet requests are modelled as PropertyRestrictions where
     * pr.propertyName equals "rep:facet" and pr.first holds the full
     * expression "rep:facet(propName)" as a string value.
     */
    private List<String> extractFacetFields(Filter filter) {
        List<String> facetFields = new ArrayList<>();
        for (Filter.PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String propName = pr.propertyName;
            if (QueryConstants.REP_FACET.equals(propName) && pr.first != null) {
                String value = pr.first.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
                if (value != null && value.startsWith(QueryConstants.REP_FACET + "(")
                        && value.endsWith(")")) {
                    String facetField = value.substring(
                        QueryConstants.REP_FACET.length() + 1, value.length() - 1).trim();
                    if (!facetField.isEmpty()) {
                        facetFields.add(facetField);
                    }
                }
            }
        }
        return facetFields;
    }
}
