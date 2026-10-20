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
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.SizeEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexNode;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgQueryBuilder;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgResultRowIterator;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgSecureSortedSetDocValuesFacetCounts;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgStatisticalSortedSetDocValuesFacetCounts;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TotalHitCountCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

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

    public LuceneNgIndex(LuceneNgIndexTracker tracker) {
        this(tracker, null);
    }

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
        if (!isRootIndex(indexPath)) {
            return null;
        }
        return tracker.acquireIndexNode(indexPath);
    }

    private static boolean isRootIndex(String path) {
        return "/oak:index".equals(PathUtils.getParentPath(path));
    }

    private String getExecutionPath(IndexPlan plan) {
        FulltextIndexPlanner.PlanResult result = getPlanResult(plan);
        String path = result != null ? result.indexPath : indexPath;
        if (path == null) {
            throw new IllegalStateException("LuceneNg query plan has no index path");
        }
        if (!isRootIndex(path)) {
            throw new IllegalArgumentException("LuceneNg queries require an index directly under /oak:index");
        }
        return path;
    }

    @Override
    protected LuceneNgIndexNode acquireIndexNode(IndexPlan plan) {
        return (LuceneNgIndexNode) super.acquireIndexNode(plan);
    }

    @Override
    protected String getType() {
        return LuceneNgIndexConstants.TYPE_LUCENE_NG;
    }

    @Override
    public String getIndexName() {
        return LuceneNgIndexConstants.TYPE_LUCENE_NG;
    }

    @Override
    protected SizeEstimator getSizeEstimator(IndexPlan plan) {
        // Port of LucenePropertyIndex.getSizeEstimator: a count-only search over the plan's query.
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
                Query query = LuceneNgQueryBuilder.query(plan.getFilter(), getPlanResult(plan));
                TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(query, collector);
                int totalHits = collector.getTotalHits();
                LOG.debug("Estimated size for query {} is {}", query, totalHits);
                return (long) totalHits;
            } catch (IOException e) {
                LOG.warn("Size-estimate query failed on index {}", getExecutionPath(plan), e);
                return -1L;
            } finally {
                indexNode.release();
            }
        };
    }

    @Override
    protected Predicate<NodeState> getIndexDefinitionPredicate() {
        return state -> LuceneNgIndexConstants.TYPE_LUCENE_NG.equals(
                state.getString(IndexConstants.TYPE_PROPERTY_NAME));
    }

    @Override
    protected String getFulltextRequestString(IndexPlan plan, IndexNode indexNode, NodeState rootState) {
        return LuceneNgQueryBuilder.query(plan.getFilter(), getPlanResult(plan)).toString();
    }

    @Override
    protected boolean filterReplacedIndexes() {
        return false; // matches this module's current behavior — no blue/green mount-info concept yet
    }

    @Override
    protected boolean runIsActiveIndexCheck() {
        return false; // matches ElasticIndex's choice; LuceneNg has no active-index-check concept yet
    }

    // ===== AdvancedQueryIndex methods =====

    @Override
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        // The first line starts with "lucene:" so that tooling recognising Lucene index plans by
        // that prefix still detects the index; the "@ng" suffix marks the LuceneNg backend, the
        // "luceneNg:" line names the index type and "luceneQuery:" keeps the existing query label.
        String path = getExecutionPath(plan);
        String shortName = PathUtils.getName(path);
        StringBuilder sb = new StringBuilder("lucene:");
        sb.append(shortName).append("@ng\n");
        sb.append(getType()).append(':').append(shortName).append("\n");
        sb.append("    indexDefinition: ").append(path).append("\n");
        sb.append("    estimatedEntries: ").append(plan.getEstimatedEntryCount()).append("\n");

        Filter filter = plan.getFilter();
        if (filter != null) {
            sb.append("    luceneQuery: ").append(LuceneNgQueryBuilder.query(filter, getPlanResult(plan)).toString()).append("\n");
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
        String selectedPath = getExecutionPath(plan);
        // Extract filter and sort order from plan
        Filter filter = plan.getFilter();
        List<OrderEntry> sortOrder = plan.getSortOrder();

        @SuppressWarnings("unchecked")
        List<String> facetFields = (List<String>) plan.getAttribute(ATTR_FACET_FIELDS);

        Query query = LuceneNgQueryBuilder.query(filter, getPlanResult(plan));
        LOG.debug("Executing query: {}", query);

        Sort sort = null;
        Map<String, List<Facet>> facets = Collections.emptyMap();

        // Facets (and the sort they may need) are computed once in a bounded, self-contained
        // acquire — this does NOT leak the index node into row iteration, which pages
        // independently inside the cursor. Sort-only queries acquire once just to build the Sort.
        if (facetFields != null && !facetFields.isEmpty()) {
            LuceneNgIndexNode facetNode = tracker.acquireIndexNode(selectedPath);
            if (facetNode == null) {
                LOG.warn("Index node not found or not yet populated: {}", selectedPath);
                return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
            }
            try {
                IndexSearcher facetSearcher = facetNode.getSearcher();
                LuceneNgIndexDefinition definition = facetNode.getDefinition();
                SecureFacetConfiguration secureFacetConfiguration = definition.getSecureFacetConfiguration();
                if (sortOrder != null && !sortOrder.isEmpty()) {
                    sort = LuceneNgQueryBuilder.sort(sortOrder, definition, docValuesTypes(facetNode));
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
                facets = topFacets(facetsMap, definition.getNumberOfTopFacets());
            } catch (IOException e) {
                LOG.error("Error computing facets on index: " + selectedPath, e);
            } finally {
                facetNode.release();
            }
        } else if (sortOrder != null && !sortOrder.isEmpty()) {
            LuceneNgIndexNode sortNode = tracker.acquireIndexNode(selectedPath);
            if (sortNode != null) {
                try {
                    sort = LuceneNgQueryBuilder.sort(sortOrder, sortNode.getDefinition(), docValuesTypes(sortNode));
                } finally {
                    sortNode.release();
                }
            }
        }

        Map<String, List<Facet>> resultFacets = facets;
        FulltextIndexPlanner.PlanResult planResult = getPlanResult(plan);
        LuceneNgResultRowIterator rows = new LuceneNgResultRowIterator(tracker, selectedPath, query, sort,
                nodeExcerptColumns(filter), resultFacets.isEmpty() ? null : (numberOfFacets, columnName) -> resultFacets.get(columnName),
                documentPath -> {
                    String path = planResult.isPathTransformed() ? planResult.transformPath(documentPath) : documentPath;
                    return path != null && shouldInclude(path, plan) ? path : null;
                });
        return new FulltextPathCursor(rows, rows, plan, filter.getQueryLimits(), getSizeEstimator(plan));
    }

    /**
     * The requested {@code rep:excerpt} columns that refer to the whole node; property-scoped
     * excerpts are not supported.
     */
    private static Set<String> nodeExcerptColumns(Filter filter) {
        Set<String> columns = new HashSet<>();
        for (Filter.PropertyRestriction pr : filter.getPropertyRestrictions()) {
            if (QueryConstants.REP_EXCERPT.equals(pr.propertyName) && pr.first != null) {
                String column = pr.first.getValue(Type.STRING);
                if (QueryConstants.REP_EXCERPT.equals(column) || column.equals(
                        QueryConstants.REP_EXCERPT + "(" + FulltextIndexConstants.EXCERPT_NODE_FIELD_NAME + ")")) {
                    columns.add(column);
                }
            }
        }
        return columns;
    }

    /**
     * The top facets of each dimension, by {@code rep:facet(dim)} column. Computed before row
     * iteration because the {@link Facets} reference a searcher that is released before then.
     */
    private static Map<String, List<Facet>> topFacets(Map<String, Facets> facetsMap, int topChildren) {
        int facetTopChildren = Math.max(1, topChildren);
        Map<String, List<Facet>> result = new HashMap<>();
        for (Map.Entry<String, Facets> entry : facetsMap.entrySet()) {
            String dimension = entry.getKey();
            try {
                FacetResult fr = entry.getValue().getTopChildren(facetTopChildren, dimension);
                if (fr == null || fr.labelValues == null) {
                    fr = entry.getValue().getTopChildren(facetTopChildren, FieldNames.createFacetFieldName(dimension));
                }
                if (fr != null && fr.labelValues != null) {
                    List<Facet> facets = new ArrayList<>();
                    for (LabelAndValue lv : fr.labelValues) {
                        facets.add(new Facet(lv.label, lv.value.intValue()));
                    }
                    result.put(QueryConstants.REP_FACET + "(" + dimension + ")", facets);
                }
            } catch (IOException e) {
                LOG.error("Failed to build facets for {}: {}", dimension, e.getMessage());
            }
        }
        return result;
    }

    private static Function<String, DocValuesType> docValuesTypes(LuceneNgIndexNode indexNode) {
        return name -> {
            FieldInfo fieldInfo = indexNode.getFieldInfos().fieldInfo(name);
            return fieldInfo == null ? DocValuesType.NONE : fieldInfo.getDocValuesType();
        };
    }
}
