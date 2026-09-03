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
package org.apache.jackrabbit.oak.plugins.index.search.spi.query;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.index.IndexName;
import org.apache.jackrabbit.oak.plugins.index.cursor.Cursors;
import org.apache.jackrabbit.oak.plugins.index.cursor.PathCursor;
import org.apache.jackrabbit.oak.plugins.index.search.IndexLookup;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.SizeEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvanceFulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.NativeQueryIndex;

/**
 * Provides an abstract QueryIndex that does lookups against a fulltext index
 *
 * @see QueryIndex
 *
 */
public abstract class FulltextIndex implements AdvancedQueryIndex, QueryIndex, NativeQueryIndex,
        AdvanceFulltextQueryIndex {

    private static final Logger LOG = LoggerFactory.getLogger(FulltextIndex.class);

    public static final String ATTR_PLAN_RESULT = "oak.fulltext.planResult";

    private static final double MIN_COST = 2.1;

    // Index types that may compete; other types (e.g. "disabled") are excluded
    private static final Set<String> COMPETING_INDEX_TYPES = Set.of("lucene", "elasticsearch");

    public static final String FT_FILTER_GLOBALLY_SUPERSEDED = "FT_OAK-12146";

    // OAK-12173: bug fix, enabled by default - disable only if the bounded
    // retry below causes a problem in some deployment.
    public static final String FT_INDEX_NOT_READY_RETRY_OAK_12173 = "FT_INDEX_NOT_READY_RETRY_OAK-12173";
    public static final AtomicBoolean FT_INDEX_NOT_READY_RETRY_OAK_12173_DISABLE = new AtomicBoolean(false);

    private static final int NOT_READY_RETRY_ATTEMPTS = 3;
    private static final long NOT_READY_RETRY_SLEEP_MILLIS = 50;

    // Total retry budget for a single getPlans() call, shared across every
    // not-yet-ready candidate index encountered during that call - not a
    // per-index budget. Without sharing this deadline, a query matching N
    // not-ready indexes would block for up to N * (attempts * sleep), which
    // defeats the point of bounding the worst case.
    private static final long NOT_READY_RETRY_BUDGET_MILLIS = NOT_READY_RETRY_ATTEMPTS * NOT_READY_RETRY_SLEEP_MILLIS;

    // Rate-limits the "index not yet ready" WARN below, per index path, so that a
    // long-running (re)indexing operation doesn't flood the log with one WARN per
    // matching query for the whole duration of the (re)index.
    private static final ConcurrentMap<String, Long> notReadyWarningTimestamps = new ConcurrentHashMap<>();
    private static final long NOT_READY_WARNING_INTERVAL_MILLIS = 60_000;

    @Nullable private Feature filterGloballySupersededFeature;

    public void setFilterGloballySupersededFeature(@Nullable Feature feature) {
        this.filterGloballySupersededFeature = feature;
    }

    protected abstract IndexNode acquireIndexNode(String indexPath);

    protected abstract String getType();

    protected abstract SizeEstimator getSizeEstimator(IndexPlan plan);

    protected abstract Predicate<NodeState> getIndexDefinitionPredicate();

    protected abstract String getFulltextRequestString(IndexPlan plan, IndexNode indexNode, NodeState rootState);

    /**
     * Whether replaced indexes (that is, if a new version of the index is
     * available) should be filtered out.
     *
     * @return true if yes (e.g. in a blue-green deployment model)
     */
    protected abstract boolean filterReplacedIndexes();

    /**
     * Whether the isActiveIndex check should run during filtering of replaced indexes.
     *
     */
    protected abstract boolean runIsActiveIndexCheck();

    /**
     * Returns the {@link FulltextIndexPlanner} for the specified arguments
     */
    protected FulltextIndexPlanner getPlanner(IndexNode indexNode, String path, Filter filter, List<OrderEntry> sortOrder) {
        return new FulltextIndexPlanner(indexNode, path, filter, sortOrder);
    }

    /**
     * @param indexPath the index path
     * @return {@code true} if {@code indexPath} is a known, well-formed index
     * definition that matched this query but is not currently queryable
     * (most commonly because it has not finished its first (re)indexing
     * cycle yet). Subclasses that can tell this apart from "no such index"
     * should override this; the default is {@code false} so index types
     * that don't support the distinction keep today's exact behavior.
     */
    protected boolean isIndexNotYetReady(String indexPath) {
        return false;
    }

    @Override
    public double getMinimumCost() {
        return MIN_COST;
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
        Collection<String> indexPaths = new IndexLookup(rootState, getIndexDefinitionPredicate())
                .collectIndexNodePaths(filter);
        if (filterReplacedIndexes()) {
            indexPaths = IndexName.filterReplacedIndexes(indexPaths, rootState, runIsActiveIndexCheck());
        } else {
            indexPaths = IndexName.filterNewestIndexes(indexPaths);
        }
        if (filterGloballySupersededFeature == null || filterGloballySupersededFeature.isEnabled()) {
            // first collect indexes of the other types (collect lucene indexes if we look at elastic,
            // and vice versa)
            Collection<String> allCompetingPathsOfOtherTypes = new IndexLookup(rootState,
                    state -> {
                        String type = state.getString(TYPE_PROPERTY_NAME);
                        if (type == null) {
                            // indexes without type don't compete (to avoid NPE)
                            return false;
                        } else if (getIndexDefinitionPredicate().test(state)) {
                            // index of this type don't compete. this is to avoid
                            // that indexes that are disabled are considered competing
                            return false;
                        }
                        return COMPETING_INDEX_TYPES.contains(type);
                    })
                    .collectIndexNodePaths(filter);
            // build a combined set: these indexes of other types,
            HashSet<String> allCompetingPaths = new HashSet<>(allCompetingPathsOfOtherTypes);
            // plus the _active_ indexes of the current type
            allCompetingPaths.addAll(indexPaths);
            indexPaths = IndexName.filterGloballySuperseded(indexPaths, allCompetingPaths);
        }
        List<IndexPlan> plans = new ArrayList<>(indexPaths.size());
        // Lazily established on the first not-yet-ready index seen below, then
        // shared by every subsequent one in this call - see
        // NOT_READY_RETRY_BUDGET_MILLIS.
        long retryDeadline = -1;
        for (String path : indexPaths) {
            IndexNode indexNode = null;
            try {
                indexNode = acquireIndexNode(path);

                if (indexNode == null && isIndexNotYetReady(path)) {
                    if (!FT_INDEX_NOT_READY_RETRY_OAK_12173_DISABLE.get()) {
                        if (retryDeadline < 0) {
                            retryDeadline = System.currentTimeMillis() + NOT_READY_RETRY_BUDGET_MILLIS;
                        }
                        indexNode = retryAcquireIndexNode(path, retryDeadline);
                    }
                    if (indexNode == null) {
                        warnIndexNotReady(path);
                    }
                }

                if (indexNode != null) {
                    IndexPlan plan = getPlanner(indexNode, path, filter, sortOrder).getPlan();
                    if (plan != null) {
                        plans.add(plan);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error getting plan for {}", path, e);
            } finally {
                if (indexNode != null) {
                    indexNode.release();
                }
            }
        }
        return plans;
    }

    /**
     * Bounded, blocking retry for an index that {@link #isIndexNotYetReady(String)}
     * says is a known candidate but not currently acquirable. Sleeps on the
     * calling (query) thread between attempts, until {@code deadlineMillis}
     * ({@link System#currentTimeMillis()} epoch millis) is reached.
     * {@code deadlineMillis} is shared across every not-yet-ready index
     * encountered within one {@link #getPlans} call, so the total added
     * latency for that call - however many such indexes it hits - is capped
     * at {@code NOT_READY_RETRY_BUDGET_MILLIS}, not that amount per index.
     */
    private IndexNode retryAcquireIndexNode(String path, long deadlineMillis) {
        long remaining;
        while ((remaining = deadlineMillis - System.currentTimeMillis()) > 0) {
            try {
                Thread.sleep(Math.min(NOT_READY_RETRY_SLEEP_MILLIS, remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            IndexNode indexNode = acquireIndexNode(path);
            if (indexNode != null) {
                return indexNode;
            }
        }
        return null;
    }

    /**
     * Logs a WARN that the index at {@code path} is being dropped from consideration
     * for this query because it is not yet ready, rate-limited to at most once per
     * {@link #NOT_READY_WARNING_INTERVAL_MILLIS} per index path so that a long-running
     * (re)indexing operation doesn't flood the log.
     */
    private static void warnIndexNotReady(String path) {
        long now = System.currentTimeMillis();
        Long last = notReadyWarningTimestamps.get(path);
        if (last == null || now - last >= NOT_READY_WARNING_INTERVAL_MILLIS) {
            notReadyWarningTimestamps.put(path, now);
            LOG.warn("Index at [{}] matches this query but is not yet ready to serve reads " +
                    "(likely still (re)indexing) - falling back to traversal for this query", path);
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
        IndexNode index = acquireIndexNode(plan);
        Validate.checkState(index != null, "The fulltext index of type %s index is not available", getType());
        try {
            FullTextExpression ft = filter.getFullTextConstraint();
            StringBuilder sb = new StringBuilder();
            sb.append(getType()).append(":").append(getIndexName(plan)).append("\n");
            String path = getPlanResult(plan).indexPath;
            sb.append("    indexDefinition: ").append(path).append("\n");
            sb.append("    estimatedEntries: ").append(plan.getEstimatedEntryCount()).append("\n");
            // luceneQuery / elasticQuery
            sb.append("    ").append(getType()).append("Query: ").append(getFulltextRequestString(plan, index, root)).append("\n");
            if (plan.getSortOrder() != null && !plan.getSortOrder().isEmpty()) {
                sb.append("    sortOrder: ").append(plan.getSortOrder()).append("\n");
            }
            if (ft != null) {
                sb.append("    fulltextCondition: ").append(ft).append("\n");
            }
            addSyncIndexPlan(plan, sb);
            return sb.toString();
        } finally {
            index.release();
        }
    }

    protected static void addSyncIndexPlan(IndexPlan plan, StringBuilder sb) {
        PlanResult pr = getPlanResult(plan);
        if (pr.hasPropertyIndexResult()) {
            FulltextIndexPlanner.PropertyIndexResult pres = pr.getPropertyIndexResult();
            sb.append("    propertyCondition: ").append(pres.propertyName);
            if (!pres.propertyName.equals(pres.pr.propertyName)) {
                sb.append("[").append(pres.pr.propertyName).append("]");
            }
            sb.append(" ").append(pres.pr);
            sb.append("\n");
        }
        if (pr.evaluateSyncNodeTypeRestriction()) {
            sb.append("    synchronousNodeType: ");
            sb.append("primaryTypes=").append(plan.getFilter().getPrimaryTypes());
            sb.append(" mixinTypes=").append(plan.getFilter().getMixinTypes());
            sb.append("\n");
        }
    }

    @Override
    public Cursor query(final Filter filter, final NodeState root) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    protected static boolean shouldInclude(String docPath, IndexPlan plan) {
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

    protected IndexNode acquireIndexNode(IndexPlan plan) {
        return acquireIndexNode(getPlanResult(plan).indexPath);
    }

    protected static String getIndexName(IndexPlan plan) {
        return PathUtils.getName(getPlanResult(plan).indexPath);
    }

    public static int determinePropertyType(PropertyDefinition defn, PropertyRestriction pr) {
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

    protected static int getPropertyType(PropertyDefinition defn, String name, int defaultVal) {
        if (defn.isTypeDefined()) {
            return defn.getType();
        }
        return defaultVal;
    }

    protected static PlanResult getPlanResult(IndexPlan plan) {
        return (PlanResult) plan.getAttribute(ATTR_PLAN_RESULT);
    }

    /**
     * Following chars are used as operators in Lucene Query and should be escaped
     */
    private static final String QUERY_OPERATORS =
            new String(new char[] {':', '/', '!', '&', '|', '='});

    /**
     * Following logic is taken from org.apache.jackrabbit.core.query.lucene.JackrabbitQueryParser#parse(java.lang.String)
     */
    public static String rewriteQueryText(String textsearch) {
        // replace escaped ' with just '
        StringBuilder rewritten = new StringBuilder();
        // most query parsers recognize 'AND' and 'NOT' as
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
            } else if (QUERY_OPERATORS.indexOf(c) >= 0) {
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

    public static String getPathRestriction(IndexPlan plan) {
        Filter f = plan.getFilter();
        String pathPrefix = plan.getPathPrefix();
        if (pathPrefix.isEmpty()) {
            return f.getPath();
        }
        String relativePath = PathUtils.relativize(pathPrefix, f.getPath());
        return "/" + relativePath;
    }

    public static class FulltextResultRow {
        public final String path;
        public final double score;
        public final String suggestion;
        public final boolean isVirutal;
        public final Map<String, String> excerpts;
        public final String explanation;
        private final FacetProvider facetProvider;

        public FulltextResultRow(String path, double score, Map<String, String> excerpts,
                                 FacetProvider facetProvider, String explanation) {
            this.explanation = explanation;
            this.excerpts = excerpts;
            this.facetProvider = facetProvider;
            this.isVirutal = false;
            this.path = path;
            this.score = score;
            this.suggestion = null;
        }

        public FulltextResultRow(String suggestion, long weight) {
            this(suggestion, (double)weight);
        }

        public FulltextResultRow(String suggestion, double weight) {
            this.isVirutal = true;
            this.path = "/";
            this.score = weight;
            this.suggestion = suggestion;
            this.excerpts = null;
            this.facetProvider = null;
            this.explanation = null;
        }

        public FulltextResultRow(String suggestion) {
            this(suggestion, 1);
        }

        @Override
        public String toString() {
            return String.format("%s (%1.2f)", path, score);
        }

        public List<Facet> getFacets(int numberOfFacets, String columnName) throws IOException {
            if (facetProvider == null) {
                return null;
            }

            return facetProvider.getFacets(numberOfFacets, columnName);
        }
    }

    public interface FacetProvider {
        List<Facet> getFacets(int numberOfFacets, String columnName) throws IOException;
    }

    /**
     * A cursor over Fulltext results. The result includes the path,
     * and the jcr:score pseudo-property as returned by Lucene.
     */
    protected static class FulltextPathCursor implements Cursor {

        private final Logger log = LoggerFactory.getLogger(getClass());
        private final int TRAVERSING_WARNING = Integer.getInteger("oak.traversing.warning", 10000);

        private final Cursor pathCursor;
        private final String pathPrefix;
        private final SizeEstimator sizeEstimator;
        private final int numberOfFacets;

        // the cached value
        private long estimatedSize;

        // the current row in the pathCursor
        // (so we don't have to extend the PathCursor)
        FulltextResultRow currentRowInPathIterator;

        public FulltextPathCursor(final Iterator<FulltextResultRow> it, final IteratorRewoundStateProvider iterStateProvider,
                                  final IndexPlan plan, QueryLimits settings, SizeEstimator sizeEstimator) {
            pathPrefix = plan.getPathPrefix();
            this.sizeEstimator = sizeEstimator;
            Iterator<String> pathIterator = new Iterator<>() {

                private int readCount;
                private int rewoundCount;

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public String next() {
                    if (iterStateProvider.rewoundCount() > rewoundCount) {
                        readCount = 0;
                        rewoundCount = iterStateProvider.rewoundCount();
                    }
                    currentRowInPathIterator = it.next();
                    readCount++;
                    if (readCount % TRAVERSING_WARNING == 0) {
                        Cursors.checkReadLimit(readCount, settings);
                        if (readCount == 2 * TRAVERSING_WARNING) {
                            log.warn("Index-Traversed {} nodes with filter {}", readCount, plan.getFilter(),
                                    new Exception("call stack"));
                        } else {
                            log.warn("Index-Traversed {} nodes with filter {}", readCount, plan.getFilter());
                        }
                    }
                    return currentRowInPathIterator.path;
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
            IndexRow pathRow = pathCursor.next();

            // currentRowInPathIterator is changed as a side effect
            // of calling pathCursor.next()
            FulltextResultRow row = currentRowInPathIterator;

            return new IndexResultRow(pathRow, row, pathPrefix, numberOfFacets);
        }

        @Override
        public long getSize(SizePrecision precision, long max) {
            if (estimatedSize != 0) {
                return estimatedSize;
            }
            return estimatedSize = sizeEstimator.getSize();
        }
    }

    private static class IndexResultRow implements IndexRow {

        private final IndexRow pathRow;
        private final FulltextResultRow resultRow;
        private final String pathPrefix;
        private final int numberOfFacets;

        IndexResultRow(IndexRow pathRow, FulltextResultRow row, String pathPrefix, int numberOfFacets) {
            this.pathRow = pathRow;
            this.resultRow = row;
            this.pathPrefix = pathPrefix;
            this.numberOfFacets = numberOfFacets;
        }

        @Override
        public boolean isVirtualRow() {
            return resultRow.isVirutal;
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
                return PropertyValues.newDouble(resultRow.score);
            }
            if (QueryConstants.REP_SPELLCHECK.equals(columnName) || QueryConstants.REP_SUGGEST.equals(columnName)) {
                return PropertyValues.newString(resultRow.suggestion);
            }
            if (QueryConstants.OAK_SCORE_EXPLANATION.equals(columnName)) {
                return PropertyValues.newString(resultRow.explanation);
            }
            if (columnName.startsWith(QueryConstants.REP_EXCERPT)) {
                String excerpt = resultRow.excerpts.get(columnName);
                if (excerpt != null) {
                    return PropertyValues.newString(excerpt);
                }
            }
            if (columnName.startsWith(QueryConstants.REP_FACET)) {
                try {
                    List<Facet> facets = resultRow.getFacets(numberOfFacets, columnName);
                    if (facets != null) {
                        JsopWriter writer = new JsopBuilder();
                        writer.object();
                        for (Facet f : facets) {
                            writer.key(f.getLabel()).value(f.getCount());
                        }
                        writer.endObject();
                        return PropertyValues.newString(writer.toString());
                    }
                } catch (IOException | RuntimeException e) {
                    LOG.warn(e.getMessage());
                    LOG.debug(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
            return pathRow.getValue(columnName);
        }

    }

    public interface IteratorRewoundStateProvider {
        int rewoundCount();
    }

    /**
     * A query result facet, composed by its label and count.
     */
    public static class Facet {

        private final String label;

        private final int count;

        public Facet(String label, int count) {
            this.label = label;
            this.count = count;
        }

        /**
         * get the facet label
         * @return a label
         */
        @NotNull
        public String getLabel() {
            return label;
        }

        /**
         * get the facet count
         * @return an integer
         */
        public int getCount() {
            return count;
        }

        @Override
        public String toString() {
            return "Facet{" +
                    "label='" + label + '\'' +
                    ", count=" + count +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Facet facet = (Facet) o;
            return Objects.equals(label, facet.label);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label);
        }
    }

    /**
     * Get the facet name from a column name.
     *
     * This method silently assumes(!) that the column name starts with "rep:facet("
     * and ends with ")".
     *
     * @param columnName the column name, e.g. "rep:facet(abc)"
     * @return the facet name, e.g. "abc"
     */
    public static String parseFacetField(String columnName) {
        return columnName.substring(QueryConstants.REP_FACET.length() + 1, columnName.length() - 1);
    }

    /**
     * Convert the facet name to a column name.
     *
     * @param facetFieldName the facet field name, e.g. "abc"
     * @return the column name, e.g. "rep:facet(abc)"
     */
    public static String convertFacetFieldNameToColumnName(String facetFieldName) {
        return QueryConstants.REP_FACET + "(" + facetFieldName + ")";
    }
}
