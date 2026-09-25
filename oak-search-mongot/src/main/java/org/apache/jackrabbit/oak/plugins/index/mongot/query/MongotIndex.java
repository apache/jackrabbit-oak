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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.mongodb.MongoCommandException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexNode;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.SizeEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

final class MongotIndex extends FulltextIndex {

    private static final Logger LOG = LoggerFactory.getLogger(MongotIndex.class);
    private static final Predicate<NodeState> MONGODB_INDEX_DEFINITION =
            state -> MongotIndexDefinition.TYPE_MONGOT.equals(state.getString(TYPE_PROPERTY_NAME));
    private static final IteratorRewoundStateProvider NEVER_REWOUND = () -> 0;
    private static final long SEARCH_READINESS_TIMEOUT_MILLIS = Long.getLong(
            "oak.mongot.searchReadinessTimeoutMillis", 15_000L);
    private static final long SEARCH_READINESS_RETRY_MILLIS = 100L;
    private static final int VECTOR_SEARCH_LIMIT = 10;
    private static final int VECTOR_SEARCH_NUM_CANDIDATES = 200;

    private final MongotIndexTracker tracker;

    MongotIndex(MongotIndexTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    protected IndexNode acquireIndexNode(String indexPath) {
        return tracker.acquireIndexNode(indexPath);
    }

    @Override
    protected String getType() {
        return MongotIndexDefinition.TYPE_MONGOT;
    }

    @Override
    protected FulltextIndexPlanner getPlanner(IndexNode indexNode, String path, Filter filter,
                                              List<OrderEntry> sortOrder) {
        return new MongotIndexPlanner(indexNode, path, filter, sortOrder);
    }

    @Override
    protected SizeEstimator getSizeEstimator(IndexPlan plan) {
        // Required by the FulltextIndex SPI contract, but not called anywhere in this
        // class: it reports the whole index's document count rather than a
        // query-specific count, so it wouldn't be accurate for Cursor.getSize().
        // See streamingCursor()'s own $count-based estimator for the one actually used.
        return () -> {
            MongotIndexNode node = (MongotIndexNode) acquireIndexNode(plan);
            try {
                return node.getIndexStatistics().numDocs();
            } finally {
                node.release();
            }
        };
    }

    @Override
    protected Predicate<NodeState> getIndexDefinitionPredicate() {
        return MONGODB_INDEX_DEFINITION;
    }

    @Override
    protected String getFulltextRequestString(IndexPlan plan, IndexNode indexNode, NodeState rootState) {
        MongotIndexDefinition definition = (MongotIndexDefinition) indexNode.getDefinition();
        MongotQueryRequest request = MongotQueryRequest.from(plan.getFilter(), definition);
        SimilaritySeed similaritySeed = request.similarityPath() == null
                ? null
                : explainSimilaritySeed(request.similarityPath(), definition);
        return pipeline(plan, definition, request, similaritySeed).toString();
    }

    @Override
    protected boolean filterReplacedIndexes() {
        return true;
    }

    @Override
    protected boolean runIsActiveIndexCheck() {
        return false;
    }

    @Override
    public String getIndexName() {
        return MongotIndexDefinition.TYPE_MONGOT;
    }

    @Override
    public Cursor query(IndexPlan plan, NodeState rootState) {
        MongotIndexNode node = (MongotIndexNode) acquireIndexNode(plan);
        try {
            MongotIndexDefinition definition = node.getDefinition();
            MongotQueryRequest request = MongotQueryRequest.from(plan.getFilter(), definition);
            MongoCollection<Document> collection = node.getConnection().getCollection(definition);
            if (request.mode() != MongotQueryRequest.Mode.NORMAL) {
                return cursor(virtualRows(collection, plan, definition, request), plan);
            }
            SimilaritySeed similaritySeed = null;
            if (request.similarityPath() != null) {
                Document reference = collection.find(new Document(
                        MongoFieldNames.PATH, request.similarityPath())).first();
                similaritySeed = similaritySeed(reference, definition);
                if (similaritySeed == null) {
                    return cursor(List.of(), plan);
                }
            }
            List<Document> queryPipeline = pipeline(plan, definition, request, similaritySeed);
            if (request.facetFields().isEmpty()) {
                return streamingCursor(collection, plan, definition, request, queryPipeline);
            }
            List<Document> acceptedResults = new ArrayList<>();
            List<Document> facetResults = new ArrayList<>();
            boolean insecureFacets = definition.getSecureFacetConfiguration().getMode()
                    == IndexDefinition.SecureFacetConfiguration.MODE.INSECURE;
            Set<String> seenPaths = new HashSet<>();
            for (Document result : aggregate(collection,
                    queryPipeline, definition)) {
                String path = getPlanResult(plan).transformPath(result.getString(MongoFieldNames.PATH));
                if (path == null || !seenPaths.add(path)) {
                    continue;
                }
                boolean included = shouldInclude(path, plan);
                LOG.trace(included
                                ? "Path {} satisfies hierarchy inclusion rules"
                                : "Path {} not included because of hierarchy inclusion rules",
                        path);
                if (!included) {
                    continue;
                }
                result.put(MongoFieldNames.PATH, path);
                acceptedResults.add(result);
                if (!request.facetFields().isEmpty()
                        && (insecureFacets || plan.getFilter().isAccessible(path))) {
                    facetResults.add(result);
                }
            }

            FulltextIndex.FacetProvider facetProvider = request.facetFields().isEmpty()
                    ? null
                    : MongotResultAdapter.facets(facetResults);
            List<FulltextResultRow> rows = new ArrayList<>(acceptedResults.size());
            for (Document result : acceptedResults) {
                Number score = result.get("_score", Number.class);
                rows.add(new FulltextResultRow(result.getString(MongoFieldNames.PATH),
                        score == null ? 0.0d : score.doubleValue(),
                        MongotResultAdapter.excerpts(result, request.excerptColumns()),
                        facetProvider, null));
            }
            return cursor(rows, plan);
        } finally {
            node.release();
        }
    }

    private Cursor streamingCursor(MongoCollection<Document> collection,
                                   IndexPlan plan,
                                   MongotIndexDefinition definition,
                                   MongotQueryRequest request,
                                   List<Document> pipeline) {
        MongoCursor<Document> documents = aggregateCursor(collection, pipeline, definition);
        Set<String> seenPaths = new HashSet<>();
        MongotResultIterator rows = new MongotResultIterator(documents, result -> {
            String path = getPlanResult(plan).transformPath(result.getString(MongoFieldNames.PATH));
            if (path == null || !seenPaths.add(path)) {
                return null;
            }
            boolean included = shouldInclude(path, plan);
            LOG.trace(included
                            ? "Path {} satisfies hierarchy inclusion rules"
                            : "Path {} not included because of hierarchy inclusion rules",
                    path);
            if (!included) {
                return null;
            }
            Number score = result.get("_score", Number.class);
            return new FulltextResultRow(path,
                    score == null ? 0.0d : score.doubleValue(),
                    MongotResultAdapter.excerpts(result, request.excerptColumns()),
                    null, null);
        }, documents::close);
        // Use a query-specific $count aggregation instead of rows::getSize: the latter
        // would fully drain the streaming cursor (in aggregateCursor()'s batches) the
        // moment anything calls Cursor.getSize()/JCR's RowIterator.getSize() - a common
        // "N results found" UI pattern - even when the caller never intends to iterate
        // all rows. $count is cheap (server-side count, no document bodies returned)
        // and, unlike the unused getSizeEstimator(IndexPlan) override above, reflects
        // this query's actual filter rather than the whole index's document count.
        SizeEstimator sizeEstimator = () -> countMatches(collection, pipeline, definition);
        return new FulltextPathCursor(rows, NEVER_REWOUND, plan,
                plan.getFilter().getQueryLimits(), sizeEstimator);
    }

    private static long countMatches(MongoCollection<Document> collection,
                                     List<Document> pipeline,
                                     MongotIndexDefinition definition) {
        // Sorting and projecting do not change the count, and $sort blocks on every hit.
        List<Document> countPipeline = pipeline.stream()
                .filter(stage -> !stage.containsKey("$sort") && !stage.containsKey("$set")
                        && !stage.containsKey("$project"))
                .collect(Collectors.toCollection(ArrayList::new));
        countPipeline.add(new Document("$count", "n"));
        try (MongoCursor<Document> cursor = aggregateCursor(collection, countPipeline, definition)) {
            return cursor.hasNext() ? ((Number) cursor.next().get("n")).longValue() : 0L;
        }
    }

    private static Cursor cursor(List<FulltextResultRow> rows, IndexPlan plan) {
        Iterator<FulltextResultRow> iterator = rows.iterator();
        return new FulltextPathCursor(iterator, NEVER_REWOUND, plan,
                plan.getFilter().getQueryLimits(), rows::size);
    }

    private List<FulltextResultRow> virtualRows(MongoCollection<Document> collection,
                                                IndexPlan plan,
                                                MongotIndexDefinition definition,
                                                MongotQueryRequest request) {
        List<Document> accessibleResults = new ArrayList<>();
        for (Document result : aggregate(collection,
                pipeline(plan, definition, request, null), definition)) {
            String path = getPlanResult(plan).transformPath(result.getString(MongoFieldNames.PATH));
            if (path != null
                    && (!definition.evaluatePathRestrictions() || shouldInclude(path, plan))
                    && plan.getFilter().isAccessible(path)) {
                accessibleResults.add(result);
            }
        }
        return request.mode() == MongotQueryRequest.Mode.SUGGEST
                ? MongotResultAdapter.suggestions(accessibleResults, request.term())
                : MongotResultAdapter.spellchecks(accessibleResults, request.term());
    }

    private static List<Document> aggregate(MongoCollection<Document> collection,
                                            List<Document> pipeline,
                                            MongotIndexDefinition definition) {
        try (MongoCursor<Document> cursor = aggregateCursor(collection, pipeline, definition)) {
            List<Document> results = new ArrayList<>();
            cursor.forEachRemaining(results::add);
            return results;
        }
    }

    private static MongoCursor<Document> aggregateCursor(MongoCollection<Document> collection,
                                                         List<Document> pipeline,
                                                         MongotIndexDefinition definition) {
        boolean usesSearch = !pipeline.isEmpty() && pipeline.get(0).containsKey("$search");
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(SEARCH_READINESS_TIMEOUT_MILLIS);
        while (true) {
            try {
                // Each driver batch is a full round-trip to the server, so the batch
                // size should be as large as the configuration allows: using the
                // smallest configured fetch size here previously turned a query with
                // many hits into hundreds of round-trips and dominated query latency
                // (e.g. ~18s for ~2000 hits at fetchSizes[0]=10, vs. <1s at 1000; the
                // MongoDB driver holds one batch at a time, and the server caps a
                // single batch response at 16MB regardless of the requested size, so
                // this is safe rather than causing unbounded per-batch memory/time).
                //
                // NOTE / follow-up optimization opportunity (out of scope here): this
                // always uses the largest configured size, even for queries that only
                // need a handful of rows (e.g. a small LIMIT / typeahead query), which
                // wastes server/network work building an oversized first batch. A
                // properly adaptive scheme (small first batch, growing only if the
                // caller keeps pulling) needs a row-limit hint to be threaded through
                // from the query plan down to here - that hint doesn't currently exist
                // in the backend-neutral IndexPlan/Filter/FulltextIndex SPI (oak-search)
                // that this and the Elastic/Lucene backends all implement, so it isn't
                // something this module can fix alone. Worth revisiting as a shared,
                // implementation-neutral improvement to FulltextIndex/IndexPlan rather
                // than a mongot-only special case.
                int batchSize = Arrays.stream(definition.getQueryFetchSizes()).max().getAsInt();
                AggregateIterable<Document> aggregation = collection.aggregate(pipeline)
                        .batchSize(batchSize)
                        .maxTime(definition.getQueryTimeoutMillis(), TimeUnit.MILLISECONDS);
                MongoCursor<Document> cursor = aggregation.iterator();
                // $search against a missing index returns no hits instead of failing, so
                // confirm the index exists only when there are none, not on every query.
                if (usesSearch && !cursor.hasNext()) {
                    requireSearchIndex(collection, definition.getSearchIndexName());
                }
                return cursor;
            } catch (MongoCommandException e) {
                if (!isSearchIndexStarting(e) || System.nanoTime() >= deadline) {
                    throw e;
                }
                // Fail fast when the index is missing or failed instead of waiting for it.
                requireSearchIndex(collection, definition.getSearchIndexName());
                try {
                    TimeUnit.MILLISECONDS.sleep(SEARCH_READINESS_RETRY_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    private static void requireSearchIndex(MongoCollection<Document> collection,
                                           String searchIndexName) {
        for (Document index : collection.listSearchIndexes()) {
            if (!searchIndexName.equals(index.getString("name"))) {
                continue;
            }
            if ("FAILED".equals(index.getString("status"))) {
                throw new IllegalStateException("Mongot index '" + searchIndexName
                        + "' failed: " + index.toJson());
            }
            return;
        }
        throw new IllegalStateException("Mongot index '" + searchIndexName
                + "' is unavailable for collection '"
                + collection.getNamespace().getFullName() + "'");
    }

    private static boolean isSearchIndexStarting(MongoCommandException exception) {
        String message = exception.getErrorMessage();
        return exception.getErrorCode() == 8
                && message != null
                && message.contains("cannot query search index")
                && message.contains("while in state");
    }

    private static Document searchStage(MongotIndexDefinition definition, Document operator,
                                        List<String> highlightPaths) {
        Document search = new Document("index", definition.getSearchIndexName());
        search.putAll(operator);
        if (!highlightPaths.isEmpty()) {
            Object path = highlightPaths.size() == 1 ? highlightPaths.get(0) : highlightPaths;
            search.append("highlight", new Document("path", path));
        }
        return new Document("$search", search);
    }

    private static List<Document> pipeline(IndexPlan plan, MongotIndexDefinition definition,
                                           MongotQueryRequest request,
                                           SimilaritySeed similaritySeed) {
        if (request.mode() != MongotQueryRequest.Mode.NORMAL) {
            return virtualPipeline(plan, definition, request);
        }
        boolean hasFullText = plan.getFilter().getFullTextConstraint() != null;
        boolean hasSimilarity = similaritySeed != null;
        boolean hasNativeQuery = request.nativeQuery() != null;
        MongotQueryTranslation fullText = hasFullText
                ? MongotQueryTranslator.translateFullText(
                        plan.getFilter().getFullTextConstraint(), definition,
                        getPlanResult(plan))
                : null;
        MongotQueryTranslation filters = MongotQueryTranslator.translateFilter(
                plan.getFilter(), getPlanResult(plan));
        if ((hasFullText && !fullText.isSupported()) || !filters.isSupported()) {
            String reason = !filters.isSupported() ? filters.reason() : fullText.reason();
            throw new IllegalStateException("Mongot query plan became unsupported: " + reason);
        }

        List<Document> pipeline = new ArrayList<>();
        boolean hasSearch = hasFullText || hasSimilarity || hasNativeQuery;
        if (hasSearch) {
            Document searchOperator;
            List<String> highlightPaths = MongotResultAdapter.highlightPaths(
                    request.excerptColumns());
            List<Document> lexicalOperators = new ArrayList<>();
            if (hasFullText) {
                lexicalOperators.add(fullText.searchOperator());
            }
            if (hasNativeQuery) {
                lexicalOperators.add(MongotNativeQueryString.operator(request.nativeQuery()));
            }
            Document lexicalOperator = combineMust(lexicalOperators);
            if (hasSimilarity && similaritySeed.hasVector()) {
                searchOperator = vectorSimilarityOperator(similaritySeed,
                        lexicalOperator);
                highlightPaths = List.of();
            } else {
                List<Document> operators = new ArrayList<>(lexicalOperators);
                if (hasSimilarity) {
                    operators.add(similarityOperator(similaritySeed.text()));
                }
                searchOperator = combineMust(operators);
            }
            pipeline.add(searchStage(definition, searchOperator, highlightPaths));
        }
        pipeline.addAll(filters.pipeline());

        Document sort = new Document();
        Document requiredSortValues = new Document();
        boolean scoreMaterialized = false;
        if (plan.getSortOrder() != null) {
            for (OrderEntry entry : plan.getSortOrder()) {
                if (org.apache.jackrabbit.JcrConstants.JCR_SCORE.equals(entry.getPropertyName())) {
                    if (hasSearch) {
                        sort.append("_score",
                                entry.getOrder() == OrderEntry.Order.ASCENDING ? 1 : -1);
                        scoreMaterialized = true;
                    }
                    continue;
                }
                String field = sortField(plan, entry);
                sort.append(field,
                        entry.getOrder() == OrderEntry.Order.ASCENDING ? 1 : -1);
                requiredSortValues.append(field, new Document("$exists", true));
            }
        }
        if (sort.equals(new Document("_score", -1))) {
            // $search already returns hits by descending score; sorting again would block.
            sort.clear();
            scoreMaterialized = false;
        }
        if (!sort.isEmpty()) {
            // Oak's ordered indexes only expose rows that have a value for every
            // sort key. MongoDB sorts documents with missing fields as null, so
            // without this guard a sort-only plan would leak unrelated index
            // documents into the result set.
            pipeline.add(new Document("$match", requiredSortValues));
            if (scoreMaterialized) {
                pipeline.add(new Document("$set", new Document("_score",
                        new Document("$meta", "searchScore"))));
            }
            pipeline.add(new Document("$sort", sort));
        }
        Document projection = new Document(MongoFieldNames.PATH, 1);
        if (hasSearch) {
            projection.append("_score", scoreMaterialized
                    ? 1
                    : new Document("$meta", "searchScore"));
            if (!request.excerptColumns().isEmpty()) {
                projection.append(MongotResultAdapter.HIGHLIGHTS,
                        new Document("$meta", "searchHighlights"));
            }
        }
        if (!request.facetFields().isEmpty()) {
            projection.append(MongoFieldNames.FACET, 1);
        }
        pipeline.add(new Document("$project", projection));
        return pipeline;
    }

    private static Document combineMust(List<Document> operators) {
        if (operators.isEmpty()) {
            return null;
        }
        if (operators.size() == 1) {
            return operators.get(0);
        }
        return new Document("compound", new Document("must", operators));
    }

    private static Document similarityOperator(Document reference) {
        return new Document("moreLikeThis", new Document("like", reference));
    }

    private static Document vectorSimilarityOperator(SimilaritySeed seed,
                                                     Document filter) {
        Document vectorSearch = new Document("path", seed.vectorPath())
                .append("queryVector", seed.vector())
                .append("limit", VECTOR_SEARCH_LIMIT)
                .append("numCandidates", VECTOR_SEARCH_NUM_CANDIDATES);
        if (filter != null) {
            vectorSearch.append("filter", filter);
        }
        return new Document("vectorSearch", vectorSearch);
    }

    private static SimilaritySeed similaritySeed(Document reference,
                                                 MongotIndexDefinition definition) {
        if (reference == null) {
            return null;
        }
        Document text = new Document();
        copySimilarityField(reference, text, MongoFieldNames.FULLTEXT);
        copySimilarityField(reference, text, MongoFieldNames.ANALYZED);
        copySimilarityField(reference, text, MongoFieldNames.RELATIVE_FULLTEXT);
        copySimilarityField(reference, text, MongoFieldNames.DYNAMIC_BOOST_TOKENS);
        copySimilarityField(reference, text, FieldNames.SIMILARITY_TAGS);

        for (IndexDefinition.IndexingRule rule : definition.getDefinedRules()) {
            for (PropertyDefinition property : rule.getSimilarityProperties()) {
                String field = FieldNames.createSimilarityFieldName(
                        MongoFieldNames.encodeProperty(property.name));
                Object stored = reference.get(field);
                if (!(stored instanceof List<?> values) || values.isEmpty()
                        || values.stream().anyMatch(value -> !(value instanceof Number))) {
                    continue;
                }
                List<Number> vector = values.stream()
                        .map(value -> (Number) value)
                        .toList();
                return new SimilaritySeed(text, field, vector);
            }
        }
        return text.isEmpty() ? null : new SimilaritySeed(text, null, List.of());
    }

    private static SimilaritySeed explainSimilaritySeed(String referencePath,
                                                        MongotIndexDefinition definition) {
        Document text = new Document(MongoFieldNames.FULLTEXT, referencePath);
        for (IndexDefinition.IndexingRule rule : definition.getDefinedRules()) {
            for (PropertyDefinition property : rule.getSimilarityProperties()) {
                int dimensions = property.getSimilaritySearchDenseVectorSize();
                if (dimensions > 0) {
                    String field = FieldNames.createSimilarityFieldName(
                            MongoFieldNames.encodeProperty(property.name));
                    return new SimilaritySeed(text, field,
                            Collections.nCopies(dimensions, 0.0d));
                }
            }
        }
        return new SimilaritySeed(text, null, List.of());
    }

    private static void copySimilarityField(Document source, Document target, String field) {
        Object value = source.get(field);
        if (value != null) {
            target.append(field, value);
        }
    }

    private record SimilaritySeed(Document text, String vectorPath, List<Number> vector) {

        private boolean hasVector() {
            return vectorPath != null;
        }
    }

    private static String sortField(IndexPlan plan, OrderEntry entry) {
        FulltextIndexPlanner.PlanResult result = getPlanResult(plan);
        org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition property =
                result.indexingRule.getConfig(entry.getPropertyName());
        String namespace = property != null && property.ordered
                ? MongoFieldNames.ORDERED
                : MongoFieldNames.TYPED;
        return namespace + "." + MongoFieldNames.encodeProperty(entry.getPropertyName());
    }

    private static List<Document> virtualPipeline(IndexPlan plan,
                                                  MongotIndexDefinition definition,
                                                  MongotQueryRequest request) {
        String field;
        Document operator;
        if (request.mode() == MongotQueryRequest.Mode.SUGGEST) {
            field = MongoFieldNames.SUGGEST;
            operator = new Document("autocomplete", new Document("path", field)
                    .append("query", request.term()));
        } else {
            field = MongoFieldNames.SPELLCHECK;
            Document fuzzy = new Document("maxEdits", request.term().length() <= 4 ? 1 : 2)
                    .append("prefixLength", 0)
                    .append("maxExpansions", 50);
            operator = new Document("text", new Document("path", field)
                    .append("query", request.term())
                    .append("fuzzy", fuzzy));
        }
        Document projection = new Document(MongoFieldNames.PATH, 1)
                .append(field, 1)
                .append("_score", new Document("$meta", "searchScore"));
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(searchStage(definition, operator, List.of()));
        MongotQueryTranslation filters = MongotQueryTranslator.translateFilter(
                plan.getFilter(), getPlanResult(plan));
        if (!filters.isSupported()) {
            throw new IllegalStateException("Mongot virtual query became unsupported: "
                    + filters.reason());
        }
        pipeline.addAll(filters.pipeline());
        pipeline.add(new Document("$project", projection));
        return pipeline;
    }
}
