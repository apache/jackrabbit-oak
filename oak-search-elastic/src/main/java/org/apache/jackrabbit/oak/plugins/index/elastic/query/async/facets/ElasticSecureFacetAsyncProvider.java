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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.async.facets;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An {@link ElasticFacetProvider} that subscribes to Elastic SearchHit events to return only accessible facets.
 */
class ElasticSecureFacetAsyncProvider implements ElasticFacetProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSecureFacetAsyncProvider.class);

    private final ElasticResponseHandler elasticResponseHandler;
    private final Predicate<String> isAccessible;
    private final Set<String> facetFields;
    private final long facetsEvaluationTimeoutMs;
    private Map<String, List<FulltextIndex.Facet>> facets;
    private final SearchRequest searchRequest;
    private final CompletableFuture<Map<String, List<FulltextIndex.Facet>>> searchFuture;

    private final long queryStartTimeNanos;

    ElasticSecureFacetAsyncProvider(ElasticConnection connection, ElasticIndexDefinition indexDefinition,
                                         ElasticRequestHandler elasticRequestHandler, ElasticResponseHandler elasticResponseHandler,
                                         Predicate<String> isAccessible, long facetsEvaluationTimeoutMs) {

        this.elasticResponseHandler = elasticResponseHandler;
        this.isAccessible = isAccessible;
        this.facetFields = elasticRequestHandler.facetFields().
                map(ElasticIndexUtils::fieldName).
                collect(Collectors.toUnmodifiableSet());
        this.facetsEvaluationTimeoutMs = facetsEvaluationTimeoutMs;

        // Base search request template (without search_after initially)
        this.searchRequest = SearchRequest.of(srb -> srb.index(indexDefinition.getIndexAlias())
                .source(SourceConfig.of(scf -> scf.filter(ff -> ff.includes(FieldNames.PATH).includes(new ArrayList<>(facetFields)))))
                .query(Query.of(qb -> qb.bool(elasticRequestHandler.baseQueryBuilder().build())))
                .size(10000) // batch size for each search_after iteration
                .sort(s -> s.field(fs -> fs.field(FieldNames.PATH)))
        );

        this.queryStartTimeNanos = System.nanoTime();
        LOG.trace("Kicking search query with search_after pagination {}", searchRequest);

        // Start the iterative search process
        this.searchFuture = searchAllResultsIncremental(connection);
    }

    private CompletableFuture<Map<String, List<FulltextIndex.Facet>>> searchAllResultsIncremental(ElasticConnection connection) {
        // Initialize empty facet accumulator
        Map<String, Map<String, Integer>> accumulatedFacets = new ConcurrentHashMap<>();

        return searchWithIncrementalFacetProcessing(connection, null, accumulatedFacets)
                .thenApplyAsync(this::buildFinalFacetResult);
    }

    private CompletableFuture<Map<String, Map<String, Integer>>> searchWithIncrementalFacetProcessing(
            ElasticConnection connection, List<FieldValue> searchAfter, Map<String, Map<String, Integer>> accumulatedFacets) {

        // Build search request with search_after if provided
        SearchRequest currentRequest = searchAfter == null ?
                searchRequest :
                SearchRequest.of(srb -> srb.index(searchRequest.index())
                        .source(searchRequest.source())
                        .query(searchRequest.query())
                        .size(searchRequest.size())
                        .sort(searchRequest.sort())
                        .searchAfter(searchAfter)
                );

        return connection.getAsyncClient()
                .search(currentRequest, ObjectNode.class)
                .thenComposeAsync(response -> {
                    List<Hit<ObjectNode>> hits = response.hits().hits();

                    // Process current page facets and merge with accumulated facets
                    Map<String, Map<String, Integer>> pageFacets = extractFacetsFromPage(hits);
                    mergeFacets(accumulatedFacets, pageFacets);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Processed {} documents in current batch, accumulated facet values: {}",
                                hits.size(), accumulatedFacets.values().stream().mapToInt(Map::size).sum());
                    }

                    // If we got fewer results than requested, we've reached the end
                    if (hits.size() < 10_000) {
                        LOG.trace("Reached end of results, final facet processing complete");
                        return CompletableFuture.completedFuture(accumulatedFacets);
                    }

                    // Extract sort values from the last hit for next search_after
                    Hit<ObjectNode> lastHit = hits.get(hits.size() - 1);
                    List<FieldValue> nextSearchAfter = lastHit.sort();

                    if (nextSearchAfter == null || nextSearchAfter.isEmpty()) {
                        LOG.warn("No sort values found for search_after, stopping pagination");
                        return CompletableFuture.completedFuture(accumulatedFacets);
                    }

                    // Recursively continue with next batch
                    return searchWithIncrementalFacetProcessing(connection, nextSearchAfter, accumulatedFacets);
                })
                .exceptionally(throwable -> {
                    LOG.error("Error during search pagination", throwable);
                    // Return accumulated facets even if there's an error
                    return accumulatedFacets;
                });
    }

    private Map<String, Map<String, Integer>> extractFacetsFromPage(List<Hit<ObjectNode>> hits) {
        Map<String, Map<String, Integer>> pageFacets = new HashMap<>();

        for (Hit<ObjectNode> hit : hits) {
            ObjectNode document = hit.source();
            if (document == null) continue;

            // Apply accessibility check
            String path = elasticResponseHandler.getPath(hit);
            if (!isAccessible.test(path)) continue;

            // Extract facet field values
            for (String facetField : facetFields) {
                JsonNode fieldValue = document.get(facetField);
                if (fieldValue != null && !fieldValue.isNull()) {
                    pageFacets.compute(facetField, (key, valueMap) -> {
                        if (valueMap == null) {
                            valueMap = new HashMap<>();
                        }
                        if (fieldValue.isArray()) {
                            for (JsonNode val : fieldValue) {
                                if (val.isTextual()) {
                                    valueMap.merge(val.asText(), 1, Integer::sum);
                                }
                            }
                        } else if (fieldValue.isTextual()) {
                            valueMap.merge(fieldValue.asText(), 1, Integer::sum);
                        }
                        return valueMap;
                    });
                }
            }
        }

        return pageFacets;
    }

    private void mergeFacets(Map<String, Map<String, Integer>> accumulated, Map<String, Map<String, Integer>> pageFacets) {
        for (Map.Entry<String, Map<String, Integer>> entry : pageFacets.entrySet()) {
            Map<String, Integer> valueMap = accumulated.computeIfAbsent(entry.getKey(), k -> new HashMap<>());
            for (Map.Entry<String, Integer> valueEntry : entry.getValue().entrySet()) {
                valueMap.merge(valueEntry.getKey(), valueEntry.getValue(), Integer::sum);
            }
        }
    }

    private Map<String, List<FulltextIndex.Facet>> buildFinalFacetResult(Map<String, Map<String, Integer>> accumulatedFacets) {
        long elapsedMs = (System.nanoTime() - queryStartTimeNanos) / 1_000_000;
        LOG.debug("Facet computation completed in {}ms", elapsedMs);

        // Convert accumulated facet values to final result format
        Map<String, List<FulltextIndex.Facet>> result = new HashMap<>();

        for (Map.Entry<String, Map<String, Integer>> entry : accumulatedFacets.entrySet()) {
            String fieldName = entry.getKey();
            List<FulltextIndex.Facet> facets = entry.getValue().entrySet().stream()
                    .map(e -> new FulltextIndex.Facet(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
            result.put(fieldName, facets);
        }

        return result;
    }

    @Override
    public List<FulltextIndex.Facet> getFacets(int numberOfFacets, String columnName) {
        // TODO: In case of failure, we log an exception and return null. This is likely not the ideal behavior, as the
        //   caller has no way to distinguish between a failure and empty results. But in this PR I'm leaving this
        //   behavior as is to not introduce further changes. We should revise this behavior once the queries for facets
        //   are decoupled from the query for results, as this will make it easier to better handle errors
        if (!searchFuture.isDone()) {
            try {
                LOG.trace("Requested facets for {}. Waiting up to: {}", columnName, facetsEvaluationTimeoutMs);
                long start = System.nanoTime();
                facets = searchFuture.get(facetsEvaluationTimeoutMs, TimeUnit.MILLISECONDS);
                LOG.trace("Facets computed in {}.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            } catch (ExecutionException e) {
                LOG.error("Error evaluating facets. Ignoring. Search request: {}", searchRequest, e);
            } catch (TimeoutException e) {
                searchFuture.cancel(true);
                LOG.error("Timed out while waiting for facets. Search request: {}", searchRequest);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();  // restore interrupt status
                throw new IllegalStateException("Error while waiting for facets. Search request: {}", searchRequest, e);
            }
        }
        LOG.trace("Reading facets for {} from {}", columnName, facets);
        String field = ElasticIndexUtils.fieldName(FulltextIndex.parseFacetField(columnName));
        return facets != null ? facets.get(field) : null;
    }
}
