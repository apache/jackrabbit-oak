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

import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.mapping.FieldType;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.mutable.MutableInt;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An {@link ElasticFacetProvider} extension that performs random sampling on the result set to compute facets.
 * SearchHit events are sampled and then used to adjust facets coming from Aggregations in order to minimize
 * access checks. This provider could improve facets performance especially when the result set is quite big.
 */
public class ElasticStatisticalFacetAsyncProvider implements ElasticFacetProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticStatisticalFacetAsyncProvider.class);

    private final ElasticResponseHandler elasticResponseHandler;
    private final Predicate<String> isAccessible;
    private final Set<String> facetFields;
    private final long facetsEvaluationTimeoutMs;
    private Map<String, List<FulltextIndex.Facet>> facets;
    private final SearchRequest searchRequest;
    private final CompletableFuture<Map<String, List<FulltextIndex.Facet>>> searchFuture;
    private int sampled;
    private long totalHits;

    private final long queryStartTimeNanos;
    // All these variables are updated only by the event handler thread of Elastic. They are read either by that
    // same thread or by the client thread that waits for the latch to complete. Since the latch causes a memory barrier,
    // the updated values will be visible to the client thread.
    private long queryTimeNanos;
    private long processAggregationsTimeNanos;
    // It is written by multiple threads, so we use LongAdder for better performance than AtomicLong
    private final LongAdder aclTestTimeNanos = new LongAdder();
    private long processHitsTimeNanos;
    private long computeStatisticalFacetsTimeNanos;

    ElasticStatisticalFacetAsyncProvider(ElasticConnection connection, ElasticIndexDefinition indexDefinition,
                                         ElasticRequestHandler elasticRequestHandler, ElasticResponseHandler elasticResponseHandler,
                                         Predicate<String> isAccessible, int sampleSize, long facetsEvaluationTimeoutMs) {

        this.elasticResponseHandler = elasticResponseHandler;
        this.isAccessible = isAccessible;
        this.facetFields = elasticRequestHandler.facetFields().
                map(ElasticIndexUtils::fieldName).
                collect(Collectors.toUnmodifiableSet());
        this.facetsEvaluationTimeoutMs = facetsEvaluationTimeoutMs;

        this.searchRequest = SearchRequest.of(srb -> srb.index(indexDefinition.getIndexAlias())
                .trackTotalHits(thb -> thb.enabled(true))
                .source(SourceConfig.of(scf -> scf.filter(ff -> ff.includes(FieldNames.PATH).includes(new ArrayList<>(facetFields)))))
                .query(Query.of(qb -> qb.bool(elasticRequestHandler.baseQueryBuilder().build())))
                .aggregations(elasticRequestHandler.aggregations())
                .size(sampleSize)
                .sort(s ->
                        s.field(fs -> fs.field(ElasticIndexDefinition.PATH_RANDOM_VALUE)
                                // this will handle the case when the field is not present in the index
                                .unmappedType(FieldType.Integer)
                        )
                )
        );

        this.queryStartTimeNanos = System.nanoTime();
        LOG.trace("Kicking search query with random sampling {}", searchRequest);
        this.searchFuture = connection.getAsyncClient()
                .search(searchRequest, ObjectNode.class)
                .thenApplyAsync(this::computeFacets);
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
                LOG.error("Error evaluating facets", e);
            } catch (TimeoutException e) {
                searchFuture.cancel(true);
                LOG.error("Timed out while waiting for facets. Search request: {}. {}", searchRequest, timingsToString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();  // restore interrupt status
                throw new IllegalStateException("Error while waiting for facets", e);
            }
        }
        LOG.trace("Reading facets for {} from {}", columnName, facets);
        String field = ElasticIndexUtils.fieldName(FulltextIndex.parseFacetField(columnName));
        return facets != null ? facets.get(field) : null;
    }

    private Map<String, List<FulltextIndex.Facet>> computeFacets(SearchResponse<ObjectNode> searchResponse) {
        LOG.trace("SearchResponse: {}", searchResponse);
        this.queryTimeNanos = System.nanoTime() - queryStartTimeNanos;
        List<Hit<ObjectNode>> searchHits = searchResponse.hits().hits();
        this.sampled = searchHits != null ? searchHits.size() : 0;
        if (sampled > 0) {
            this.totalHits = searchResponse.hits().total() != null ? searchResponse.hits().total().value() : 0;
            Map<String, List<FulltextIndex.Facet>> allFacets = processAggregations(searchResponse.aggregations());
            Map<String, Map<String, MutableInt>> accessibleFacets = new HashMap<>();
            searchResponse.hits().hits().stream()
                    // Possible candidate for parallelization using parallel streams
                    .filter(this::isAccessible)
                    .forEach(hit -> processFilteredHit(hit, accessibleFacets));
            Map<String, List<FulltextIndex.Facet>> facets = computeStatisticalFacets(allFacets, accessibleFacets);
            if (LOG.isDebugEnabled()) {
                LOG.debug(timingsToString());
            }
            return facets;
        } else {
            return Map.of();
        }
    }

    private void processFilteredHit(Hit<ObjectNode> searchHit, Map<String, Map<String, MutableInt>> accessibleFacets) {
        long start = System.nanoTime();
        ObjectNode source = searchHit.source();
        for (String field : facetFields) {
            JsonNode value;
            if (source != null) {
                value = source.get(field);
                if (value != null) {
                    if (value.getNodeType() == JsonNodeType.ARRAY) {
                        for (JsonNode item : value) {
                            updateAccessibleFacets(accessibleFacets, field, item.asText());
                        }
                    } else {
                        updateAccessibleFacets(accessibleFacets, field, value.asText());
                    }
                }
            }
        }
        this.processHitsTimeNanos += System.nanoTime() - start;
    }

    private void updateAccessibleFacets(Map<String, Map<String, MutableInt>> accessibleFacets, String field, String value) {
        accessibleFacets.compute(field, (column, facetValues) -> {
            if (facetValues == null) {
                Map<String, MutableInt> values = new HashMap<>();
                values.put(value, new MutableInt(1));
                return values;
            } else {
                facetValues.compute(value, (k, v) -> {
                    if (v == null) {
                        return new MutableInt(1);
                    } else {
                        v.increment();
                        return v;
                    }
                });
                return facetValues;
            }
        });
    }

    private boolean isAccessible(Hit<ObjectNode> searchHit) {
        long start = System.nanoTime();
        String path = elasticResponseHandler.getPath(searchHit);
        boolean result = path != null && isAccessible.test(path);
        long durationNanos = System.nanoTime() - start;
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);
        if (durationMillis > 10) {
            LOG.debug("Slow path checking ACLs: {}, {} ms", path, durationMillis);
        }
        aclTestTimeNanos.add(durationNanos);
        return result;
    }

    private Map<String, List<FulltextIndex.Facet>> processAggregations(Map<String, Aggregate> aggregations) {
        long start = System.nanoTime();
        Map<String, List<FulltextIndex.Facet>> allFacets = new HashMap<>();
        for (String field : facetFields) {
            List<StringTermsBucket> buckets = aggregations.get(field).sterms().buckets().array();
            allFacets.put(field, buckets.stream()
                    .map(b -> new FulltextIndex.Facet(b.key().stringValue(), (int) b.docCount()))
                    .collect(Collectors.toUnmodifiableList())
            );
        }
        this.processAggregationsTimeNanos = System.nanoTime() - start;
        return allFacets;
    }

    private Map<String, List<FulltextIndex.Facet>> computeStatisticalFacets(Map<String, List<FulltextIndex.Facet>> allFacets, Map<String, Map<String, MutableInt>> accessibleFacetCounts) {
        long start = System.nanoTime();
        for (String facetKey : allFacets.keySet()) {
            if (accessibleFacetCounts.containsKey(facetKey)) {
                Map<String, MutableInt> accessibleFacet = accessibleFacetCounts.get(facetKey);
                List<FulltextIndex.Facet> uncheckedFacet = allFacets.get(facetKey);
                for (FulltextIndex.Facet facet : uncheckedFacet) {
                    MutableInt currCount = accessibleFacet.get(facet.getLabel());
                    if (currCount != null) {
                        double sampleProportion = accessibleFacet.get(facet.getLabel()).doubleValue() / sampled;
                        // returned count is the minimum between the accessible count and the count computed from the sample
                        currCount.setValue(Math.min(facet.getCount(), (int) (sampleProportion * totalHits)));
                    }
                }
            }
        }
        // create Facet objects, order by count (desc) and then by label (asc)
        Comparator<FulltextIndex.Facet> comparator = Comparator
                .comparing(FulltextIndex.Facet::getCount).reversed()
                .thenComparing(FulltextIndex.Facet::getLabel);
        facets = accessibleFacetCounts.entrySet()
                .stream()
                .collect(Collectors.toMap
                        (Map.Entry::getKey, x -> x.getValue().entrySet()
                                .stream()
                                .map(e -> new FulltextIndex.Facet(e.getKey(), e.getValue().intValue()))
                                .sorted(comparator)
                                .collect(Collectors.toList())
                        )
                );
        this.computeStatisticalFacetsTimeNanos = System.nanoTime() - start;
        LOG.trace("Statistical facets {}", facets);
        return facets;
    }

    private String timingsToString() {
        return String.format("Facet computation times: {query: %d ms, processAggregations: %d ms, filterByAcl: %d ms, processHits: %d ms, computeStatisticalFacets: %d ms}. Total hits: %d, samples: %d",
                queryTimeNanos > 0 ? TimeUnit.NANOSECONDS.toMillis(queryTimeNanos) : -1,
                processAggregationsTimeNanos > 0 ? TimeUnit.NANOSECONDS.toMillis(processAggregationsTimeNanos) : -1,
                aclTestTimeNanos.sum() > 0 ? TimeUnit.NANOSECONDS.toMillis(aclTestTimeNanos.sum()) : -1,
                processHitsTimeNanos > 0 ? TimeUnit.NANOSECONDS.toMillis(processHitsTimeNanos) : -1,
                computeStatisticalFacetsTimeNanos > 0 ? TimeUnit.NANOSECONDS.toMillis(computeStatisticalFacetsTimeNanos) : -1,
                totalHits, sampled);
    }
}
