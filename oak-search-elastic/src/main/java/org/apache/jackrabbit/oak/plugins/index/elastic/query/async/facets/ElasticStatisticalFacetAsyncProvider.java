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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.mapping.FieldType;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    private final Map<String, List<FulltextIndex.Facet>> allFacets = new HashMap<>();
    private final Map<String, Map<String, Integer>> accessibleFacetCounts = new ConcurrentHashMap<>();
    private Map<String, List<FulltextIndex.Facet>> facets;
    private final CountDownLatch latch = new CountDownLatch(1);
    private int sampled;
    private long totalHits;

    ElasticStatisticalFacetAsyncProvider(ElasticConnection connection, ElasticIndexDefinition indexDefinition,
                                         ElasticRequestHandler elasticRequestHandler, ElasticResponseHandler elasticResponseHandler,
                                         Predicate<String> isAccessible, long randomSeed, int sampleSize) {

        this.elasticResponseHandler = elasticResponseHandler;
        this.isAccessible = isAccessible;
        this.facetFields = elasticRequestHandler.facetFields().collect(Collectors.toSet());

        SearchRequest searchRequest = SearchRequest.of(srb -> srb.index(indexDefinition.getIndexAlias())
                .trackTotalHits(thb -> thb.enabled(true))
                .source(SourceConfig.of(scf -> scf.filter(ff -> ff.includes(FieldNames.PATH).includes(new ArrayList<>(facetFields)))))
                .query(Query.of(qb -> qb.bool(elasticRequestHandler.baseQueryBuilder().build())))
                .aggregations(elasticRequestHandler.aggregations())
                .size(sampleSize)
                .sort(s ->
                        s.field(fs -> fs.field(
                                ElasticIndexDefinition.PATH_RANDOM_VALUE)
                                // this will handle the case when the field is not present in the index
                                .unmappedType(FieldType.Integer)
                        )
                )
        );

        LOG.trace("Kicking search query with random sampling {}", searchRequest);
        CompletableFuture<SearchResponse<ObjectNode>> searchFuture =
                connection.getAsyncClient().search(searchRequest, ObjectNode.class);

        searchFuture.whenCompleteAsync((searchResponse, throwable) -> {
            try {
                if (throwable != null) {
                    LOG.error("Error while retrieving sample documents", throwable);
                } else {
                    List<Hit<ObjectNode>> searchHits = searchResponse.hits().hits();
                    this.sampled = searchHits != null ? searchHits.size() : 0;
                    if (sampled > 0) {
                        this.totalHits = searchResponse.hits().total().value();
                        processAggregations(searchResponse.aggregations());
                        searchResponse.hits().hits().forEach(this::processHit);
                        computeStatisticalFacets();
                    }
                }
            } finally {
                latch.countDown();
            }
        });
    }

    @Override
    public List<FulltextIndex.Facet> getFacets(int numberOfFacets, String columnName) {
        LOG.trace("Requested facets for {} - Latch count: {}", columnName, latch.getCount());
        try {
            boolean completed = latch.await(15, TimeUnit.SECONDS);
            if (!completed) {
                throw new IllegalStateException("Timed out while waiting for facets");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // restore interrupt status
            throw new IllegalStateException("Error while waiting for facets", e);
        }
        LOG.trace("Reading facets for {} from {}", columnName, facets);
        return facets != null ? facets.get(FulltextIndex.parseFacetField(columnName)) : null;
    }

    private void processHit(Hit<ObjectNode> searchHit) {
        final String path = elasticResponseHandler.getPath(searchHit);
        if (path != null && isAccessible.test(path)) {
            for (String field : facetFields) {
                JsonNode value = searchHit.source().get(field);
                if (value != null) {
                    accessibleFacetCounts.compute(field, (column, facetValues) -> {
                        if (facetValues == null) {
                            Map<String, Integer> values = new HashMap<>();
                            values.put(value.asText(), 1);
                            return values;
                        } else {
                            facetValues.merge(value.asText(), 1, Integer::sum);
                            return facetValues;
                        }
                    });
                }
            }
        }
    }

    private void processAggregations(Map<String, Aggregate> aggregations) {
        for (String field : facetFields) {
            List<StringTermsBucket> buckets = aggregations.get(field).sterms().buckets().array();
            allFacets.put(field, buckets.stream()
                    .map(b -> new FulltextIndex.Facet(b.key().stringValue(), (int) b.docCount()))
                    .collect(Collectors.toList())
            );
        }
    }

    private void computeStatisticalFacets() {
        for (String facetKey : allFacets.keySet()) {
            if (accessibleFacetCounts.containsKey(facetKey)) {
                Map<String, Integer> accessibleFacet = accessibleFacetCounts.get(facetKey);
                List<FulltextIndex.Facet> uncheckedFacet = allFacets.get(facetKey);
                for (FulltextIndex.Facet facet : uncheckedFacet) {
                    if (accessibleFacet.containsKey(facet.getLabel())) {
                        double sampleProportion = (double) accessibleFacet.get(facet.getLabel()) / sampled;
                        // returned count is the minimum between the accessible count and the count computed from the sample
                        accessibleFacet.put(facet.getLabel(), Math.min(facet.getCount(), (int) (sampleProportion * totalHits)));
                    }
                }
            }
        }
        // create Facet objects, order by count (desc) and then by label (asc)
        facets = accessibleFacetCounts.entrySet()
                .stream()
                .collect(Collectors.toMap
                        (Map.Entry::getKey, x -> x.getValue().entrySet()
                                .stream()
                                .map(e -> new FulltextIndex.Facet(e.getKey(), e.getValue()))
                                .sorted((f1, f2) -> {
                                    int f1Count = f1.getCount();
                                    int f2Count = f2.getCount();
                                    if (f1Count == f2Count) {
                                        return f1.getLabel().compareTo(f2.getLabel());
                                    } else return f2Count - f1Count;
                                })
                                .collect(Collectors.toList())
                        )
                );
        LOG.trace("Statistical facets {}", facets);
    }

}
