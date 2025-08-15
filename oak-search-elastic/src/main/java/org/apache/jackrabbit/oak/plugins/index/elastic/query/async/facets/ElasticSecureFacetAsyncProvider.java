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

import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResponseListener;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An {@link ElasticFacetProvider} that subscribes to Elastic SearchHit events to return only accessible facets.
 */
class ElasticSecureFacetAsyncProvider implements ElasticFacetProvider, ElasticResponseListener.SearchHitListener {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSecureFacetAsyncProvider.class);

    private final Set<String> facetFields;
    private final long facetsEvaluationTimeoutMs;
    private final Map<String, Map<String, MutableInt>> accessibleFacets = new ConcurrentHashMap<>();
    private final ElasticResponseHandler elasticResponseHandler;
    private final Predicate<String> isAccessible;
    private final CountDownLatch latch = new CountDownLatch(1);
    private Map<String, List<FulltextIndex.Facet>> facets;

    ElasticSecureFacetAsyncProvider(
            ElasticRequestHandler elasticRequestHandler,
            ElasticResponseHandler elasticResponseHandler,
            Predicate<String> isAccessible,
            long facetsEvaluationTimeoutMs) {
        this.elasticResponseHandler = elasticResponseHandler;
        this.isAccessible = isAccessible;
        this.facetFields = elasticRequestHandler.facetFields().
                map(ElasticIndexUtils::fieldName).
                collect(Collectors.toUnmodifiableSet());
        this.facetsEvaluationTimeoutMs = facetsEvaluationTimeoutMs;
    }

    @Override
    public Set<String> sourceFields() {
        return facetFields;
    }

    @Override
    public boolean isFullScan() {
        return true;
    }

    @Override
    public boolean on(Hit<ObjectNode> searchHit) {
        final String path = elasticResponseHandler.getPath(searchHit);
        if (path != null && isAccessible.test(path)) {
            ObjectNode source = searchHit.source();
            for (String field : facetFields) {
                JsonNode value;
                if (source != null) {
                    value = source.get(field);
                    if (value != null) {
                        if (value.getNodeType() == JsonNodeType.ARRAY) {
                            for (JsonNode item : value) {
                                updateAccessibleFacets(field, item.asText());
                            }
                        } else {
                            updateAccessibleFacets(field, value.asText());
                        }
                    }
                }
            }
        }
        return true;
    }

    private void updateAccessibleFacets(String field, String value) {
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

    @Override
    public void endData() {
        // create Facet objects, order by count (desc) and then by label (asc)
        Comparator<FulltextIndex.Facet> comparator = Comparator
                .comparing(FulltextIndex.Facet::getCount).reversed()
                .thenComparing(FulltextIndex.Facet::getLabel);
        // create Facet objects, order by count (desc) and then by label (asc)
        facets = accessibleFacets.entrySet()
                .stream()
                .collect(Collectors.toMap
                        (Map.Entry::getKey, x -> x.getValue().entrySet()
                                .stream()
                                .map(e -> new FulltextIndex.Facet(e.getKey(), e.getValue().intValue()))
                                .sorted(comparator)
                                .collect(Collectors.toList())
                        )
                );
        LOG.trace("End data {}", facets);
        latch.countDown();
    }

    @Override
    public List<FulltextIndex.Facet> getFacets(int numberOfFacets, String columnName) {
        LOG.trace("Requested facets for {} - Latch count: {}", columnName, latch.getCount());
        try {
            boolean completed = latch.await(facetsEvaluationTimeoutMs, TimeUnit.MILLISECONDS);
            if (!completed) {
                throw new IllegalStateException("Timed out while waiting for facets");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // restore interrupt status
            throw new IllegalStateException("Error while waiting for facets", e);
        }
        LOG.trace("Reading facets for {} from {}", columnName, facets);
        String field = ElasticIndexUtils.fieldName(FulltextIndex.parseFacetField(columnName));
        return facets != null ? facets.get(field) : null;
    }
}
