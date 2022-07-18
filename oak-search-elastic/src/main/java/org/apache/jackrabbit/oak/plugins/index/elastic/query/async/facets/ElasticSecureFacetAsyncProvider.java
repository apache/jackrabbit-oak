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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResponseListener;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    protected static final Logger LOG = LoggerFactory.getLogger(ElasticSecureFacetAsyncProvider.class);

    protected final Set<String> facetFields;
    private final Map<String, Map<String, Integer>> facetsMap = new ConcurrentHashMap<>();
    private Map<String, List<FulltextIndex.Facet>> facets;
    protected final ElasticResponseHandler elasticResponseHandler;
    protected final Predicate<String> isAccessible;

    private final CountDownLatch latch = new CountDownLatch(1);

    ElasticSecureFacetAsyncProvider(
            ElasticRequestHandler elasticRequestHandler,
            ElasticResponseHandler elasticResponseHandler,
            Predicate<String> isAccessible
    ) {
        this.elasticResponseHandler = elasticResponseHandler;
        this.isAccessible = isAccessible;
        this.facetFields = elasticRequestHandler.facetFields().collect(Collectors.toSet());
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
    public void on(Hit<ObjectNode> searchHit) {
        final String path = elasticResponseHandler.getPath(searchHit);
        if (path != null && isAccessible.test(path)) {
            for (String field: facetFields) {
                JsonNode value = searchHit.source().get(field);
                if (value != null) {
                    facetsMap.compute(field, (column, facetValues) -> {
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

    @Override
    public void endData() {
        // create Facet objects, order by count (desc) and then by label (asc)
        facets = facetsMap.entrySet()
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
        LOG.trace("End data {}", facetsMap);
        latch.countDown();
    }

    @Override
    public List<FulltextIndex.Facet> getFacets(int numberOfFacets, String columnName) {
        LOG.trace("Requested facets for {} - Latch count: {}", columnName, latch.getCount());
        try {
            latch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Error while waiting for facets", e);
        }
        LOG.trace("Reading facets for {} from {}", columnName, facets);
        return facets != null ? facets.get(FulltextIndex.parseFacetField(columnName)) : null;
    }
}
