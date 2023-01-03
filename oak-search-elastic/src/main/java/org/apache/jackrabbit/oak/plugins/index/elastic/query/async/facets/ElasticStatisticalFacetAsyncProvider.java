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
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResponseListener;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An {@link ElasticSecureFacetAsyncProvider} extension that subscribes also on Elastic Aggregation events.
 * SearchHit events are sampled and then used to adjust facets coming from Aggregations in order to minimize
 * access checks. This provider could improve facets performance but only when the result set is quite big.
 */
public class ElasticStatisticalFacetAsyncProvider extends ElasticSecureFacetAsyncProvider
        implements ElasticResponseListener.AggregationListener {

    private final int sampleSize;
    private long totalHits;

    private final Random rGen;
    private int sampled = 0;
    private int seen = 0;
    private long accessibleCount = 0;

    private final Map<String, List<FulltextIndex.Facet>> facetMap = new HashMap<>();

    private final CountDownLatch latch = new CountDownLatch(1);

    ElasticStatisticalFacetAsyncProvider(ElasticRequestHandler elasticRequestHandler,
                                         ElasticResponseHandler elasticResponseHandler,
                                         Predicate<String> isAccessible,
                                         long randomSeed, int sampleSize) {
        super(elasticRequestHandler, elasticResponseHandler, isAccessible);
        this.sampleSize = sampleSize;
        this.rGen = new Random(randomSeed);
    }

    @Override
    public void startData(long totalHits) {
        this.totalHits = totalHits;
    }

    @Override
    public void on(Hit<ObjectNode> searchHit) {
        if (totalHits < sampleSize) {
            super.on(searchHit);
        } else {
            if (sampleSize == sampled) {
                return;
            }
            int r = rGen.nextInt((int) (totalHits - seen)) + 1;
            seen++;

            if (r <= sampleSize - sampled) {
                sampled++;
                final String path = elasticResponseHandler.getPath(searchHit);
                if (path != null && isAccessible.test(path)) {
                    accessibleCount++;
                }
            }
        }
    }

    @Override
    public void on(Map<String, Aggregate> aggregations) {
        for (String field : facetFields) {
            List<StringTermsBucket> buckets = aggregations.get(field).sterms().buckets().array();
            facetMap.put(field, buckets.stream()
                    .map(b -> new FulltextIndex.Facet(b.key().stringValue(), (int) b.docCount()))
                    .collect(Collectors.toList())
            );
        }
    }

    @Override
    public void endData() {
        if (totalHits < sampleSize) {
            super.endData();
        } else {
            for (String facet: facetMap.keySet()) {
                facetMap.compute(facet, (s, facets1) -> updateLabelAndValueIfRequired(facets1));
            }
            latch.countDown();
        }
    }

    @Override
    public List<FulltextIndex.Facet> getFacets(int numberOfFacets, String columnName) {
        if (totalHits < sampleSize) {
            return super.getFacets(numberOfFacets, columnName);
        } else {
            LOG.trace("Requested facets for {} - Latch count: {}", columnName, latch.getCount());
            try {
                latch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Error while waiting for facets", e);
            }
            LOG.trace("Reading facets for {} from {}", columnName, facetMap);
            return facetMap.get(FulltextIndex.parseFacetField(columnName));
        }
    }

    private List<FulltextIndex.Facet> updateLabelAndValueIfRequired(List<FulltextIndex.Facet> labelAndValues) {
        if (accessibleCount < sampleSize) {
            int numZeros = 0;
            List<FulltextIndex.Facet> newValues;
            {
                List<FulltextIndex.Facet> proportionedLVs = new LinkedList<>();
                for (FulltextIndex.Facet labelAndValue : labelAndValues) {
                    long count = labelAndValue.getCount() * accessibleCount / sampleSize;
                    if (count == 0) {
                        numZeros++;
                    }
                    proportionedLVs.add(new FulltextIndex.Facet(labelAndValue.getLabel(), Math.toIntExact(count)));
                }
                labelAndValues = proportionedLVs;
            }
            if (numZeros > 0) {
                newValues = new LinkedList<>();
                for (FulltextIndex.Facet lv : labelAndValues) {
                    if (lv.getCount() > 0) {
                        newValues.add(lv);
                    }
                }
            } else {
                newValues = labelAndValues;
            }
            return newValues;
        } else {
            return labelAndValues;
        }
    }
}
