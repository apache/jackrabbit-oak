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

import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResponseListener;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * An {@link ElasticFacetProvider} that subscribes to Elastic Aggregation events.
 * The provider is async: {@code getFacets} waits until the aggregation is read or for a max of 15 seconds. In the latter
 * case, an {@link IllegalStateException} is thrown.
 */
class ElasticInsecureFacetAsyncProvider implements ElasticFacetProvider, ElasticResponseListener.AggregationListener {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticInsecureFacetAsyncProvider.class);

    private Map<String, ElasticResponseHandler.AggregationBuckets> aggregations;

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public List<FulltextIndex.Facet> getFacets(int numberOfFacets, String columnName) {
        LOG.trace("Requested facets for {} - Latch count: {}", columnName, latch.getCount());
        try {
            latch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Error while waiting for facets", e);
        }
        LOG.trace("Reading facets for {} from aggregations {}", columnName, aggregations);
        if (aggregations != null) {
            final String facetProp = FulltextIndex.parseFacetField(columnName);
            ElasticResponseHandler.AggregationBuckets terms = aggregations.get(facetProp);
            List<FulltextIndex.Facet> facets = new ArrayList<>(terms.buckets.length);
            for (ElasticResponseHandler.AggregationBucket bucket : terms.buckets) {
                facets.add(new FulltextIndex.Facet(bucket.key.toString(), bucket.count));
            }
            return facets;
        } else return null;
    }

    @Override
    public void on(Map<String, ElasticResponseHandler.AggregationBuckets> aggregations) {
        this.aggregations = aggregations;
        this.endData();
    }

    @Override
    public void endData() {
        latch.countDown();
    }
}
