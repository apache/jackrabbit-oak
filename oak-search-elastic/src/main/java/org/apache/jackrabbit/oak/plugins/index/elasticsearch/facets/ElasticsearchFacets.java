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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.facets;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcher;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.index.query.QueryBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ElasticsearchFacets {

    /**
     * @return ElasticsearchSearcher
     */
    ElasticsearchSearcher getSearcher();

    /**
     * @return QueryBuilder
     */
    QueryBuilder getQuery();

    /**
     * @return QueryIndex.IndexPlan
     */
    QueryIndex.IndexPlan getPlan();

    /**
     * @param numberOfFacets number of topFacets to be returned
     * @return A map with facetName as key and List of facets in descending order of facetCount.
     * @throws IOException
     */
    Map<String, List<FulltextIndex.Facet>> getElasticSearchFacets(int numberOfFacets) throws IOException;

    /**
     * We can retrieve Aggregation in a single call to elastic search while querying. Which can then be passed
     * to ElasticSearchfacets instead of calling ES again to fetch same info. If ElasticsearchAggregationData is null
     * then we get data by again querying ES
     *
     * @return ElasticsearchAggregationData
     */
    @Nullable
    ElasticsearchAggregationData getElasticsearchAggregationData();

    class ElasticSearchFacet {

        private final String label;
        private final Long count;

        public ElasticSearchFacet(String label, Long count) {
            this.label = label;
            this.count = count;
        }

        @NotNull
        public String getLabel() {
            return label;
        }

        public Long getCount() {
            return count;
        }

        public FulltextIndex.Facet convertToFacet() {
            return new FulltextIndex.Facet(this.getLabel(), Math.toIntExact(this.getCount()));
        }
    }
}
