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

import org.elasticsearch.search.aggregations.Aggregations;

/*
    This class's object is used in facets to save unnecessary call to Elasticsearch
    as this info is also retrieved when calling ES in rowIterator.
 */
public class ElasticsearchAggregationData {
    private int numberOfFacets; // topFacet count from indexDefinition
    private long totalDocuments; // total documents in query result.
    private Aggregations aggregations; // Aggregated data for query from ES

    public ElasticsearchAggregationData(int numberOfFacets, long totalDocuments, Aggregations aggregations) {
        this.numberOfFacets = numberOfFacets;
        this.totalDocuments = totalDocuments;
        this.aggregations = aggregations;
    }

    public int getNumberOfFacets() {
        return numberOfFacets;
    }

    public long getTotalDocuments() {
        return totalDocuments;
    }

    public Aggregations getAggregations() {
        return aggregations;
    }
}
