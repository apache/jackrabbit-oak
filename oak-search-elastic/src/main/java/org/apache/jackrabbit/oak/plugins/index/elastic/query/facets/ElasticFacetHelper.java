/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.query.facets;

import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticSearcher;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ElasticFacetHelper {

    private ElasticFacetHelper() {
    }

    public static ElasticFacets getAggregates(ElasticSearcher searcher, QueryBuilder query,
                                              ElasticIndexNode indexNode, QueryIndex.IndexPlan plan,
                                              ElasticAggregationData elasticAggregationData) {
        ElasticFacets elasticFacets;
        SecureFacetConfiguration secureFacetConfiguration = indexNode.getDefinition().getSecureFacetConfiguration();
        switch (secureFacetConfiguration.getMode()) {
            case INSECURE:
                elasticFacets = new InsecureElasticFacets(searcher, query, plan, elasticAggregationData);
                break;
            case STATISTICAL:
                elasticFacets = new StatisticalElasticFacets(searcher, query, plan,
                        secureFacetConfiguration, elasticAggregationData);
                break;
            case SECURE:
            default:
                elasticFacets = new SecureElasticFacets(searcher, query, plan);
                break;
        }
        return elasticFacets;
    }

    public static List<String> getAccessibleDocIds(SearchHit[] searchHits, Filter filter) {
        List<String> accessibleDocs = new LinkedList<>();
        for (SearchHit searchHit : searchHits) {
            final Map<String, Object> sourceMap = searchHit.getSourceAsMap();
            String path = (String) sourceMap.get(FieldNames.PATH);
            if (filter.isAccessible(path)) {
                accessibleDocs.add(path);
            }
        }
        return accessibleDocs;
    }

    public static int getAccessibleDocCount(Iterator<SearchHit> searchHitIterator, Filter filter) {
        int count = 0;
        while (searchHitIterator.hasNext()) {
            SearchHit searchHit = searchHitIterator.next();
            final Map<String, Object> sourceMap = searchHit.getSourceAsMap();
            String path = (String) sourceMap.get(FieldNames.PATH);
            if (filter.isAccessible(path)) {
                count++;
            }
        }
        return count;
    }
}
