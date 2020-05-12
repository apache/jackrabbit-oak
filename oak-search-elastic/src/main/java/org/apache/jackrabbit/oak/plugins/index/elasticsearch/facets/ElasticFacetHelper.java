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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.facets;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcher;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ElasticFacetHelper {

    private ElasticFacetHelper() {
    }

    public static ElasticsearchFacets getAggregates(ElasticsearchSearcher searcher, QueryBuilder query,
                                                    ElasticsearchIndexNode indexNode, QueryIndex.IndexPlan plan,
                                                    ElasticsearchAggregationData elasticsearchAggregationData) {
        ElasticsearchFacets elasticsearchFacets;
        SecureFacetConfiguration secureFacetConfiguration = indexNode.getDefinition().getSecureFacetConfiguration();
        switch (secureFacetConfiguration.getMode()) {
            case INSECURE:
                elasticsearchFacets = new InsecureElasticSearchFacets(searcher, query, plan, elasticsearchAggregationData);
                break;
            case STATISTICAL:
                elasticsearchFacets = new StatisticalElasticSearchFacets(searcher, query, plan,
                        secureFacetConfiguration, elasticsearchAggregationData);
                break;
            case SECURE:
            default:
                elasticsearchFacets = new SecureElasticSearchFacets(searcher, query, plan);
                break;
        }
        return elasticsearchFacets;
    }

    public static List<String> getAccessibleDocIds(SearchHit[] searchHits, Filter filter) throws UnsupportedEncodingException {
        List<String> accessibleDocs = new LinkedList<>();
        for (SearchHit searchHit : searchHits) {
            String id = searchHit.getId();
            String path = idToPath(id);
            if (filter.isAccessible(path)) {
                accessibleDocs.add(id);
            }
        }
        return accessibleDocs;
    }

    public static int getAccessibleDocCount(Iterator<SearchHit> searchHitIterator, Filter filter) throws UnsupportedEncodingException {
        int count = 0;
        while (searchHitIterator.hasNext()) {
            SearchHit searchHit = searchHitIterator.next();
            String id = searchHit.getId();
            String path = idToPath(id);
            if (filter.isAccessible(path)) {
                count++;
            }
        }
        return count;
    }

    public static String idToPath(String id) throws UnsupportedEncodingException {
        return URLDecoder.decode(id, "UTF-8");
    }
}
