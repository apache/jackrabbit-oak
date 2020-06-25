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

import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResponseListener;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;

import java.util.function.Predicate;

/**
 * Provider of facets through an {@link ElasticResponseListener}
 */
public interface ElasticFacetProvider extends FulltextIndex.FacetProvider, ElasticResponseListener {

    /**
     * Returns the appropriate provider based on the {@link SecureFacetConfiguration}
     * @param facetConfiguration the {@link SecureFacetConfiguration} to extract facet options
     * @param requestHandler the {@link ElasticRequestHandler} to perform actions at request time
     * @param responseHandler the {@link ElasticResponseHandler} to decode responses
     * @param isAccessible a {@link Predicate} to check if a node is accessible
     *
     * @return an {@link ElasticFacetProvider} based on {@link SecureFacetConfiguration#getMode()} with
     * {@link SecureFacetConfiguration.MODE#SECURE} as default
     */
    static ElasticFacetProvider getProvider(
            SecureFacetConfiguration facetConfiguration,
            ElasticRequestHandler requestHandler,
            ElasticResponseHandler responseHandler,
            Predicate<String> isAccessible
    ) {
        final ElasticFacetProvider facetProvider;
        switch (facetConfiguration.getMode()) {
            case INSECURE:
                facetProvider = new ElasticInsecureFacetAsyncProvider();
                break;
            case STATISTICAL:
                facetProvider = new ElasticStatisticalFacetAsyncProvider(
                        requestHandler, responseHandler, isAccessible,
                        facetConfiguration.getRandomSeed(), facetConfiguration.getStatisticalFacetSampleSize()
                );
                break;
            case SECURE:
            default:
                facetProvider = new ElasticSecureFacetAsyncProvider(requestHandler, responseHandler, isAccessible);

        }
        return facetProvider;
    }

}
