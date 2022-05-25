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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.async;

import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Generic listener of Elastic response
 */
public interface ElasticResponseListener {

    Set<String> DEFAULT_SOURCE_FIELDS = Collections.singleton(FieldNames.PATH);

    /**
     * Returns the source fields this listener is interested on
     *
     * @return the list of fields to listen to (only PATH as default)
     */
    default Set<String> sourceFields() {
        return DEFAULT_SOURCE_FIELDS;
    }

    /**
     * This method is invoked when there is no more data to process.
     */
    void endData();

    /**
     * {@link ElasticResponseListener} extension to subscribe on response hit events
     */
    interface SearchHitListener extends ElasticResponseListener {

        /**
         * Returns {@code true} if the listener is interested in the entire result set
         */
        default boolean isFullScan() {
            return false;
        }

        /**
         * This method is invoked at the beginning of the listener lifecycle to notify the number of hits this
         * listener could receive
         * @param totalHits the total number of hits
         */
        default void startData(long totalHits) { /*empty*/ }

        /**
         * This method is called for each {@link Hit} retrieved
         * @param searchHit a search result
         */
        void on(Hit<ObjectNode> searchHit);
    }

    /**
     * {@link ElasticResponseListener} extension to subscribe on aggregations events
     */
    interface AggregationListener extends ElasticResponseListener {

        /**
         * This method is called once when the aggregations are retrieved
         * @param aggregations the {@link Map} with aggregations or {@code null} if there are no results
         */
        void on(Map<String, Aggregate> aggregations);
    }
}
