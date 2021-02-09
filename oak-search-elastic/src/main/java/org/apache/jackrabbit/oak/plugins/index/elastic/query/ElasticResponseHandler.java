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
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.elasticsearch.client.Response;
import org.elasticsearch.search.SearchHit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Class to process Elastic response objects.
 */
public class ElasticResponseHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticResponseHandler.class);

    private static final ObjectMapper JSON_MAPPER;
    static {
        // disable String.intern
        // https://github.com/elastic/elasticsearch/issues/39890
        // https://github.com/FasterXML/jackson-core/issues/332
        JsonFactoryBuilder factoryBuilder = new JsonFactoryBuilder();
        factoryBuilder.disable(JsonFactory.Feature.INTERN_FIELD_NAMES);
        JSON_MAPPER = new ObjectMapper(factoryBuilder.build());
        JSON_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final PlanResult planResult;
    private final Filter filter;

    ElasticResponseHandler(@NotNull FulltextIndexPlanner.PlanResult planResult, @NotNull Filter filter) {
        this.planResult = planResult;
        this.filter = filter;
    }

    public String getPath(SearchResponseHit hit) {
        return transformPath((String) hit.source.get(FieldNames.PATH));
    }

    public String getPath(SearchHit hit) {
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        return transformPath((String) sourceMap.get(FieldNames.PATH));
    }

    private String transformPath(String path) {
        String transformedPath = planResult.transformPath(("".equals(path)) ? "/" : path);

        if (transformedPath == null) {
            LOG.trace("Ignoring path {} : Transformation returned null", path);
            return null;
        }

        return transformedPath;
    }

    public boolean isAccessible(String path) {
        return filter.isAccessible(path);
    }

    public SearchResponse parse(Response response) throws IOException {
        return JSON_MAPPER.readValue(response.getEntity().getContent(), SearchResponse.class);
    }

    // POJO for Elastic json deserialization

    public static class SearchResponse {

        public final long took;
        public final boolean timedOut;
        public final SearchResponseHits hits;
        public final Map<String, AggregationBuckets> aggregations;

        @JsonCreator
        public SearchResponse(@JsonProperty("took") long took, @JsonProperty("timed_out") boolean timedOut,
                              @JsonProperty("hits") SearchResponseHits hits,
                              @JsonProperty("aggregations") Map<String, AggregationBuckets> aggregations) {
            this.took = took;
            this.timedOut = timedOut;
            this.hits = hits;
            this.aggregations = aggregations;
        }

    }

    public static class SearchResponseHits {

        public final SearchResponseHitsTotal total;
        public final SearchResponseHit[] hits;

        @JsonCreator
        public SearchResponseHits(@JsonProperty("total") SearchResponseHitsTotal total,
                                  @JsonProperty("hits") SearchResponseHit[] hits) {
            this.total = total;
            this.hits = hits;
        }

    }

    public static class SearchResponseHit {

        public final Map<String, Object> source;
        public final Object[] sort;
        public final double score;

        @JsonCreator
        public SearchResponseHit(@JsonProperty("_source") Map<String, Object> source,
                                 @JsonProperty("sort") Object[] sort, @JsonProperty("_score") double score) {
            this.source = source;
            this.sort = sort;
            this.score = score;
        }

    }

    public static class SearchResponseHitsTotal {
        public final long value;

        @JsonCreator
        public SearchResponseHitsTotal(@JsonProperty("value") long value) {
            this.value = value;
        }
    }

    public static class AggregationBuckets {

        public final AggregationBucket[] buckets;

        @JsonCreator
        public AggregationBuckets(@JsonProperty("buckets") AggregationBucket[] buckets) {
            this.buckets = buckets;
        }

    }

    public static class AggregationBucket {

        public final Object key;
        public final int count;

        @JsonCreator
        public AggregationBucket(@JsonProperty("key") Object key, @JsonProperty("doc_count") int count) {
            this.key = key;
            this.count = count;
        }

    }
}
