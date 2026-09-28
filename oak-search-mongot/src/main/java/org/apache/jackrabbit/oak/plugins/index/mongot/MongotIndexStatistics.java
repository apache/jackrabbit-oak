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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.time.Duration;

import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.LoadingCache;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.bson.Document;

import static com.mongodb.client.model.Filters.exists;

public final class MongotIndexStatistics implements IndexStatistics {

    private static final long MAX_SIZE = Long.getLong("oak.mongot.statsMaxSize", 10_000L);
    private static final long EXPIRE_SECONDS = Long.getLong("oak.mongot.statsExpireSeconds", 10 * 60L);
    private static final long REFRESH_SECONDS = Long.getLong("oak.mongot.statsRefreshSeconds", 60L);
    private static final String ALL_DOCUMENTS = "";

    private final MongoConnection connection;
    private final MongotIndexDefinition definition;
    // Oak's planner reads these counts for every query. Like the Elasticsearch index, serve
    // them from a cache so planning does not cost a round trip to MongoDB per count.
    private final LoadingCache<String, Integer> counts;

    MongotIndexStatistics(MongoConnection connection, MongotIndexDefinition definition) {
        this.connection = connection;
        this.definition = definition;
        this.counts = CacheBuilder.<String, Integer>newBuilder()
                .maximumSize(MAX_SIZE)
                .expireAfterWrite(Duration.ofSeconds(EXPIRE_SECONDS))
                .refreshAfterWrite(Duration.ofSeconds(REFRESH_SECONDS))
                .build(this::countDocuments);
    }

    @Override
    public int numDocs() {
        return counts.get(ALL_DOCUMENTS);
    }

    @Override
    public int getDocCountFor(String key) {
        return counts.get(FieldNames.NULL_PROPS.equals(key)
                ? MongoFieldNames.NULL_PROPERTIES
                : MongoFieldNames.TYPED + "." + MongoFieldNames.encodeProperty(key));
    }

    private int countDocuments(String field) {
        long count = connection.getCollection(definition).countDocuments(
                ALL_DOCUMENTS.equals(field) ? new Document() : exists(field));
        return (int) Math.min(Integer.MAX_VALUE, count);
    }
}
