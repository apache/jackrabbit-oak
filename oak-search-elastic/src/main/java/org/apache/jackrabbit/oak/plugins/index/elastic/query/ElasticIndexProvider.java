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

import org.apache.jackrabbit.oak.plugins.index.ConfigHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class ElasticIndexProvider implements QueryIndexProvider {

    public static final String ASYNC_ITERATOR_ENQUEUE_TIMEOUT_MS_PROPERTY = "oak.index.elastic.query.asyncIteratorEnqueueTimeoutMs";
    public static final long DEFAULT_ASYNC_ITERATOR_ENQUEUE_TIMEOUT_MS = 60000L; // 60 seconds
    public static final String FACETS_EVALUATION_TIMEOUT_MS_PROPERTY = "oak.index.elastic.query.facetsEvaluationTimeoutMs";
    public static final long DEFAULT_FACETS_EVALUATION_TIMEOUT_MS = 15000L; // 15 seconds

    private final ElasticIndexTracker indexTracker;
    private final long asyncIteratorEnqueueTimeoutMs;
    private final long facetsEvaluationTimeoutMs;

    public ElasticIndexProvider(ElasticIndexTracker indexTracker,
                                long asyncIteratorEnqueueTimeoutMs,
                                long facetsEvaluationTimeoutMs) {
        this.indexTracker = indexTracker;
        this.asyncIteratorEnqueueTimeoutMs = asyncIteratorEnqueueTimeoutMs;
        this.facetsEvaluationTimeoutMs = facetsEvaluationTimeoutMs;
    }

    public ElasticIndexProvider(ElasticIndexTracker indexTracker) {
        this(indexTracker,
                ConfigHelper.getSystemPropertyAsLong(ASYNC_ITERATOR_ENQUEUE_TIMEOUT_MS_PROPERTY, DEFAULT_ASYNC_ITERATOR_ENQUEUE_TIMEOUT_MS),
                ConfigHelper.getSystemPropertyAsLong(FACETS_EVALUATION_TIMEOUT_MS_PROPERTY, DEFAULT_FACETS_EVALUATION_TIMEOUT_MS)
        );
    }

    public long getAsyncIteratorEnqueueTimeoutMs() {
        return asyncIteratorEnqueueTimeoutMs;
    }

    public long getFacetsEvaluationTimeoutMs() {
        return facetsEvaluationTimeoutMs;
    }

    @Override
    public @NotNull List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        return List.of(new ElasticIndex(indexTracker, asyncIteratorEnqueueTimeoutMs, facetsEvaluationTimeoutMs));
    }
}
