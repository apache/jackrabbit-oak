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

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElasticResultRowAsyncIteratorParsingErrorTest {

    @Test
    public void nonElasticExceptionIsNotAParsingError() {
        assertFalse(ElasticResultRowAsyncIterator.isQueryParsingError(new IOException("connection reset")));
        assertFalse(ElasticResultRowAsyncIterator.isQueryParsingError(null));
    }

    @Test
    public void directParsingExceptionIsDetected() {
        ElasticsearchException ex = elasticException("parsing_exception", "Cannot parse '...'", null);
        assertTrue(ElasticResultRowAsyncIterator.isQueryParsingError(ex));
    }

    @Test
    public void nestedQueryShardParsingErrorIsDetected() {
        // Mirrors the typical shape returned by Elastic for a malformed query_string term:
        // search_phase_execution_exception -> query_shard_exception -> parse_exception
        ElasticsearchException ex = elasticException(
                "search_phase_execution_exception", "all shards failed",
                elasticCause("query_shard_exception", "Failed to parse query [...]",
                        elasticCause("parse_exception", "Cannot parse '...'  ", null)));
        assertTrue(ElasticResultRowAsyncIterator.isQueryParsingError(ex));
    }

    @Test
    public void queryShardExceptionReasonWithoutExplicitParseTypeIsDetected() {
        ElasticsearchException ex = elasticException(
                "search_phase_execution_exception", "all shards failed",
                elasticCause("query_shard_exception", "Failed to parse query [foo}]", null));
        assertTrue(ElasticResultRowAsyncIterator.isQueryParsingError(ex));
    }

    @Test
    public void genuineSystemErrorIsNotAParsingError() {
        ElasticsearchException ex = elasticException(
                "search_phase_execution_exception", "all shards failed",
                elasticCause("node_disconnected_exception", "node disconnected", null));
        assertFalse(ElasticResultRowAsyncIterator.isQueryParsingError(ex));
    }

    private static co.elastic.clients.elasticsearch._types.ErrorCause elasticCause(String type, String reason,
            co.elastic.clients.elasticsearch._types.ErrorCause causedBy) {
        return co.elastic.clients.elasticsearch._types.ErrorCause.of(b -> {
            b.type(type).reason(reason);
            if (causedBy != null) {
                b.causedBy(causedBy);
            }
            return b;
        });
    }

    private static ElasticsearchException elasticException(String type, String reason,
            co.elastic.clients.elasticsearch._types.ErrorCause causedBy) {
        ErrorResponse response = ErrorResponse.of(b -> b
                .status(400)
                .error(e -> {
                    e.type(type).reason(reason);
                    if (causedBy != null) {
                        e.causedBy(causedBy);
                    }
                    return e;
                }));
        return new ElasticsearchException("search", response);
    }
}
