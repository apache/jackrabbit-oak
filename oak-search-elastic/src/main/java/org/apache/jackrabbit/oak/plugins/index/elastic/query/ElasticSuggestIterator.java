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

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * This class is in charge to extract suggestions for a given query. Suggestion is more like
 * a completion result.
 */
class ElasticSuggestIterator implements Iterator<FulltextResultRow> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSuggestIterator.class);

    private final ElasticIndexNode indexNode;
    private final ElasticRequestHandler requestHandler;
    private final ElasticResponseHandler responseHandler;
    private final String suggestQuery;

    private Iterator<? extends FulltextResultRow> internalIterator;
    private boolean loaded;

    ElasticSuggestIterator(@NotNull ElasticIndexNode indexNode,
                           @NotNull ElasticRequestHandler requestHandler,
                           @NotNull ElasticResponseHandler responseHandler) {
        this.indexNode = indexNode;
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.suggestQuery = requestHandler.getPropertyRestrictionQuery().replace(ElasticRequestHandler.SUGGEST_PREFIX, "");
    }

    @Override
    public boolean hasNext() {
        if (!loaded) {
            try {
                loadSuggestions();
            } catch (IOException e) {
                LOG.error("Failed loading suggestions", e);
                throw new RuntimeException(e);
            }
            loaded = true;
        }
        return internalIterator != null && internalIterator.hasNext();
    }

    @Override
    public FulltextResultRow next() {
        return internalIterator.next();
    }
    
    private void loadSuggestions() throws IOException {
        Query suggestionQuery = requestHandler.suggestionMatchQuery(suggestQuery);
        SearchRequest sReq = SearchRequest.of(s -> s
                .index(indexNode.getDefinition().getIndexAlias())
                .query(suggestionQuery)
                .size(100)
                .source(ss -> ss.filter(f -> f.includes(FieldNames.PATH))));

        SearchResponse<ObjectNode> sRes = indexNode.getConnection().getClient().search(sReq, ObjectNode.class);
        this.internalIterator = sRes.hits().hits().stream()
                .filter(hit -> responseHandler.isAccessible(responseHandler.getPath(hit)))
                .map(hit -> hit.innerHits().get(FieldNames.SUGGEST).hits().hits())
                .flatMap(Collection::stream)
                .map(hit -> new ElasticSuggestion(hit.source().to(ObjectNode.class).get("value").asText(), hit.score()))
                .collect(Collectors.toCollection(() -> new PriorityQueue<>((a, b) -> Double.compare(b.score, a.score))))
                .stream().distinct().iterator();
    }

    private final static class ElasticSuggestion extends FulltextResultRow {
        private ElasticSuggestion(String suggestion, double score) {
            super(suggestion, score);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FulltextResultRow fulltextResultRow = (FulltextResultRow) o;
            return Objects.equals(this.suggestion, fulltextResultRow.suggestion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.suggestion);
        }
    }
}
