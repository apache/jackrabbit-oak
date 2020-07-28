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

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

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
        BoolQueryBuilder suggestionQuery = requestHandler.suggestionMatchQuery(suggestQuery);
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(suggestionQuery)
                .size(100)
                .fetchSource(FieldNames.PATH, null);
        SearchRequest searchRequest = new SearchRequest(indexNode.getDefinition().getRemoteIndexAlias())
                .source(searchSourceBuilder);
        SearchResponse res = indexNode.getConnection().getClient().search(searchRequest, RequestOptions.DEFAULT);
        PriorityQueue<ElasticSuggestion> suggestionPriorityQueue = new PriorityQueue<>((a, b) -> Double.compare(b.score, a.score));
        for (SearchHit doc : res.getHits()) {
            if (responseHandler.isAccessible(responseHandler.getPath(doc))) {
                for (SearchHit suggestion : doc.getInnerHits().get(FieldNames.SUGGEST).getHits()) {
                    suggestionPriorityQueue.add(new ElasticSuggestion(((List<String>) suggestion.getSourceAsMap().get("suggestion")).get(0), suggestion.getScore()));
                }
            }
        }
        this.internalIterator = suggestionPriorityQueue.iterator();
    }

    private final static class ElasticSuggestion extends FulltextResultRow{
        private ElasticSuggestion(String suggestion, double score) {
            super(suggestion, score);
        }
    }
}
