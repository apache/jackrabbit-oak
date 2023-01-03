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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.PhraseSuggestOption;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.MsearchResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.msearch.MultiSearchResponseItem;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import co.elastic.clients.elasticsearch.core.search.Hit;

/**
 * This class is in charge to extract spell checked suggestions for a given query.
 * <p>
 * It requires 2 calls to Elastic:
 * <ul>
 *     <li>get all the possible spellchecked suggestions</li>
 *     <li>multi search query to get a sample of 100 results for each suggestion for ACL check</li>
 * </ul>
 */
class ElasticSpellcheckIterator implements Iterator<FulltextResultRow> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSpellcheckIterator.class);
    protected static final String SPELLCHECK_PREFIX = "spellcheck?term=";

    private static final ObjectMapper JSON_MAPPER;

    static {
        JsonFactoryBuilder factoryBuilder = new JsonFactoryBuilder();
        factoryBuilder.disable(JsonFactory.Feature.INTERN_FIELD_NAMES);
        JSON_MAPPER = new ObjectMapper(factoryBuilder.build());
        JSON_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final ElasticIndexNode indexNode;
    private final ElasticRequestHandler requestHandler;
    private final ElasticResponseHandler responseHandler;
    private final String spellCheckQuery;

    private Iterator<FulltextResultRow> internalIterator;
    private boolean loaded = false;

    ElasticSpellcheckIterator(@NotNull ElasticIndexNode indexNode,
                              @NotNull ElasticRequestHandler requestHandler,
                              @NotNull ElasticResponseHandler responseHandler) {
        this.indexNode = indexNode;
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.spellCheckQuery = requestHandler.getPropertyRestrictionQuery().replace(SPELLCHECK_PREFIX, "");
    }

    @Override
    public boolean hasNext() {
        if (!loaded) {
            loadSuggestions();
            loaded = true;
        }
        return internalIterator != null && internalIterator.hasNext();
    }

    @Override
    public FulltextResultRow next() {
        return internalIterator.next();
    }

    private void loadSuggestions() {
        try {
            // spell check requires 2 calls to elastic
            final ArrayDeque<String> suggestionTexts = new ArrayDeque<>();
            // 1. loads the possible top 10 corrections for the input text
            MsearchRequest.Builder multiSearch = suggestions().map(s -> {
                        suggestionTexts.offer(s);
                        return requestHandler.suggestMatchQuery(s);
                    })
                    .map(query -> RequestItem.of(rib ->
                            rib.header(hb -> hb.index(indexNode.getDefinition().getIndexAlias()))
                                    .body(bb -> bb.query(qb -> qb.bool(query)).size(100))))
                    .reduce(
                            new MsearchRequest.Builder().index(indexNode.getDefinition().getIndexAlias()),
                            MsearchRequest.Builder::searches, (ms, ms2) -> ms);

            // 2. executes a multi search query with the results of the previous query. For each sub query, we then check
            // there is at least 1 accessible result. In that case the correction is returned, otherwise it's filtered out
            if (!suggestionTexts.isEmpty()) {
                MsearchResponse<ObjectNode> mSearchResponse = indexNode.getConnection().getClient()
                        .msearch(multiSearch.build(), ObjectNode.class);
                ArrayList<FulltextResultRow> results = new ArrayList<>();
                for (MultiSearchResponseItem<ObjectNode> r : mSearchResponse.responses()) {
                    for (Hit<ObjectNode> hit : r.result().hits().hits()) {
                        if (responseHandler.isAccessible(responseHandler.getPath(hit))) {
                            results.add(new FulltextResultRow(suggestionTexts.poll()));
                            break;
                        }
                    }
                }
                this.internalIterator = results.iterator();
            }
        } catch (IOException e) {
            LOG.error("Error processing suggestions for " + spellCheckQuery, e);
        }

    }

    private Stream<String> suggestions() throws IOException {
        SearchRequest searchReq = SearchRequest.of(sr -> sr
                .index(indexNode.getDefinition().getIndexAlias())
                .suggest(sb -> sb
                        .text(spellCheckQuery)
                        .suggesters("oak:suggestion",
                                fs -> fs.phrase(requestHandler.suggestQuery()))
                ));

        ElasticsearchClient esClient = indexNode.getConnection().getClient();

        SearchResponse<ObjectNode> searchRes = esClient.search(searchReq, ObjectNode.class);

        return searchRes
                .suggest()
                .get("oak:suggestion")
                .stream()
                .flatMap(node -> node.phrase().options().stream())
                .map(PhraseSuggestOption::text);
    }
}
