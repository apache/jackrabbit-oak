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
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class is in charge to extract spell checked suggestions for a given query.
 *
 * It requires 2 calls to Elastic:
 * <ul>
 *     <li>get all the possible spellchecked suggestions</li>
 *     <li>multi search query to get a sample of 100 results for each suggestion for ACL check</li>
 * </ul>
 */
class ElasticSpellcheckIterator implements Iterator<FulltextResultRow> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSpellcheckIterator.class);
    protected final static String SPELLCHECK_PREFIX = "spellcheck?term=";

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
            final ArrayDeque<String> suggestionTexts = new ArrayDeque<>();
            final MultiSearchRequest multiSearch = suggestions()
                    .map(s -> {
                        String text = s.getText().string();
                        suggestionTexts.offer(text);
                        return requestHandler.suggestMatchQuery(text);
                    })
                    .map(query -> SearchSourceBuilder.searchSource()
                            .query(query)
                            .size(100)
                            .fetchSource(FieldNames.PATH, null))
                    .map(searchSource -> new SearchRequest(indexNode.getDefinition().getRemoteIndexAlias())
                            .source(searchSource))
                    .reduce(new MultiSearchRequest(), MultiSearchRequest::add, (ms, ms2) -> ms);

            if (!multiSearch.requests().isEmpty()) {
                MultiSearchResponse res = indexNode.getConnection().getClient().msearch(multiSearch, RequestOptions.DEFAULT);
                ArrayList<FulltextResultRow> results = new ArrayList<>();
                for (MultiSearchResponse.Item response : res.getResponses()) {
                    for (SearchHit doc : response.getResponse().getHits()) {
                        if (responseHandler.isAccessible(responseHandler.getPath(doc))) {
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

    private Stream<PhraseSuggestion.Entry.Option> suggestions() throws IOException {
        final SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("oak:suggestion",
                requestHandler.suggestQuery(spellCheckQuery));

        final SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .suggest(suggestBuilder);

        final SearchRequest searchRequest = new SearchRequest(indexNode.getDefinition().getRemoteIndexAlias())
                .source(searchSourceBuilder);

        SearchResponse searchResponse = indexNode.getConnection().getClient().search(searchRequest, RequestOptions.DEFAULT);

        return StreamSupport
                .stream(searchResponse.getSuggest().spliterator(), false)
                .map(s -> (PhraseSuggestion) s)
                .flatMap(ps -> ps.getEntries().stream())
                .flatMap(ps -> ps.getOptions().stream());
    }
}
