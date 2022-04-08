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

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.reindex.Source;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.PhraseSuggestCollateQuery;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.elasticsearch.core.search.SourceFilter;
import co.elastic.clients.elasticsearch.core.search.Suggester;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;
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
            final MsearchRequest multiSearch = null; 
            final SearchRequest searchRequest = SearchRequest.of(s->s
                    .index(indexNode.getDefinition().getIndexAlias())
                    .suggest(f->f
                            .suggesters("oak:suggestion", t->t
                                    .phrase(requestHandler.suggestQuery(spellCheckQuery)))));
            
//            SearchResponse<PhraseSuggestion> searchResponse = indexNode.getConnection().getClient().search(searchRequest, PhraseSuggestion.class);
//            for(String key: searchResponse.suggest().keySet()) {
//                if (responseHandler.isAccessible(key)) { //TODO Angela check that key is a path
//                    for (Hit<PhraseSuggestion> hit : searchResponse.hits().hits()) {
//                        suggestionTexts.offer(hit.so);
//                    }
//                }
//            }
//
//            if (!multiSearch.requests().isEmpty()) {
//                MultiSearchResponse res = indexNode.getConnection().getClient().msearch(multiSearch, FulltextResultRow.class);
//                ArrayList<FulltextResultRow> results = new ArrayList<>();
//                for (MultiSearchResponse.Item response : res.getResponses()) {
//                    for (SearchHit doc : response.getResponse().getHits()) {
//                        if (responseHandler.isAccessible(responseHandler.getPath(doc))) {
//                            results.add(new FulltextResultRow(suggestionTexts.poll()));
//                            break;
//                        }
//                    }
//                }
//                this.internalIterator = results.iterator();
//            }

        } catch (Exception e) {
            LOG.error("Error processing suggestions for " + spellCheckQuery, e);
        }

    }
}
