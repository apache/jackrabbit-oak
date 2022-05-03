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
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
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
    
    class Nothing {
        
    }
    private void loadSuggestions() {
        try {
            final SearchRequest searchReq = SearchRequest.of(sr->sr
                    .index(String.join(",", indexNode.getDefinition().getIndexAlias()))
                    .suggest(su->su
                            .text(spellCheckQuery)
                            .suggesters("oak:suggestion", fs->fs
                                    .phrase(requestHandler.suggestQuery()))));
            String endpoint = "/" + String.join(",", searchReq.index()) + "/_search?filter_path=suggest";
            Request request = new Request("POST", endpoint);
            request.setJsonEntity(ElasticIndexUtils.toString(searchReq)); 
            
            Response searchRes = indexNode.getConnection().getOldClient().getLowLevelClient().performRequest(request);
            
            JsonFactoryBuilder factoryBuilder = new JsonFactoryBuilder();
            factoryBuilder.disable(JsonFactory.Feature.INTERN_FIELD_NAMES);
            ObjectMapper JSON_MAPPER = new ObjectMapper(factoryBuilder.build());
            JSON_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            ObjectNode ress = JSON_MAPPER.readValue(searchRes.getEntity().getContent(), ObjectNode.class);
            
            List<String> options = new ArrayList<String>();
            for(JsonNode sug : ress.get("suggest").get("oak:suggestion")) {
                for(JsonNode opt : sug.get("options")) {
                    options.add(opt.get("text").asText());
                }
            };
            
            final ArrayDeque<String> suggestionTexts = new ArrayDeque<>();
            
            List<RequestItem> list = new ArrayList<RequestItem>();
            for(String option: options) {
                suggestionTexts.offer(option);
                list.add(RequestItem.of(ri->ri
                        .header(h->h
                                .index(String.join(",", searchReq.index())))
                        .body(b->b
                                .query(q->q
                                        .bool(requestHandler.suggestMatchQuery(option)))
                                .size(100))));
            }
            if(!list.isEmpty()) {
                MsearchRequest mSearchRequest = MsearchRequest.of(ms->ms
                        .searches(list));
                MsearchResponse<JsonNode> mSearchResponse = indexNode.getConnection().getClient().msearch(mSearchRequest, JsonNode.class);
                ArrayList<FulltextResultRow> results = new ArrayList<>();
                for(MultiSearchResponseItem<JsonNode> r: mSearchResponse.responses()) {
                    for(Hit<JsonNode> hit: r.result().hits().hits()) {
                        if (responseHandler.isAccessible(responseHandler.getPath2(hit))) {
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
}
