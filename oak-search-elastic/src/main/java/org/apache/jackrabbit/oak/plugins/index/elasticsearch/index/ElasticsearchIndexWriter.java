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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.index;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchConnection;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDescriptor;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchDocument.pathToId;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchIndexWriter implements FulltextIndexWriter<ElasticsearchDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIndexWriter.class);

    private final ElasticsearchIndexDescriptor indexDescriptor;

    private final boolean isAsync;

    // TODO: use bulk API - https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-bulk-processor.html
    ElasticsearchIndexWriter(@NotNull IndexDefinition indexDefinition,
                             @NotNull ElasticsearchConnection elasticsearchConnection) {
        indexDescriptor = new ElasticsearchIndexDescriptor(elasticsearchConnection, indexDefinition);

        // TODO: ES indexing put another bit delay before docs appear in search.
        // For test without "async" indexing, we can use following hack BUT those where we
        // would setup async, we'd need to find another way.
        isAsync = indexDefinition.getDefinitionNodeState().getProperty("async") != null;
    }

    @Override
    public void updateDocument(String path, ElasticsearchDocument doc) throws IOException {
        IndexRequest request = new IndexRequest(indexDescriptor.getIndexName())
                .id(pathToId(path))
                // immediate refresh would slow indexing response such that next
                // search would see the effect of this indexed doc. Must only get
                // enabled in tests (hopefully there are no non-async indexes in real life)
                .setRefreshPolicy(isAsync ? NONE : IMMEDIATE)
                .source(doc.build(), XContentType.JSON);
        IndexResponse response = indexDescriptor.getClient().index(request, RequestOptions.DEFAULT);
        LOG.trace("update {} - {}. Response: {}", path, doc, response);
    }

    @Override
    public void deleteDocuments(String path) throws IOException {
        DeleteRequest request = new DeleteRequest(indexDescriptor.getIndexName())
                .id(pathToId(path))
                // immediate refresh would slow indexing response such that next
                // search would see the effect of this indexed doc. Must only get
                // enabled in tests (hopefully there are no non-async indexes in real life)
                .setRefreshPolicy(isAsync ? NONE : IMMEDIATE);
        DeleteResponse response = indexDescriptor.getClient().delete(request, RequestOptions.DEFAULT);
        LOG.trace("delete {}. Response: {}", path, response);

    }

    @Override
    public boolean close(long timestamp) throws IOException {
        // TODO : track index updates and return accordingly
        // TODO : if/when we do async push, this is where to wait for those ops to complete
        return false;
    }

    // TODO: we need to check if the index already exists and in that case we have to figure out if there are
    // "breaking changes" in the index definition
    protected void provisionIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexDescriptor.getIndexName());

        // provision settings
        request.settings(Settings.builder()
                .put("analysis.analyzer.ancestor_analyzer.type", "custom")
                .put("analysis.analyzer.ancestor_analyzer.tokenizer", "path_hierarchy"));

        // provision mappings
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject(FieldNames.ANCESTORS)
                        .field("type", "text")
                        .field("analyzer", "ancestor_analyzer")
                        .field("search_analyzer", "keyword")
                        .field("search_quote_analyzer", "keyword")
                        .endObject();
                mappingBuilder.startObject(FieldNames.PATH_DEPTH)
                        .field("type", "integer")
                        .endObject();
                mappingBuilder.startObject(FieldNames.SUGGEST)
                        .field("type", "completion")
                        .endObject();
                mappingBuilder.startObject(FieldNames.NOT_NULL_PROPS)
                        .field("type", "keyword")
                        .endObject();
                mappingBuilder.startObject(FieldNames.NULL_PROPS)
                        .field("type", "keyword")
                        .endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        request.mapping(mappingBuilder);

        String requestMsg = Strings.toString(request.toXContent(jsonBuilder(), EMPTY_PARAMS));
        CreateIndexResponse response = indexDescriptor.getClient().indices().create(request, RequestOptions.DEFAULT);

        LOG.info("Updated settings {}. Response acknowledged: {}", requestMsg, response.isAcknowledged());
    }
}
