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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import co.elastic.clients.elasticsearch.indices.*;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * A {@link FulltextIndexWriter} proxy that defers creation of the real {@link ElasticIndexWriter}
 * to the first {@link #updateDocument}, {@link #deleteDocumentTree} or {@link #deleteDocument}
 * call (OAK-12249).
 *
 * <p>If {@link #close} is called before any document is written — i.e. the reindex produced zero
 * documents — the supplier is never invoked, so no Elasticsearch index or alias is created.
 * Instead, {@link ElasticIndexDefinition#PROP_REQUIRES_PROVISIONING} is set on the definition
 * node so the next incremental-write cycle provisions the index on demand. If the index was
 * already provisioned before this reindex (i.e. it previously had documents), its now-stale
 * alias and backing index are removed — otherwise pre-reindex content would keep being served
 * indefinitely.
 *
 * <p>The supplier is expected to create and provision the index as a side effect of construction
 * (as {@link ElasticIndexWriter} does). Thread safety is not required: Oak calls each writer
 * instance from a single thread.
 */
class LazyElasticIndexWriter implements FulltextIndexWriter<ElasticDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(LazyElasticIndexWriter.class);

    private final Supplier<ElasticIndexWriter> writerSupplier;
    private final NodeBuilder definitionBuilder;
    private final ElasticConnection elasticConnection;
    private final ElasticIndexDefinition indexDefinition;
    private ElasticIndexWriter delegate;

    LazyElasticIndexWriter(Supplier<ElasticIndexWriter> writerSupplier, NodeBuilder definitionBuilder,
                            ElasticConnection elasticConnection, ElasticIndexDefinition indexDefinition) {
        this.writerSupplier = writerSupplier;
        this.definitionBuilder = definitionBuilder;
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
    }

    /**
     * Removes the alias and deletes the backing index for {@code indexDefinition}, if one is
     * currently provisioned. Used when a lazy-provisioning reindex (OAK-12249) closes having
     * written zero documents: without this, a previously-provisioned index would keep its stale
     * alias and backing index, serving pre-reindex content indefinitely instead of going empty.
     *
     * <p>No-op if nothing is currently aliased — that is the state a never-provisioned index is
     * already in, so there is nothing to clean up.
     */
    static void unaliasIfProvisioned(@NotNull ElasticConnection elasticConnection,
                                      @NotNull ElasticIndexDefinition indexDefinition) throws IOException {
        ElasticsearchIndicesClient client = elasticConnection.getClient().indices();
        GetAliasResponse aliasResponse = client.getAlias(garb ->
                garb.index(indexDefinition.getIndexAlias()).ignoreUnavailable(true));
        if (aliasResponse.result().isEmpty()) {
            return;
        }

        UpdateAliasesRequest removeAliasesRequest = UpdateAliasesRequest.of(rb -> {
            aliasResponse.result().forEach((idx, idxAliases) -> rb.actions(ab ->
                    ab.remove(rab -> rab.index(idx).aliases(new ArrayList<>(idxAliases.aliases().keySet())))));
            return rb;
        });
        UpdateAliasesResponse updateAliasesResponse = client.updateAliases(removeAliasesRequest);
        if (!updateAliasesResponse.acknowledged()) {
            throw new IllegalStateException("Remove alias call not acknowledged for alias " + indexDefinition.getIndexAlias());
        }

        DeleteIndexResponse deleteIndexResponse = client.delete(db -> db.index(new ArrayList<>(aliasResponse.result().keySet())));
        if (!deleteIndexResponse.acknowledged()) {
            throw new IllegalStateException("Delete index call not acknowledged for indices " + aliasResponse.result().keySet());
        }
        LOG.info("Reindex produced no documents for a previously-provisioned index — removed stale alias {} and deleted {}",
                indexDefinition.getIndexAlias(), aliasResponse.result().keySet());
    }

    @Override
    public void updateDocument(String path, ElasticDocument doc) throws IOException {
        getOrCreate().updateDocument(path, doc);
    }

    @Override
    public void deleteDocumentTree(String path) throws IOException {
        getOrCreate().deleteDocumentTree(path);
    }

    @Override
    public void deleteDocument(String path) throws IOException {
        getOrCreate().deleteDocument(path);
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        if (delegate == null) {
            LOG.info("Reindex produced no documents — skipping ES index creation (OAK-12249)");
            definitionBuilder.setProperty(ElasticIndexDefinition.PROP_REQUIRES_PROVISIONING, true);
            unaliasIfProvisioned(elasticConnection, indexDefinition);
            return false;
        }
        return delegate.close(timestamp);
    }

    private ElasticIndexWriter getOrCreate() {
        if (delegate == null) {
            delegate = writerSupplier.get();
        }
        return delegate;
    }
}
