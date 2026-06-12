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

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * A {@link FulltextIndexWriter} proxy that defers creation of the real {@link ElasticIndexWriter}
 * to the first {@link #updateDocument} or {@link #deleteDocuments} call (OAK-12249).
 *
 * <p>If {@link #close} is called before any document is written — i.e. the reindex produced zero
 * documents — the supplier is never invoked, so no Elasticsearch index or alias is created.
 * Instead, {@link ElasticIndexDefinition#PROP_REQUIRES_PROVISIONING} is set on the definition
 * node so the next incremental-write cycle provisions the index on demand.
 *
 * <p>The supplier is expected to create and provision the index as a side effect of construction
 * (as {@link ElasticIndexWriter} does). Thread safety is not required: Oak calls each writer
 * instance from a single thread.
 */
class LazyElasticIndexWriter implements FulltextIndexWriter<ElasticDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(LazyElasticIndexWriter.class);

    private final Supplier<ElasticIndexWriter> writerSupplier;
    private final NodeBuilder definitionBuilder;
    private ElasticIndexWriter delegate;

    LazyElasticIndexWriter(Supplier<ElasticIndexWriter> writerSupplier, NodeBuilder definitionBuilder) {
        this.writerSupplier = writerSupplier;
        this.definitionBuilder = definitionBuilder;
    }

    @Override
    public void updateDocument(String path, ElasticDocument doc) throws IOException {
        getOrCreate().updateDocument(path, doc);
    }

    @Override
    public void deleteDocuments(String path) throws IOException {
        getOrCreate().deleteDocuments(path);
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        if (delegate == null) {
            LOG.info("Reindex produced no documents — skipping ES index creation (OAK-12249)");
            definitionBuilder.setProperty(ElasticIndexDefinition.PROP_REQUIRES_PROVISIONING, true);
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
