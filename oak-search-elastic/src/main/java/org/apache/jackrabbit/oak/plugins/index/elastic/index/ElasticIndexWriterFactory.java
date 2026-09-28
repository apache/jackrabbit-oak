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

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;

public class ElasticIndexWriterFactory implements FulltextIndexWriterFactory<ElasticDocument> {
    private final ElasticConnection elasticConnection;
    private final ElasticIndexTracker indexTracker;
    private final ElasticBulkProcessorHandler bulkProcessorHandler;
    private final ElasticRetryPolicy retryPolicy;

    public ElasticIndexWriterFactory(@NotNull ElasticConnection elasticConnection, @NotNull ElasticIndexTracker indexTracker, ElasticBulkProcessorHandler bulkProcessorHandler) {
        this(elasticConnection, indexTracker, bulkProcessorHandler, ElasticRetryPolicy.NO_RETRY);
    }

    public ElasticIndexWriterFactory(@NotNull ElasticConnection elasticConnection, @NotNull ElasticIndexTracker indexTracker, ElasticBulkProcessorHandler bulkProcessorHandler, ElasticRetryPolicy retryPolicy) {
        this.elasticConnection = elasticConnection;
        this.indexTracker = indexTracker;
        this.bulkProcessorHandler = bulkProcessorHandler;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public ElasticIndexWriter newInstance(IndexDefinition definition, NodeBuilder definitionBuilder,
                                           CommitInfo commitInfo, boolean reindex) {
        if (!(definition instanceof ElasticIndexDefinition)) {
            throw new IllegalArgumentException("IndexDefinition must be of type ElasticsearchIndexDefinition " +
                    "instead of " + definition.getClass().getName());
        }

        ElasticIndexDefinition esDefinition = (ElasticIndexDefinition) definition;

        // requiresProvisioning=true for a standard reindex, or when a prior lazy reindex produced
        // zero documents and set PROP_REQUIRES_PROVISIONING in the node store.
        boolean requiresProvisioning = reindex || esDefinition.requiresProvisioning();

        if (requiresProvisioning && ElasticIndexEditorProvider.isLazyProvisioningActive()) {
            // OAK-12249: defer provisioning to the first write, whether this is a reindex or an
            // incremental cycle after an empty lazy reindex. If no documents arrive the supplier is
            // never called, PROP_REQUIRES_PROVISIONING is re-written, and the next cycle retries.
            return new LazyElasticIndexWriter(
                    () -> new EagerElasticIndexWriter(indexTracker, elasticConnection, esDefinition,
                            definitionBuilder, true, commitInfo, bulkProcessorHandler, retryPolicy),
                    definitionBuilder, elasticConnection, esDefinition);
        }

        return new EagerElasticIndexWriter(indexTracker, elasticConnection, esDefinition,
                definitionBuilder, requiresProvisioning, commitInfo, bulkProcessorHandler, retryPolicy);
    }
}
