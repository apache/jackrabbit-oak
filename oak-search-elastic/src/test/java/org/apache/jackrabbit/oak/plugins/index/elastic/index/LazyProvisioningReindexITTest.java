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

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticAbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * OAK-12249: a reindex under lazy provisioning that produces zero documents must not leave a
 * previously-provisioned index's stale alias and backing index behind — otherwise the system
 * keeps serving pre-reindex content indefinitely, with no signal that anything is wrong.
 */
public class LazyProvisioningReindexITTest extends ElasticAbstractQueryTest {

    @Override
    public void tearDown() throws IOException {
        ElasticIndexEditorProvider.FT_OAK_12249_ENABLE.set(false);
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(false);
        super.tearDown();
    }

    @Test
    public void reindexToZeroDocuments_onPreviouslyProvisionedIndex_removesStaleAlias() throws Exception {
        String indexName = "lazyReindexToZero";
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();
        IndexDefinitionBuilder idxBuilder = new ElasticIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        idxBuilder.indexRule("nt:base").property("propa").propertyIndex();
        NodeState defNodeState = idxBuilder.build();
        ElasticIndexDefinition definition = new ElasticIndexDefinition(root, defNodeState, indexName, esConnection.getIndexPrefix());

        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(esConnection);
        ElasticIndexWriterFactory factory = new ElasticIndexWriterFactory(esConnection, indexTracker, bulkProcessorHandler);

        // GIVEN: a normal (eager) reindex that provisions the index and writes one document —
        // simulates an index that has been live and populated for a while.
        NodeBuilder definitionBuilder = builder.child("oak:index").getChildNode(indexName);
        FulltextIndexWriter<ElasticDocument> firstWriter = factory.newInstance(definition, definitionBuilder, CommitInfo.EMPTY, true);
        firstWriter.updateDocument("/content/a", new ElasticDocument("/content/a"));
        firstWriter.close(System.currentTimeMillis());

        assertTrue("sanity check: alias must exist after the initial provisioning reindex",
                esConnection.getClient().indices().exists(i -> i.index(definition.getIndexAlias())).value());

        // WHEN: lazy provisioning is turned on, and the index is reindexed again — this time the
        // index rule (or content) has changed such that the reindex matches zero documents.
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(true);
        ElasticIndexEditorProvider.FT_OAK_12249_ENABLE.set(true);

        ElasticIndexDefinition definitionAfterFirstReindex =
                new ElasticIndexDefinition(root, definitionBuilder.getNodeState(), indexName, esConnection.getIndexPrefix());
        FulltextIndexWriter<ElasticDocument> secondWriter =
                factory.newInstance(definitionAfterFirstReindex, definitionBuilder, CommitInfo.EMPTY, true);
        // No updateDocument/deleteDocument calls at all — the reindex traversal found nothing to index.
        secondWriter.close(System.currentTimeMillis());

        // THEN: the stale, previously-populated backing index must no longer be aliased/queryable —
        // it must not keep serving pre-reindex content indefinitely.
        assertFalse("stale backing index from before the empty reindex must be unaliased",
                esConnection.getClient().indices().exists(i -> i.index(definition.getIndexAlias())).value());
    }
}
