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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElasticIndexCleanerTest extends ElasticAbstractQueryTest {

    @Override
    protected boolean useAsyncIndexing() {
        return true;
    }

    private String createIndexAndContentNode(String indexProperty, String contentNodeName) throws Exception {
        IndexDefinitionBuilder builder = createIndex(indexProperty);
        builder.async("async");
        builder.indexRule("nt:base").property(indexProperty);

        String indexId1 = UUID.randomUUID().toString();
        setIndex(indexId1, builder);
        root.commit();
        addContent(indexProperty, contentNodeName);
        String indexPath = "/" + INDEX_DEFINITIONS_NAME + "/" + indexId1;
        assertEventually(() -> {
            NodeState indexState = nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(indexId1);
            String remoteIndexName = ElasticIndexNameHelper.getRemoteIndexName(esConnection.getIndexPrefix(), indexState,
                    indexPath);
            try {
                assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName), RequestOptions.DEFAULT));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return indexId1;
    }

    private void addContent(String indexProperty, String nodeName) throws Exception {
        Tree test = root.getTree("/").addChild(nodeName);
        test.addChild("a").setProperty(indexProperty, "Hello World!");
        test.addChild("b").setProperty(indexProperty, "Simple test");
        root.commit();
    }

    @Test
    public void testIndexDeletion() throws Exception {
        String indexId1 = createIndexAndContentNode("propa", "test1");
        String indexId2 = createIndexAndContentNode("propb", "test2");
        String indexId3 = createIndexAndContentNode("propc", "test3");
        String indexPath1 = "/" + INDEX_DEFINITIONS_NAME + "/" + indexId1;
        String indexPath2 = "/" + INDEX_DEFINITIONS_NAME + "/" + indexId2;
        String indexPath3 = "/" + INDEX_DEFINITIONS_NAME + "/" + indexId3;
        NodeState oakIndex = nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        NodeState indexState1 = oakIndex.getChildNode(indexId1);
        NodeState indexState2 = oakIndex.getChildNode(indexId2);
        NodeState indexState3 = oakIndex.getChildNode(indexId3);

        root.refresh();
        root.getTree(indexPath1).remove();
        root.getTree(indexPath2).remove();
        root.commit();

        oakIndex = nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        assertFalse(oakIndex.getChildNode(indexId1).exists());
        assertFalse(oakIndex.getChildNode(indexId2).exists());

        ElasticIndexCleaner cleaner = new ElasticIndexCleaner(esConnection, nodeStore, 5);
        cleaner.run();

        String remoteIndexName1 = ElasticIndexNameHelper.getRemoteIndexName(esConnection.getIndexPrefix(), indexState1,
                indexPath1);
        String remoteIndexName2 = ElasticIndexNameHelper.getRemoteIndexName(esConnection.getIndexPrefix(), indexState2,
                indexPath2);
        String remoteIndexName3 = ElasticIndexNameHelper.getRemoteIndexName(esConnection.getIndexPrefix(), indexState3,
                indexPath3);

        assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName1), RequestOptions.DEFAULT));
        assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName2), RequestOptions.DEFAULT));
        assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName3), RequestOptions.DEFAULT));

        Thread.sleep(5000);
        cleaner.run();

        assertFalse(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName1), RequestOptions.DEFAULT));
        assertFalse(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName2), RequestOptions.DEFAULT));
        assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName3), RequestOptions.DEFAULT));
    }

    @Test
    public void preventDisabledIndexDeletion() throws Exception {
        int indexDeletionThresholdTime = 5;
        String indexId = createIndexAndContentNode("propa", "test1");
        String indexPath = "/" + INDEX_DEFINITIONS_NAME + "/" + indexId;
        NodeState oakIndex = nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        NodeState indexState = oakIndex.getChildNode(indexId);
        indexState.builder().remove();

        root.refresh();
        root.getTree(indexPath).setProperty("type", "disabled");
        root.commit();

        ElasticIndexCleaner cleaner = new ElasticIndexCleaner(esConnection, nodeStore, indexDeletionThresholdTime);
        cleaner.run();

        String remoteIndexName = ElasticIndexNameHelper.getRemoteIndexName(esConnection.getIndexPrefix(), indexState,
                indexPath);
        assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName), RequestOptions.DEFAULT));

        Thread.sleep(TimeUnit.SECONDS.toMillis(indexDeletionThresholdTime));
        cleaner.run();

        assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName), RequestOptions.DEFAULT));

    }

    @Test
    public void preventIndexDeletionWhenSeedNotFound() throws Exception {
        int indexDeletionThresholdTime = 5;
        String indexId = createIndexAndContentNode("propa", "test1");
        String indexPath = "/" + INDEX_DEFINITIONS_NAME + "/" + indexId;
        NodeState oakIndex = nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        NodeState indexState = oakIndex.getChildNode(indexId);
        indexState.builder().remove();

        root.refresh();
        root.getTree(indexPath).removeProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED);
        root.commit();

        ElasticIndexCleaner cleaner = new ElasticIndexCleaner(esConnection, nodeStore, indexDeletionThresholdTime);
        cleaner.run();

        String remoteIndexName = ElasticIndexNameHelper.getRemoteIndexName(esConnection.getIndexPrefix(), indexState,
                indexPath);
        assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName), RequestOptions.DEFAULT));

        Thread.sleep(TimeUnit.SECONDS.toMillis(indexDeletionThresholdTime));
        cleaner.run();

        assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName), RequestOptions.DEFAULT));

    }

}
