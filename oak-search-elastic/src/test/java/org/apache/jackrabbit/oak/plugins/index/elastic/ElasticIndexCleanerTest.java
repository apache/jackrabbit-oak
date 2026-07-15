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
import org.junit.Test;

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

    private Tree createIndexAndContentNode(String indexProperty, String contentNodeName) throws Exception {
        IndexDefinitionBuilder builder = createIndex(indexProperty);

        String indexName = UUID.randomUUID().toString();
        Tree index = setIndex(indexName, builder);
        root.commit();

        // add content
        Tree test = root.getTree("/").addChild(contentNodeName);
        test.addChild("a").setProperty(indexProperty, "Hello World!");
        test.addChild("b").setProperty(indexProperty, "Simple test");
        root.commit();

        assertEventually(() -> assertTrue(exists(index)));
        return index;
    }

    @Test
    public void testIndexDeletion() throws Exception {
        Tree indexId1 = createIndexAndContentNode("propa", "test1");
        Tree indexId2 = createIndexAndContentNode("propb", "test2");
        Tree indexId3 = createIndexAndContentNode("propc", "test3");

        root.refresh();
        indexId1.remove();
        indexId2.remove();
        root.commit();

        NodeState oakIndex = nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        assertFalse(oakIndex.getChildNode(indexId1.getName()).exists());
        assertFalse(oakIndex.getChildNode(indexId2.getName()).exists());

        ElasticIndexCleaner cleaner = new ElasticIndexCleaner(esConnection, nodeStore, 5);
        cleaner.run();

        assertTrue(exists(indexId1));
        assertTrue(exists(indexId2));
        assertTrue(exists(indexId3));

        Thread.sleep(5000);

        assertEventually(() -> {
            cleaner.run();
            assertFalse(exists(indexId1));
            assertFalse(exists(indexId2));
            assertTrue(exists(indexId3));
        });
    }

    @Test
    public void preventDisabledIndexDeletion() throws Exception {
        int indexDeletionThresholdTime = 5;
        Tree index = createIndexAndContentNode("propa", "test1");
        index.remove();

        root.refresh();
        index.setProperty("type", "disabled");
        root.commit();

        ElasticIndexCleaner cleaner = new ElasticIndexCleaner(esConnection, nodeStore, indexDeletionThresholdTime);
        cleaner.run();

        assertTrue(exists(index));

        Thread.sleep(TimeUnit.SECONDS.toMillis(indexDeletionThresholdTime));
        cleaner.run();

        assertTrue(exists(index));
    }

    @Test
    public void preventIndexDeletionWhenSeedNotFound() throws Exception {
        int indexDeletionThresholdTime = 5;
        Tree index = createIndexAndContentNode("propa", "test1");
        index.remove();

        root.refresh();
        index.removeProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED);
        root.commit();

        ElasticIndexCleaner cleaner = new ElasticIndexCleaner(esConnection, nodeStore, indexDeletionThresholdTime);
        cleaner.run();

        assertTrue(exists(index));

        Thread.sleep(TimeUnit.SECONDS.toMillis(indexDeletionThresholdTime));
        cleaner.run();

        assertTrue(exists(index));
    }

}
