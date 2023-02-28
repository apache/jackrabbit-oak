/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.indexversion;

import ch.qos.logback.classic.Level;
import co.elastic.clients.elasticsearch._types.ExpandWildcard;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.index.ElasticAbstractIndexCommandTest;
import org.apache.jackrabbit.oak.index.ElasticPurgeOldIndexVersionCommand;
import org.apache.jackrabbit.oak.index.ElasticRepositoryFixture;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ElasticPurgeOldIndexVersionTest extends ElasticAbstractIndexCommandTest {

    private final static String FOO1_INDEX_PATH = "/oak:index/fooIndex1";

    private void createCustomIndex(String path, int ootbVersion, int customVersion, boolean asyncIndex) throws IOException,
            RepositoryException {
        ElasticIndexDefinitionBuilder idxBuilder = new ElasticIndexDefinitionBuilder();
        if (!asyncIndex) {
            idxBuilder.noAsync();
        }
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();

        Session session = fixture.getAdminSession();
        String indexName = customVersion != 0 ? path + "-" + ootbVersion + "-custom-" + customVersion : path + "-" + ootbVersion;
        Node fooIndex = getOrCreateByPath(indexName, "oak:QueryIndexDefinition", session);

        idxBuilder.build(fooIndex);
        session.save();
        session.logout();
    }

    @Test
    public void deleteOldIndexCompletely() throws Exception {
        createTestData(false);
        createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 2, false);

        IndicesResponse indicesRes = getListOfRemoteIndexes();
        // 8 indices in ES remote
        Assert.assertEquals(8, indicesRes.valueBody().size());

        runIndexPurgeCommand(true, 1, "");

        indicesRes = getListOfRemoteIndexes();

        NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");

        // Assert names of Remote ES indexes present
        List<String> expectedRemoteIndexNames = new ArrayList<>();

        expectedRemoteIndexNames.add(getRemoteIndexName("fooIndex-4-custom-2", indexRootNode));
        expectedRemoteIndexNames.add(getRemoteIndexName("fooIndex-4", indexRootNode));

        Assert.assertEquals(expectedRemoteIndexNames.size(), indicesRes.valueBody().size());

        for (IndicesRecord i : indicesRes.valueBody()) {
            Assert.assertTrue(expectedRemoteIndexNames.contains(i.index()));
        }

        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", indexRootNode.getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", indexRootNode.getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", indexRootNode.getChildNode("fooIndex").exists());
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING));
        Assert.assertEquals("elasticsearch", indexRootNode.getChildNode("fooIndex-4").getProperty(":originalType").getValue(Type.STRING));
        Assert.assertFalse("Index:" + "fooIndex-4-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-2").exists());

        runIndexPurgeCommand(true, 1, "");
        indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");

        // check that the disabled base index is not deleted in the subsequent runs.
        Assert.assertTrue("Index:" + "fooIndex-4" + " deleted", indexRootNode.getChildNode("fooIndex-4").exists());
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING));
        Assert.assertEquals("elasticsearch", indexRootNode.getChildNode("fooIndex-4").getProperty(":originalType").getValue(Type.STRING));
    }

    @Test
    public void noDeleteForDisabledIndexes() throws Exception {
        createTestData(false);
        createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 2, false);

        NodeStore store = fixture.getNodeStore();
        NodeBuilder rootBuilder = store.getRoot().builder();

        rootBuilder.getChildNode("oak:index").getChildNode("fooIndex-3-custom-1").setProperty("type", "disabled");
        rootBuilder.getChildNode("oak:index").getChildNode("fooIndex-3-custom-1").setProperty(":originalType", "elasticsearch");
        rootBuilder.getChildNode("oak:index").getChildNode("fooIndex-4-custom-1").setProperty("type", "disabled");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        runIndexPurgeCommand(true, 1, "/oak:index/fooIndex,/oak:index");
        NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");
        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", indexRootNode.getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", indexRootNode.getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", indexRootNode.getChildNode("fooIndex").exists());
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING));
        Assert.assertTrue("Index:" + "fooIndex-3-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-2").exists());
    }

    @Test
    public void noDeleteIfActiveIndexTimeThresholdNotMeet() throws Exception {
        LogCustomizer custom = LogCustomizer.forLogger("org.apache.jackrabbit.oak.indexversion.IndexVersionOperation")
                .enable(Level.INFO)
                .create();
        try {
            custom.starting();
            createTestData(false);
            createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 2, false);

            IndicesResponse indicesRes = getListOfRemoteIndexes();
            // 7 indices in ES remote
            Assert.assertEquals(7, indicesRes.valueBody().size());
            runIndexPurgeCommand(true, TimeUnit.DAYS.toMillis(1), "");

            indicesRes = getListOfRemoteIndexes();
            Assert.assertEquals(7, indicesRes.valueBody().size());

            List<String> logs = custom.getLogs();
            assertThat(logs.toString(),
                    containsString("The active index '/oak:index/fooIndex-4-custom-2' indexing time isn't old enough"));

            NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-4-custom-1").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-4-custom-2").exists());
        } finally {
            custom.finished();
        }
    }

    // currently, there is known issue where indexing time will be missed during indexing job running, that need to be fixed separately.
    // but before which is fixed, the purging will not do deletion in that case
    @Test
    public void noDeleteIfActiveIndexTimeMissing() throws Exception {
        LogCustomizer custom = LogCustomizer.forLogger("org.apache.jackrabbit.oak.indexversion.IndexVersionOperation")
                .enable(Level.INFO)
                .create();
        try {
            custom.starting();
            createTestData(false);
            createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 2, false);

            NodeStore store = fixture.getNodeStore();
            NodeBuilder rootBuilder = store.getRoot().builder();
            rootBuilder.getChildNode("oak:index")
                    .getChildNode("fooIndex-4-custom-2")
                    .getChildNode(IndexDefinition.STATUS_NODE)
                    .removeProperty(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP);
            store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            IndicesResponse indicesRes = getListOfRemoteIndexes();
            // 7 indices in ES remote
            Assert.assertEquals(7, indicesRes.valueBody().size());
            runIndexPurgeCommand(true, 1, "");


            indicesRes = getListOfRemoteIndexes();
            // 7 indices in ES remote
            Assert.assertEquals(7, indicesRes.valueBody().size());

            List<String> logs = custom.getLogs();
            assertThat(logs.toString(),
                    containsString("reindexCompletionTimestamp property is not set for index /oak:index/fooIndex-4-custom-2"));

            NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-4-custom-1").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-4-custom-2").exists());
        } finally {
            custom.finished();
        }
    }


    @Test
    public void noDeleteIfInvalidIndexOperationVersion() throws Exception {
        LogCustomizer custom = LogCustomizer.forLogger("org.apache.jackrabbit.oak.indexversion.IndexVersionOperation")
                .enable(Level.INFO)
                .create();
        try {
            custom.starting();
            createTestData(false);
            createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 2, false);

            IndicesResponse indicesRes = getListOfRemoteIndexes();
            // 7 indices in ES remote
            Assert.assertEquals(7, indicesRes.valueBody().size());

            runIndexPurgeCommand(true, 1, "");

            indicesRes = getListOfRemoteIndexes();
            // 1 indices in ES remote
            Assert.assertEquals(1, indicesRes.valueBody().size());


            List<String> logs = custom.getLogs();
            assertThat("custom fooIndex don't have product version ", logs.toString(),
                    containsString("Repository don't have base index:"));
        } finally {
            custom.finished();
        }
    }

    @Test
    public void onlyDeleteVersionIndexesMentionedUnderIndexPaths() throws Exception {
        createTestData(false);
        createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 2, false);
        createCustomIndex(FOO1_INDEX_PATH, 2, 1, false);
        createCustomIndex(FOO1_INDEX_PATH, 2, 2, false);
        createCustomIndex(FOO1_INDEX_PATH, 3, 0, false);
        createCustomIndex(FOO1_INDEX_PATH, 3, 1, false);
        createCustomIndex(FOO1_INDEX_PATH, 3, 2, false);

        IndicesResponse indicesRes = getListOfRemoteIndexes();
        // 13 indices in ES remote
        Assert.assertEquals(13, indicesRes.valueBody().size());

        runIndexPurgeCommand(true, 1, "/oak:index/fooIndex");

        indicesRes = getListOfRemoteIndexes();
        // 7 indices in ES remote
        Assert.assertEquals(7, indicesRes.valueBody().size());

        NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");


        // Assert names of Remote ES indexes present
        List<String> expectedRemoteIndexNames = new ArrayList<>();

        expectedRemoteIndexNames.add(getRemoteIndexName("fooIndex-4-custom-2", indexRootNode));
        expectedRemoteIndexNames.add(getRemoteIndexName("fooIndex-4", indexRootNode));
        expectedRemoteIndexNames.add(getRemoteIndexName("fooIndex1-2-custom-1", indexRootNode));
        expectedRemoteIndexNames.add(getRemoteIndexName("fooIndex1-2-custom-2", indexRootNode));
        expectedRemoteIndexNames.add(getRemoteIndexName("fooIndex1-3", indexRootNode));
        expectedRemoteIndexNames.add(getRemoteIndexName("fooIndex1-3-custom-1", indexRootNode));
        expectedRemoteIndexNames.add(getRemoteIndexName("fooIndex1-3-custom-2", indexRootNode));


        Assert.assertEquals(expectedRemoteIndexNames.size(), indicesRes.valueBody().size());

        for (IndicesRecord i : indicesRes.valueBody()) {
            Assert.assertTrue(expectedRemoteIndexNames.contains(i.index()));
        }

        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", indexRootNode.getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", indexRootNode.getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", indexRootNode.getChildNode("fooIndex").exists());
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING));
        Assert.assertEquals("elasticsearch", indexRootNode.getChildNode("fooIndex-4").getProperty(":originalType").getValue(Type.STRING));
        Assert.assertFalse("Index:" + "fooIndex-4-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-2").exists());

        Assert.assertTrue("Index:" + "fooIndex1-3-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex1-3-custom-1").exists());
    }

    @Test
    public void noDeleteIfNonReadWriteMode() throws Exception {
        LogCustomizer custom = LogCustomizer.forLogger("org.apache.jackrabbit.oak.run.PurgeOldIndexVersionCommand")
                .enable(Level.INFO)
                .create();
        try {
            custom.starting();
            createTestData(false);
            createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 0, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 2, false);

            IndicesResponse indicesRes = getListOfRemoteIndexes();
            // 8 indices in ES remote
            Assert.assertEquals(8, indicesRes.valueBody().size());
            runIndexPurgeCommand(false, 1, "");

            indicesRes = getListOfRemoteIndexes();
            // 8 indices in ES remote
            Assert.assertEquals(8, indicesRes.valueBody().size());

            List<String> logs = custom.getLogs();
            assertThat("repository is opened in read only mode ", logs.toString(),
                    containsString("Repository connected in read-only mode."));
        } finally {
            custom.finished();
        }
    }

    private void runIndexPurgeCommand(boolean readWrite, long threshold, String indexPaths) throws Exception {
        fixture.getAdminSession();
        fixture.getAsyncIndexUpdate("async").run();
        fixture.close();
        ElasticPurgeOldIndexVersionCommand command = new ElasticPurgeOldIndexVersionCommand();
        File storeDir = fixture.getDir();
        List<String> argsList = new ArrayList<>();
        argsList.add(storeDir.getAbsolutePath());
        if (readWrite) {
            argsList.add("--read-write");
        }
        argsList.add("--scheme=" + elasticRule.getElasticConnectionModel().getScheme());
        argsList.add("--host=" + elasticRule.getElasticConnectionModel().getElasticHost());
        argsList.add("--port=" + elasticRule.getElasticConnectionModel().getElasticPort());
        argsList.add("--apiKeyId=" + elasticRule.getElasticConnectionModel().getElasticApiKey());
        argsList.add("--apiKeySecret=" + elasticRule.getElasticConnectionModel().getElasticApiSecret());
        argsList.add("--indexPrefix=" + elasticRule.getElasticConnectionModel().getIndexPrefix());
        argsList.add("--threshold=" + threshold);
        if (StringUtils.isNotEmpty(indexPaths)) {
            argsList.add("--index-paths=" + indexPaths);
        }
        command.execute(argsList.toArray((new String[0])));
        fixture = new ElasticRepositoryFixture(storeDir, esConnection);
        fixture.close();
    }

    private IndicesResponse getListOfRemoteIndexes() throws IOException {
        return elasticRule.getElasticConnection().getClient()
                .cat().indices(r -> r
                        .index(esConnection.getIndexPrefix() + "*")
                        .expandWildcards(ExpandWildcard.Open));
    }

    private String getRemoteIndexName(String indexName, NodeState indexRootNode) {
        return ElasticIndexNameHelper.getRemoteIndexName(esConnection.getIndexPrefix(), "/oak:index/" + indexName,
                indexRootNode.getChildNode(indexName).getProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED).getValue(Type.LONG));
    }
}
