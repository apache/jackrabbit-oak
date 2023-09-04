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

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.index.LuceneAbstractIndexCommandTest;
import org.apache.jackrabbit.oak.index.LuceneRepositoryFixture;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.run.LucenePurgeOldIndexVersionCommand;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Test;

import ch.qos.logback.classic.Level;

public class LucenePurgeOldIndexVersionTest extends LuceneAbstractIndexCommandTest {
    private final static String FOO1_INDEX_PATH = "/oak:index/fooIndex1";

    private void createCustomIndex(String path, int ootbVersion, int customVersion, boolean asyncIndex) throws IOException,
            RepositoryException {
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder();
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

        addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-4-custom-2"));

        runIndexPurgeCommand(true, 1, "");

        NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");

        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", indexRootNode.getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", indexRootNode.getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", indexRootNode.getChildNode("fooIndex").exists());
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING));
        Assert.assertFalse(isHiddenChildNodePresent(indexRootNode.getChildNode("fooIndex-4")));
        Assert.assertFalse("Index:" + "fooIndex-4-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-2").exists());

        // check if disabled index deleted in subsequent runs if hidden oak mount not present
        runIndexPurgeCommand(true, 1, "");
        indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");
        Assert.assertFalse("Index:" + "fooIndex-4" + " deleted", indexRootNode.getChildNode("fooIndex").exists());
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

            addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-4-custom-2"));

            runIndexPurgeCommand(true, TimeUnit.DAYS.toMillis(1), "");

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
    @Test
    public void noDeleteIfNoActiveIndex() throws Exception {
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

            runIndexPurgeCommand(true, TimeUnit.DAYS.toMillis(1), "");

            List<String> logs = custom.getLogs();
            assertThat(logs.toString(), containsString("Cannot find any active index from the list: [/oak:index/fooIndex-4-custom-2"));

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

    // Test the scenario where the latest index is an OOB index but is disabled, and a lower versioned custom index is actually the only active index serving queries.
    // Verify that the lower versioned index that is being used for queries should not get purged.
    @Test
    public void noDeleteIfLatestOOBIndexIsDisabled() throws Exception {
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
            createCustomIndex(TEST_INDEX_PATH, 3, 3, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 0, false);

            NodeStore store = fixture.getNodeStore();
            NodeBuilder rootBuilder = store.getRoot().builder();
            rootBuilder.getChildNode("oak:index")
                    .getChildNode("fooIndex-4")
                    .setProperty("type", "disabled");
            rootBuilder.getChildNode("oak:index")
                    .getChildNode("fooIndex-3-custom-3")
                    .setProperty("type", "disabled")
                    .setProperty(":originalType", "lucene");
            store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-3-custom-1", "fooIndex-3-custom-2"));

            // At this point of time - we have /oak:index/fooIndex-4 and /oak:index/fooIndex-3-custom-3 which are disabled
            // /oak:index/fooIndex-3-custom-2 and /oak:index/fooIndex-3-custom-1 which are active (hidden mount exists)
            // Indexes of lower versions are all inactive (not disabled but no hidden oak-mount-exists)

            runIndexPurgeCommand(true, 1, "");

            List<String> logs = custom.getLogs();
            // This log verification shows that /oak:index/fooIndex-4 gets filtered at the very first stage (before the list is sent to IndexVersionOperation)
            // This is because it is disabled and does not have :originalType property - so it was disabled manually and not by auto purge command - hence it's simply ignored.
            assertThat(logs.toString(), containsString("Reverse Sorted list [/oak:index/fooIndex-3-custom-3"));

            // This log verifies that /oak:index/fooIndex-3-custom-3 (which is disabled and has :originalType property) is moved to disabled list to be handled accordingly
            assertThat(logs.toString(), containsString("Disabled index list [/oak:index/fooIndex-3-custom-3"));

            // This verifies that the index that is considered for isActive check and further verifications is /oak:index/fooIndex-3-custom-2
            assertThat(logs.toString(), containsString("new reverse sorted list after removing disabled indexes[/oak:index/fooIndex-3-custom-2"));

            NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");
            Assert.assertFalse(indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3").exists());
            Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-3").getProperty("type").getValue(Type.STRING));
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
            Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-3-custom-1").getProperty("type").getValue(Type.STRING));
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
            Assert.assertFalse(indexRootNode.getChildNode("fooIndex-3-custom-3").exists());
            Assert.assertTrue(indexRootNode.getChildNode("fooIndex-4").exists());
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

            addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-4-custom-2"));

            runIndexPurgeCommand(true, 1, "");

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

            addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-4-custom-2"));

            runIndexPurgeCommand(true, 1, "");

            List<String> logs = custom.getLogs();
            assertThat("custom fooIndex don't have product version ", logs.toString(),
                    containsString("Repository don't have base index:"));
        } finally {
            custom.finished();
        }
    }

    @Test
    public void noDeleteForDisabledIndexesIfOakMountExists() throws Exception {
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
        // for disabled indexes, it will still be deleted if no oak mount, but will be kept if oak mount exists
        rootBuilder.getChildNode("oak:index").getChildNode("fooIndex-3-custom-1").setProperty("type", "disabled");
        rootBuilder.getChildNode("oak:index").getChildNode("fooIndex-3-custom-1").setProperty(":originalType", "lucene");
        rootBuilder.getChildNode("oak:index").getChildNode("fooIndex-4-custom-1").setProperty("type", "disabled");
        rootBuilder.getChildNode("oak:index").getChildNode("fooIndex-4-custom-1").setProperty(":originalType", "lucene");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-4-custom-1", "fooIndex-4-custom-2"));

        runIndexPurgeCommand(true, 1, "/oak:index/fooIndex,/oak:index");
        NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");
        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", indexRootNode.getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", indexRootNode.getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", indexRootNode.getChildNode("fooIndex").exists());
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING));
        Assert.assertFalse(isHiddenChildNodePresent(indexRootNode.getChildNode("fooIndex-4")));
        Assert.assertTrue("Index:" + "fooIndex-4-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-2").exists());
    }

    @Test
    public void noDeleteForDisabledIndexesIfNoOriginalPropertySet() throws Exception {
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
        // for disabled indexes, if :originalType property is not set, it will be skipped from IndexPurgeOperation generation list and will be ignored (assuming it was manually marked as disabled)
        rootBuilder.getChildNode("oak:index").getChildNode("fooIndex-3-custom-1").setProperty("type", "disabled");
        rootBuilder.getChildNode("oak:index").getChildNode("fooIndex-4-custom-1").setProperty("type", "disabled");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-4-custom-1", "fooIndex-4-custom-2"));

        runIndexPurgeCommand(true, 1, "/oak:index/fooIndex,/oak:index");
        NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");
        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", indexRootNode.getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", indexRootNode.getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", indexRootNode.getChildNode("fooIndex").exists());
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING));
        Assert.assertFalse(isHiddenChildNodePresent(indexRootNode.getChildNode("fooIndex-4")));
        //fooIndex-3-custom-1 should exist even if it doesn't have hidden oak mount - because it got ignored from purge operation generation list because of no :originalType set
        Assert.assertTrue("Index:" + "fooIndex-3-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-4-custom-2").exists());
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

        addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-4-custom-2"));

        runIndexPurgeCommand(true, 1, "/oak:index/fooIndex");

        NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");

        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", indexRootNode.getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", indexRootNode.getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-1" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", indexRootNode.getChildNode("fooIndex").exists());
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING));
        Assert.assertEquals("lucene", indexRootNode.getChildNode("fooIndex-4").getProperty(":originalType").getValue(Type.STRING));
        Assert.assertFalse(isHiddenChildNodePresent(indexRootNode.getChildNode("fooIndex-4")));
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

            addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-4-custom-2"));

            runIndexPurgeCommand(false, 1, "");

            List<String> logs = custom.getLogs();
            assertThat("repository is opened in read only mode ", logs.toString(),
                    containsString("Repository connected in read-only mode."));
        } finally {
            custom.finished();
        }
    }

    @Test
    public void disableIndexesIfOakMountExists() throws Exception {
        createTestData(false);
        createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 2, false);

        addMockHiddenOakMount(fixture.getNodeStore(), Arrays.asList("fooIndex-3", "fooIndex-3-custom-1", "fooIndex-4-custom-2"));

        runIndexPurgeCommand(true, 1, "");

        NodeState indexRootNode = fixture.getNodeStore().getRoot().getChildNode("oak:index");
        Assert.assertFalse(indexRootNode.getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3").exists());
        Assert.assertTrue(indexRootNode.getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse(indexRootNode.getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse(indexRootNode.getChildNode("fooIndex").exists());
        Assert.assertFalse(indexRootNode.getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue(indexRootNode.getChildNode("fooIndex-4-custom-2").exists());

        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-3").getProperty("type").getValue(Type.STRING));
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-3-custom-1").getProperty("type").getValue(Type.STRING));
        Assert.assertEquals("disabled", indexRootNode.getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING));
        Assert.assertEquals("lucene", indexRootNode.getChildNode("fooIndex-3").getProperty(":originalType").getValue(Type.STRING));
        Assert.assertEquals("lucene", indexRootNode.getChildNode("fooIndex-3-custom-1").getProperty(":originalType").getValue(Type.STRING));
        Assert.assertEquals("lucene", indexRootNode.getChildNode("fooIndex-4").getProperty(":originalType").getValue(Type.STRING));
        Assert.assertFalse(isHiddenChildNodePresent(indexRootNode.getChildNode("fooIndex-4")));
    }

    private void runIndexPurgeCommand(boolean readWrite, long threshold, String indexPaths) throws Exception {
        fixture.getAdminSession();
        fixture.getAsyncIndexUpdate("async").run();
        fixture.close();
        LucenePurgeOldIndexVersionCommand command = new LucenePurgeOldIndexVersionCommand();
        File storeDir = fixture.getDir();
        List<String> argsList = new ArrayList<>();
        argsList.add(storeDir.getAbsolutePath());
        if (readWrite) {
            argsList.add("--read-write");
        }
        argsList.add("--threshold=" + threshold);
        if (StringUtils.isNotEmpty(indexPaths)) {
            argsList.add("--index-paths=" + indexPaths);
        }
        command.execute(argsList.toArray((new String[0])));
        fixture = new LuceneRepositoryFixture(storeDir);
        fixture.close();
    }

    private void addMockHiddenOakMount(NodeStore nodeStore, List<String> indexes) throws CommitFailedException {
        NodeBuilder rootNodeBuilder = nodeStore.getRoot().builder();
        for (String item : indexes) {
            if (rootNodeBuilder.getChildNode("oak:index").getChildNode(item).exists()) {
                rootNodeBuilder.getChildNode("oak:index")
                        .getChildNode(item)
                        .setChildNode(":oak:mount-mock-node", EmptyNodeState.EMPTY_NODE);
            }
        }
        EditorHook hook = new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider()));
        nodeStore.merge(rootNodeBuilder, hook, CommitInfo.EMPTY);
    }

    private boolean isHiddenChildNodePresent(NodeState nodeState) {
        boolean isHiddenChildNodePresent = false;
        for (String childNodeName : nodeState.getChildNodeNames()) {
            if (childNodeName.charAt(0) == ':') {
                isHiddenChildNodePresent = true;
            }
        }
        return isHiddenChildNodePresent;
    }
}
