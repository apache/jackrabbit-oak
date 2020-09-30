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
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.index.AbstractIndexCommandTest;
import org.apache.jackrabbit.oak.index.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.run.PurgeOldIndexVersionCommand;
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
import java.util.List;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PurgeOldIndexVersionTest extends AbstractIndexCommandTest {
    private final static String FOO1_INDEX_PATH = "/oak:index/fooIndex1";

    private void createCustomIndex(String path, int ootbVersion, int customVersion, boolean asyncIndex) throws IOException, RepositoryException {
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder();
        if (!asyncIndex) {
            idxBuilder.noAsync();
        }
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();

        Session session = fixture.getAdminSession();
        String indexName = customVersion != 0
                ? path + "-" + ootbVersion + "-custom-" + customVersion
                : path + "-" + ootbVersion;
        Node fooIndex = getOrCreateByPath(indexName,
                "oak:QueryIndexDefinition", session);

        idxBuilder.build(fooIndex);
        session.save();
        session.logout();
    }

    /*
        All indexes have reindexCompletionTime present (Reindexing is done after creating all indexes)
     */
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
        fixture.getAsyncIndexUpdate("async").run();
        fixture.close();
        PurgeOldIndexVersionCommand command = new PurgeOldIndexVersionCommand();
        File storeDir = fixture.getDir();
        String[] args = {
                storeDir.getAbsolutePath(),
                "--read-write",
                "--threshold=1"
        };

        command.execute(args);
        fixture = new RepositoryFixture(storeDir);
        fixture.close();

        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex").exists());
        Assert.assertEquals(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING), "disabled");
        Assert.assertFalse(isHiddenChildNodePresent(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4")));
        Assert.assertFalse("Index:" + "fooIndex-4-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4-custom-2").exists());
    }

    @Test
    public void invalidIndexOperationVersion() throws Exception {

        LogCustomizer custom = LogCustomizer
                .forLogger(
                        "org.apache.jackrabbit.oak.indexversion.IndexVersionOperation")
                .enable(Level.INFO).create();
        try {
            custom.starting();
            createTestData(false);
            createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
            createCustomIndex(TEST_INDEX_PATH, 4, 2, false);
            fixture.getAsyncIndexUpdate("async").run();
            fixture.close();
            PurgeOldIndexVersionCommand command = new PurgeOldIndexVersionCommand();

            // File outDir = temporaryFolder.newFolder();
            File storeDir = fixture.getDir();
            String[] args = {
                    storeDir.getAbsolutePath(),
                    "--read-write",
                    "--threshold=1"
            };
            command.execute(args);
            fixture = new RepositoryFixture(storeDir);
            fixture.close();
            List<String> logs = custom.getLogs();
            assertTrue(logs.size() == 1);
            assertThat("custom fooIndex don't have product version ", logs.toString(),
                    containsString("IndexVersionOperation List is not valid for index"));
        } finally {
            custom.finished();
        }
    }

/*
    Not all indexes have reIndexCompletionTime present (Reindexing is done before creating all indexes)
    We are deleting hidden nodes for indexes fooIndex-4-custom-2 and fooIndex-3-custom-2
    Now according to logic because   fooIndex-4-custom-1 will have reindexCompletionTimestamp all index
    versions previous to this will get be marked for deletion or deleting hidden node operation.
 */

    @Test
    public void deleteOldIndexPartially() throws Exception {
        createTestData(false);
        createCustomIndex(TEST_INDEX_PATH, 2, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 1, false);
        createCustomIndex(TEST_INDEX_PATH, 3, 2, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 0, false);
        createCustomIndex(TEST_INDEX_PATH, 4, 1, false);
        fixture.getAsyncIndexUpdate("async").run();
        createCustomIndex(TEST_INDEX_PATH, 4, 2, false);
        PurgeOldIndexVersionCommand command = new PurgeOldIndexVersionCommand();
        PurgeOldVersionUtils.recursiveDeleteHiddenChildNodes(fixture.getNodeStore(), "/oak:index/fooIndex-4-custom-2");
        PurgeOldVersionUtils.recursiveDeleteHiddenChildNodes(fixture.getNodeStore(), "/oak:index/fooIndex-3-custom-2");
        fixture.close();

        File storeDir = fixture.getDir();
        String[] args = {
                storeDir.getAbsolutePath(),
                "--read-write",
                "--threshold=1"
        };

        command.execute(args);
        fixture = new RepositoryFixture(storeDir);
        fixture.close();
        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex").exists());
        Assert.assertEquals(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING), "disabled");
        Assert.assertFalse(isHiddenChildNodePresent(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4")));
        Assert.assertTrue("Index:" + "fooIndex-4-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4-custom-2").exists());
    }

    @Test
    public void donotDeleteDisabledIndexes() throws Exception {
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
        NodeBuilder nodeBuilder = rootBuilder.getChildNode("oak:index")
                .getChildNode("fooIndex-4-custom-1").setProperty("type", "disabled");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fixture.close();
        PurgeOldIndexVersionCommand command = new PurgeOldIndexVersionCommand();
        File storeDir = fixture.getDir();
        String[] args = {
                storeDir.getAbsolutePath(),
                "--read-write",
                "--threshold=1",
                "--index-paths=/oak:index/fooIndex,/oak:index"
        };
        command.execute(args);
        fixture = new RepositoryFixture(storeDir);
        fixture.close();

        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex").exists());
        Assert.assertEquals(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING), "disabled");
        Assert.assertFalse(isHiddenChildNodePresent(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4")));
        Assert.assertTrue("Index:" + "fooIndex-4-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4-custom-2").exists());
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

        fixture.getAsyncIndexUpdate("async").run();
        fixture.close();
        PurgeOldIndexVersionCommand command = new PurgeOldIndexVersionCommand();
        File storeDir = fixture.getDir();
        String[] args = {
                storeDir.getAbsolutePath(),
                "--read-write",
                "--threshold=1",
                "--index-paths=/oak:index/fooIndex"
        };
        command.execute(args);
        fixture = new RepositoryFixture(storeDir);
        fixture.close();

        Assert.assertFalse("Index:" + "fooIndex-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-2").exists());
        Assert.assertFalse("Index:" + "fooIndex-2-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-2-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3-custom-1").exists());
        Assert.assertFalse("Index:" + "fooIndex-3-custom-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-3-custom-2").exists());
        Assert.assertFalse("Index:" + "fooIndex" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex").exists());
        Assert.assertEquals(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING), "disabled");
        Assert.assertFalse(isHiddenChildNodePresent(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4")));
        Assert.assertFalse("Index:" + "fooIndex-4-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4-custom-1").exists());
        Assert.assertTrue("Index:" + "fooIndex-4-custom-2" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4-custom-2").exists());

        Assert.assertTrue("Index:" + "fooIndex1-3-custom-1" + " deleted", fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex1-3-custom-1").exists());
    }

    @Test
    public void donotDeleteNonReadWriteMode() throws Exception {
        LogCustomizer custom = LogCustomizer
                .forLogger(
                        "org.apache.jackrabbit.oak.indexversion.PurgeOldIndexVersion")
                .enable(Level.INFO).create();
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
            fixture.getAsyncIndexUpdate("async").run();
            fixture.close();
            PurgeOldIndexVersionCommand command = new PurgeOldIndexVersionCommand();

            File storeDir = fixture.getDir();
            String[] args = {
                    storeDir.getAbsolutePath()
            };

            command.execute(args);
            fixture = new RepositoryFixture(storeDir);
            fixture.close();
            List<String> logs = custom.getLogs();
            assertThat("repository is opened in read only mode ", logs.toString(),
                    containsString("Repository is opened in read-only mode"));
        } finally {
            custom.finished();
        }
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
