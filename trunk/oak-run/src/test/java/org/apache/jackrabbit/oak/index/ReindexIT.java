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

package org.apache.jackrabbit.oak.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.importer.ClusterNodeStoreLock;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ReindexIT extends AbstractIndexCommandTest {

    @Before
    public void setUp() {
        IndexCommand.setDisableExitOnError(true);
    }

    @Test
    public void reindexOutOfBand() throws Exception{
        createTestData(true);
        fixture.getAsyncIndexUpdate("async").run();

        String checkpoint = fixture.getNodeStore().checkpoint(TimeUnit.HOURS.toMillis(24));

        //Close the repository so as all changes are flushed
        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        File storeDir = fixture.getDir();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir="  + outDir.getAbsolutePath(),
                "--index-paths=/oak:index/fooIndex",
                "--checkpoint="+checkpoint,
                "--reindex",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                storeDir.getAbsolutePath()
        };

        command.execute(args);

        RepositoryFixture fixture2 = new RepositoryFixture(storeDir);
        NodeStore store2 = fixture2.getNodeStore();
        PropertyState reindexCount = getNode(store2.getRoot(), "/oak:index/fooIndex").getProperty(IndexConstants.REINDEX_COUNT);
        assertEquals(1, reindexCount.getValue(Type.LONG).longValue());

        File indexes = new File(outDir, OutOfBandIndexer.LOCAL_INDEX_ROOT_DIR);
        assertTrue(indexes.exists());

        IndexRootDirectory idxRoot = new IndexRootDirectory(indexes);
        List<LocalIndexDir> idxDirs = idxRoot.getAllLocalIndexes();

        assertEquals(1, idxDirs.size());
    }

    @Test
    public void reindexAndThenImport() throws Exception {
        createTestData(true);
        fixture.getAsyncIndexUpdate("async").run();

        //Update index to bar property also but do not index yet
        indexBarPropertyAlso(fixture);

        int fooCount = getFooCount(fixture, "foo");
        String checkpoint = fixture.getNodeStore().checkpoint(TimeUnit.HOURS.toMillis(24));

        //Close the repository so as all changes are flushed
        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        File storeDir = fixture.getDir();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir="  + outDir.getAbsolutePath(),
                "--index-paths=/oak:index/fooIndex",
                "--checkpoint="+checkpoint,
                "--reindex",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                storeDir.getAbsolutePath()
        };

        command.execute(args);

        //----------------------------------------
        //Phase 2 - Add some more indexable content. This would let us validate that post
        //import

        RepositoryFixture fixture2 = new RepositoryFixture(storeDir);
        addTestContent(fixture2, "/testNode/b", "foo", 100);
        addTestContent(fixture2, "/testNode/c", "bar", 100);
        fixture2.getAsyncIndexUpdate("async").run();

        String explain = getQueryPlan(fixture2, "select * from [nt:base] where [bar] is not null");
        assertThat(explain, containsString("traverse"));
        assertThat(explain, not(containsString(TEST_INDEX_PATH)));

        int foo2Count = getFooCount(fixture2, "foo");
        assertEquals(fooCount + 100, foo2Count);
        assertNotNull(fixture2.getNodeStore().retrieve(checkpoint));
        fixture2.close();

        //~-----------------------------------------
        //Phase 3 - Import the indexes

        IndexCommand command3 = new IndexCommand();
        File outDir3 = temporaryFolder.newFolder();
        File indexDir = new File(outDir, OutOfBandIndexer.LOCAL_INDEX_ROOT_DIR);
        String[] args3 = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir="  + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-import-dir="  + indexDir.getAbsolutePath(),
                "--index-import",
                "--read-write",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                storeDir.getAbsolutePath()
        };

        command3.execute(args3);

        //~-----------------------------------------
        //Phase 4 - Validate the import

        RepositoryFixture fixture4 = new RepositoryFixture(storeDir);
        int foo4Count = getFooCount(fixture4, "foo");

        //new count should be same as previous
        assertEquals(foo2Count, foo4Count);

        //Checkpoint must be released
        assertNull(fixture4.getNodeStore().retrieve(checkpoint));

        //Lock should also be released
        ClusterNodeStoreLock clusterLock = new ClusterNodeStoreLock(fixture4.getNodeStore());
        assertFalse(clusterLock.isLocked("async"));

        //Updates to the index definition should have got picked up
        String explain4 = getQueryPlan(fixture4, "select * from [nt:base] where [bar] is not null");
        assertThat(explain4, containsString(TEST_INDEX_PATH));
        fixture4.close();
    }

    @Test
    public void reindexInReadWriteMode() throws Exception{
        createTestData(true);
        fixture.getAsyncIndexUpdate("async").run();
        addTestContent(fixture, "/testNode/c", "bar", 100);
        indexBarPropertyAlso(fixture);

        String explain = getQueryPlan(fixture, "select * from [nt:base] where [bar] is not null");
        assertThat(explain, containsString("traverse"));
        assertThat(explain, not(containsString(TEST_INDEX_PATH)));

        explain = getQueryPlan(fixture, "select * from [nt:base] where [foo] is not null");
        assertThat(explain, containsString(TEST_INDEX_PATH));

        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        File storeDir = fixture.getDir();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir="  + outDir.getAbsolutePath(),
                "--index-paths=/oak:index/fooIndex",
                "--reindex",
                "--read-write",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                storeDir.getAbsolutePath()
        };

        command.execute(args);

        RepositoryFixture fixture2 = new RepositoryFixture(storeDir);

        explain = getQueryPlan(fixture2, "select * from [nt:base] where [bar] is not null");
        assertThat(explain, containsString(TEST_INDEX_PATH));

        explain = getQueryPlan(fixture2, "select * from [nt:base] where [foo] is not null");
        assertThat(explain, containsString(TEST_INDEX_PATH));

        int barCount = getFooCount(fixture2, "bar");
        assertEquals(100, barCount);
    }

    @Test
    public void newIndexDefinition() throws Exception{
        createTestData(true);
        addTestContent(fixture, "/testNode/c", "bar", 100);
        fixture.getAsyncIndexUpdate("async").run();

        String explain = getQueryPlan(fixture, "select * from [nt:base] where [bar] is not null");
        assertThat(explain, containsString("traverse"));

        fixture.close();

        IndexCommand command = new IndexCommand();

        String json = "{\n" +
                "  \"/oak:index/barIndex\": {\n" +
                "    \"compatVersion\": 2,\n" +
                "    \"type\": \"lucene\",\n" +
                "    \"async\": \"async\",\n" +
                "    \"jcr:primaryType\": \"oak:QueryIndexDefinition\",\n" +
                "    \"indexRules\": {\n" +
                "      \"jcr:primaryType\": \"nt:unstructured\",\n" +
                "      \"nt:base\": {\n" +
                "        \"jcr:primaryType\": \"nt:unstructured\",\n" +
                "        \"properties\": {\n" +
                "          \"jcr:primaryType\": \"nt:unstructured\",\n" +
                "          \"bar\": {\n" +
                "            \"name\": \"bar\",\n" +
                "            \"propertyIndex\": true,\n" +
                "            \"jcr:primaryType\": \"nt:unstructured\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        File jsonFile = temporaryFolder.newFile();
        Files.write(json, jsonFile, UTF_8);

        File outDir = temporaryFolder.newFolder();
        File storeDir = fixture.getDir();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir="  + outDir.getAbsolutePath(),
                "--index-definitions-file=" + jsonFile.getAbsolutePath(),
                "--reindex",
                "--read-write",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                storeDir.getAbsolutePath()
        };

        command.execute(args);

        RepositoryFixture fixture2 = new RepositoryFixture(storeDir);

        explain = getQueryPlan(fixture2, "select * from [nt:base] where [bar] is not null");
        assertThat(explain, containsString("/oak:index/barIndex"));

        IndexPathService idxPathService = new IndexPathServiceImpl(fixture2.getNodeStore());
        List<String> indexPaths = Lists.newArrayList(idxPathService.getIndexPaths());

        assertThat(indexPaths, hasItem("/oak:index/nodetype"));
        assertThat(indexPaths, hasItem("/oak:index/barIndex"));
    }

    private void indexBarPropertyAlso(RepositoryFixture fixture2) throws IOException, RepositoryException {
        Session session = fixture2.getAdminSession();
        NodeState idxState = NodeStateUtils.getNode(fixture2.getNodeStore().getRoot(), TEST_INDEX_PATH);
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder(
                new MemoryNodeBuilder(idxState), false);
        idxb.indexRule("nt:base").property("bar").propertyIndex();

        Node idxNode = session.getNode(TEST_INDEX_PATH);
        idxb.build(idxNode);
        session.save();
        session.logout();
    }

    private int getFooCount(RepositoryFixture fixture, String propName) throws IOException, RepositoryException {
        Session session = fixture.getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        String explanation = getQueryPlan(fixture, "select * from [nt:base] where ["+propName+"] is not null");
        assertThat(explanation, containsString("/oak:index/fooIndex"));

        Query q = qm.createQuery("select * from [nt:base] where [foo] is not null", Query.JCR_SQL2);
        QueryResult result = q.execute();
        int size = Iterators.size(result.getNodes());
        session.logout();
        return size;
    }

    private static String getQueryPlan(RepositoryFixture fixture, String query) throws RepositoryException, IOException {
        Session session = fixture.getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query explain = qm.createQuery("explain "+query, Query.JCR_SQL2);
        QueryResult explainResult = explain.execute();
        Row explainRow = explainResult.getRows().nextRow();
        String explanation = explainRow.getValue("plan").getString();
        session.logout();
        return explanation;
    }

}
