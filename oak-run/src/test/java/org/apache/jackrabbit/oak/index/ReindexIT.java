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

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.io.Files;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.importer.ClusterNodeStoreLock;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

public class ReindexIT extends LuceneAbstractIndexCommandTest {
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalErr = System.err;

    @Before
    public void setUp() {
        IndexCommand.setDisableExitOnError(true);
        System.setErr(new PrintStream(errContent));
    }

    @After
    public void tearDown() {
        System.setErr(originalErr);
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

        IndexRepositoryFixture fixture2 = new LuceneRepositoryFixture(storeDir);
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
    public void reindexIgnoreMissingTikaDepThrow() throws Exception{
        final AtomicInteger exitCode = new AtomicInteger(-1);
        IndexCommand command = new IndexCommand() {
            @Override
            public void checkTikaDependency() throws ClassNotFoundException {
                throw new ClassNotFoundException();
            }

            //avoid System.exit(), we just want to make sure that exit is called with the correct code
            @Override
            public void exit(int status) {
                exitCode.set(status);
            }
        };
        String[] args = {
                "--reindex",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                "test"
        };
        command.execute(args);
        assertEquals("Epxpect to exit with status 1", 1, exitCode.get());
        assertEquals("Missing tika parser dependencies, use --ignore-missing-tika-dep to force continue", errContent.toString("UTF-8").trim());
    }

    @Test
    public void reindexAndThenImport() throws Exception {
        createTestData(true);
        int fooSuggestCount = 2;
        int contentCount = 100;
        addSuggestContent(fixture, "/testnode/suggest1", "foo", fooSuggestCount);
        fixture.getAsyncIndexUpdate("async").run();

        //Update index to bar property also but do not index yet
        indexBarPropertyAlso(fixture);

        int fooCount = getFooCount(fixture, "foo");
        List<String> suggestResults1 = getSuggestResults(fixture);
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

        IndexRepositoryFixture fixture2 = new LuceneRepositoryFixture(storeDir);
        addTestContent(fixture2, "/testNode/b", "foo", contentCount);
        addTestContent(fixture2, "/testNode/c", "bar", contentCount);
        addSuggestContent(fixture2, "/testNode/suggest2", "foo", fooSuggestCount);
        fixture2.getAsyncIndexUpdate("async").run();

        String explain = getQueryPlan(fixture2, "select * from [nt:base] where [bar] is not null");
        assertThat(explain, containsString("traverse"));
        assertThat(explain, not(containsString(TEST_INDEX_PATH)));
        int foo2Count = getFooCount(fixture2, "foo");
        List<String> suggestResults2 = getSuggestResults(fixture2);
        assertEquals(suggestResults1.size() + fooSuggestCount, suggestResults2.size());
        assertEquals(fooCount + contentCount + fooSuggestCount, foo2Count);
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

        IndexRepositoryFixture fixture4 = new LuceneRepositoryFixture(storeDir);
        int foo4Count = getFooCount(fixture4, "foo");
        List<String> suggestResults4 = getSuggestResults(fixture4);
        //new count should be same as previous
        assertEquals(foo2Count, foo4Count);
        assertEquals(suggestResults2.size(), suggestResults4.size());

        //Checkpoint must be released
        assertNull(fixture4.getNodeStore().retrieve(checkpoint));

        //Lock should also be released
        ClusterNodeStoreLock clusterLock = new ClusterNodeStoreLock(fixture4.getNodeStore());
        assertFalse(clusterLock.isLocked("async"));

        //Updates to the index definition should have got picked up
        String explain4 = getQueryPlan(fixture4, "select * from [nt:base] where [bar] is not null");
        assertThat(explain4, containsString(TEST_INDEX_PATH));

        // Run the async index update
        // This is needed for the refresh of the stored index def to take place.
        fixture4.getAsyncIndexUpdate("async").run();

        // check if the stored index def has the correct async property set
        NodeState index = fixture4.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex");
        NodeState storedDef = index.getChildNode(":index-definition");

        // This assertion checks that the diff b/w index def and stored index def should not have async property in it.
        assertFalse(JsopDiff.diffToJsop(index, storedDef).contains("async"));

        // This checks that the stored index def has the proper async value set.
        assertTrue(storedDef.toString().contains("async = async"));
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

        IndexRepositoryFixture fixture2 = new LuceneRepositoryFixture(storeDir);

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
        Files.write(json, jsonFile, StandardCharsets.UTF_8);

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

        IndexRepositoryFixture fixture2 = new LuceneRepositoryFixture(storeDir);

        explain = getQueryPlan(fixture2, "select * from [nt:base] where [bar] is not null");
        assertThat(explain, containsString("/oak:index/barIndex"));

        IndexPathService idxPathService = new IndexPathServiceImpl(fixture2.getNodeStore());
        List<String> indexPaths = CollectionUtils.toList(idxPathService.getIndexPaths());

        assertThat(indexPaths, hasItem("/oak:index/nodetype"));
        assertThat(indexPaths, hasItem("/oak:index/barIndex"));
    }

    private void indexBarPropertyAlso(IndexRepositoryFixture fixture2) throws IOException, RepositoryException {
        Session session = fixture2.getAdminSession();
        NodeState idxState = NodeStateUtils.getNode(fixture2.getNodeStore().getRoot(), TEST_INDEX_PATH);
        LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder(
                new MemoryNodeBuilder(idxState), false);
        idxb.indexRule("nt:base").property("bar").propertyIndex();

        Node idxNode = session.getNode(TEST_INDEX_PATH);
        idxb.build(idxNode);
        session.save();
        session.logout();
    }

    private int getFooCount(IndexRepositoryFixture fixture, String propName) throws IOException, RepositoryException {
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

    private static String getQueryPlan(IndexRepositoryFixture fixture, String query) throws RepositoryException, IOException {
        Session session = fixture.getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query explain = qm.createQuery("explain "+query, Query.JCR_SQL2);
        QueryResult explainResult = explain.execute();
        Row explainRow = explainResult.getRows().nextRow();
        String explanation = explainRow.getValue("plan").getString();
        session.logout();
        return explanation;
    }

    public List<String> getSuggestResults(IndexRepositoryFixture fixture) throws Exception {
        Session session = fixture.getAdminSession();

        QueryManager qm = session.getWorkspace().getQueryManager();
        String sql = "SELECT [rep:suggest()] FROM [nt:base] WHERE SUGGEST('sugge')";
        String explanation = getQueryPlan(fixture, sql);
        assertThat(explanation, containsString("/oak:index/fooIndex"));

        Query q = qm.createQuery(sql, Query.SQL);
        List<String> result = getResult(q.execute(), "rep:suggest()");
        assertNotNull(result);
        return result;
    }

    private List<String> getResult(QueryResult result, String propertyName) throws RepositoryException {
        List<String> results = new ArrayList<>();
        RowIterator it = result.getRows();
        while (it.hasNext()) {
            Row row = it.nextRow();
            results.add(row.getValue(propertyName).getString());
        }
        return results;
    }
}
