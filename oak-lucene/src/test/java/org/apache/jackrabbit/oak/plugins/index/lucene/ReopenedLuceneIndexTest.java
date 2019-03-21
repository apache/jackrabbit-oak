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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.hamcrest.core.StringContains;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReopenedLuceneIndexTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Rule
    public TemporarySystemProperty systemProperty = new TemporarySystemProperty();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Closer closer;

    private Root root;
    private QueryEngine qe;

    private static final int NUM_NODES = 1000;
    // Use sufficiently large number to cross batched results from index
    private static final int READ_BEFORE_REOPEN = 400;
    private static final int READ_LIMIT = NUM_NODES  + READ_BEFORE_REOPEN / 2;

    private void createRepository() throws RepositoryException, IOException, LoginException {
        System.setProperty("oak.traversing.warning", "100");

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        closer.register(new ExecutorCloser(executorService));
        IndexCopier copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier);
        LuceneIndexProvider queryIndexProvider = new LuceneIndexProvider(copier);

        QueryEngineSettings qeSettings = new QueryEngineSettings();
        qeSettings.setLimitReads(READ_LIMIT);
        qeSettings.setFailTraversal(true);

        NodeStore nodeStore = new MemoryNodeStore(INITIAL_CONTENT);
        Oak oak = new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) queryIndexProvider)
                .with((Observer) queryIndexProvider)
                .with(editorProvider)
                .with(qeSettings)
                ;

        root = oak.createContentRepository().login(null, null).getLatestRoot();
        qe = root.getQueryEngine();
    }

    @Before
    public void setup() throws Exception {
        closer = Closer.create();
        createRepository();
        createIndex();
        createData();
    }

    @After
    public void after() throws IOException {
        closer.close();
        IndexDefinition.setDisableStoredIndexDefinition(false);
    }

    @Test
    public void resultSizeWithinLimitCompatV1() throws Exception {
        int resultSize = iterateResultWhileReopening("v1");

        assertEquals("Unexpected number of result rows", NUM_NODES, resultSize);
    }

    @Test
    public void resultSizeWithinLimitCompatV2() throws Exception {
        int resultSize = iterateResultWhileReopening("v2");

        assertEquals("Unexpected number of result rows", NUM_NODES, resultSize);
    }

    @Test
    public void resultSizeAboveLimitCompatV1() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage(StringContains.containsString("The query read or traversed more than " + READ_LIMIT + " nodes. To avoid affecting other tasks, processing was stopped."));

        // Add more data such that the query genuinely supasses query limit
        createData(NUM_NODES, READ_LIMIT + 100);

        iterateResultWhileReopening("v1");
    }

    @Test
    public void resultSizeAboveLimitCompatV2() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage(StringContains.containsString("The query read or traversed more than " + READ_LIMIT + " nodes. To avoid affecting other tasks, processing was stopped."));

        // Add more data such that the query genuinely supasses query limit
        createData(NUM_NODES, READ_LIMIT + 100);

        iterateResultWhileReopening("v2");
    }

    private void createIndex() throws CommitFailedException {
        IndexDefinitionBuilder idxBuilderV1 = new IndexDefinitionBuilder();
        idxBuilderV1.noAsync().evaluatePathRestrictions()
                .indexRule("nt:base")
                .property("cons").nodeScopeIndex()
                // to make a change in index but we won't query for this
                .enclosingRule().property("foo").propertyIndex();

        IndexDefinitionBuilder idxBuilderV2 = new IndexDefinitionBuilder();
        idxBuilderV2.noAsync().evaluatePathRestrictions()
                .indexRule("nt:base")
                .property("cons").propertyIndex()
                // to make a change in index but we won't query for this
                .enclosingRule().property("foo").propertyIndex();

        Tree oi = root.getTree("/oak:index");

        idxBuilderV1.getBuilderTree().setProperty(LuceneIndexConstants.COMPAT_MODE, 1); // to force aggregate index
        idxBuilderV1.build(oi.addChild("index-v1"));

        idxBuilderV2.build(oi.addChild("index-v2"));

        root.commit();
    }

    private void createData() throws CommitFailedException {
        createData(0, NUM_NODES);
    }

    private void createData(int initialIndex, int lastIndex /* exclusive */) throws CommitFailedException {
        Tree par = root.getTree("/").addChild("parent");

        for (int i = initialIndex; i < lastIndex; i++) {
            par.addChild("c" + i).setProperty("cons", "val");
        }

        root.commit();
    }

    private int iterateResultWhileReopening(String indexTag) throws ParseException, CommitFailedException {
        String queryV1 = "SELECT * FROM [nt:base] WHERE CONTAINS(*, 'val')";
        String queryV2 = "SELECT * FROM [nt:base] WHERE [cons] = 'val'";

        String query = "v1".equals(indexTag) ? queryV1 : queryV2;

        int resultSize = 0;

        Result result = qe.executeQuery(query, Query.JCR_SQL2, QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        Iterator<? extends ResultRow> rows = result.getRows().iterator();

        // get few rows to open the cursor
        for (int i = 0; i < READ_BEFORE_REOPEN; i++) {
            assertTrue("Insufficient result rows. Current iteration count: " + i, rows.hasNext());
            rows.next();
            resultSize++;
        }

        // make a change while we still haven't expected the query cursor
        root.getTree("/").addChild("parent").addChild("c-new").setProperty("foo", "val");
        root.commit();

        // iterate over rest of the results
        while (rows.hasNext()) {
            rows.next();
            resultSize++;
        }

        return resultSize;
    }
}
