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
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.importer.ClusterNodeStoreLock;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class IndexImportIT extends AbstractIndexCommandTest {

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

        int fooCount = getFooCount(fixture);
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
        addTestContent(fixture2, "/testNode/b", 100);
        fixture2.getAsyncIndexUpdate("async").run();

        int foo2Count = getFooCount(fixture2);
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
        int foo4Count = getFooCount(fixture4);

        //new count should be same as previous
        assertEquals(foo2Count, foo4Count);

        //Checkpoint must be released
        assertNull(fixture4.getNodeStore().retrieve(checkpoint));

        //Lock should also be released
        ClusterNodeStoreLock clusterLock = new ClusterNodeStoreLock(fixture4.getNodeStore());
        assertFalse(clusterLock.isLocked("async"));
        fixture4.close();
    }

    private int getFooCount(RepositoryFixture fixture) throws IOException, RepositoryException {
        Session session = fixture.getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        assertFooIndexBeingUsed(qm);

        Query q = qm.createQuery("select * from [nt:base] where [foo] is not null", Query.JCR_SQL2);
        QueryResult result = q.execute();
        int size = Iterators.size(result.getNodes());
        session.logout();
        return size;
    }

    private static void assertFooIndexBeingUsed(QueryManager qm) throws RepositoryException {
        Query explain = qm.createQuery("explain select * from [nt:base] where [foo] is not null", Query.JCR_SQL2);
        QueryResult explainResult = explain.execute();
        Row explainRow = explainResult.getRows().nextRow();
        assertThat(explainRow.getValue("plan").getString(), containsString("/oak:index/fooIndex"));
    }

}
