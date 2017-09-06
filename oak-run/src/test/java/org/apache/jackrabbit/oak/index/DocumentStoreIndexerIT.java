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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

public class DocumentStoreIndexerIT extends AbstractIndexCommandTest {
    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Test
    public void indexMongoRepo() throws Exception{
        fixture = new RepositoryFixture(temporaryFolder.getRoot(), getNodeStore());
        createTestData(false);
        String checkpoint = fixture.getNodeStore().checkpoint(TimeUnit.HOURS.toMillis(24));
        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir="  + outDir.getAbsolutePath(),
                "--index-paths=/oak:index/fooIndex",
                "--doc-traversal-mode",
                "--checkpoint="+checkpoint,
                "--reindex",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                MongoUtils.URL
        };

        command.execute(args);

        File indexes = new File(outDir, IndexerSupport.LOCAL_INDEX_ROOT_DIR);
        assertTrue(indexes.exists());

        IndexRootDirectory idxRoot = new IndexRootDirectory(indexes);
        List<LocalIndexDir> idxDirs = idxRoot.getAllLocalIndexes();

        assertEquals(1, idxDirs.size());
    }

    private NodeStore getNodeStore(){
        return builderProvider.newBuilder().setMongoDB(getConnection().getDB()).getNodeStore();
    }

    private MongoConnection getConnection(){
        MongoConnection conn = connectionFactory.getConnection();
        assumeNotNull(conn);
        conn.getDB().dropDatabase();
        return conn;
    }

}
