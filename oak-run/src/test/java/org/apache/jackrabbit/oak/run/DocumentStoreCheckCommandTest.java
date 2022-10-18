/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.run;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

public class DocumentStoreCheckCommandTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Rule
    public TemporaryFolder temp = new TemporaryFolder(new File("target"));

    private DocumentNodeStore ns;

    private File output;

    @BeforeClass
    public static void assumeMongoDB() {
        assumeTrue(MongoUtils.isAvailable());
    }

    @Before
    public void before() throws Exception {
        ns = createDocumentNodeStore();
        populateWithTestData();
        output = temp.newFile();
    }

    @Test
    public void check() throws Exception {
        DocumentStoreCheckCommand cmd = new DocumentStoreCheckCommand();
        cmd.execute(
                "--out",
                output.getAbsolutePath(),
                MongoUtils.URL
        );
        List<String> lines = Files.readAllLines(output.toPath(), UTF_8);
        assertEquals(1, lines.size());
        assertThat(lines.get(0), containsString("summary"));
    }

    @Test
    public void baseVersion() throws Exception {
        createVersionableWithMissingBaseVersion();
        DocumentStoreCheckCommand cmd = new DocumentStoreCheckCommand();
        cmd.execute(
                "--summary", "false",
                "--out", output.getAbsolutePath(),
                MongoUtils.URL
        );
        List<String> lines = Files.readAllLines(output.toPath(), UTF_8);
        assertEquals(1, lines.size());
        assertThat(lines.get(0), containsString(JCR_BASEVERSION));
    }

    @Test
    public void versionHistory() throws Exception {
        createVersionableWithMissingVersionHistory();
        DocumentStoreCheckCommand cmd = new DocumentStoreCheckCommand();
        cmd.execute(
                "--summary", "false",
                "--out", output.getAbsolutePath(),
                MongoUtils.URL
        );
        List<String> lines = Files.readAllLines(output.toPath(), UTF_8);
        assertEquals(1, lines.size());
        assertThat(lines.get(0), containsString(JCR_VERSIONHISTORY));
    }

    private DocumentNodeStore createDocumentNodeStore() {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        MongoUtils.dropCollections(c.getDatabase());
        return builderProvider.newBuilder().setAsyncDelay(0)
                .setBlobStore(new MemoryBlobStore())
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
    }

    private void createVersionableWithMissingBaseVersion()
            throws CommitFailedException {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("versionable").setProperty(
                JCR_BASEVERSION, UUID.randomUUID().toString(), Type.REFERENCE);
        merge(builder);
    }

    private void createVersionableWithMissingVersionHistory()
            throws CommitFailedException {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("versionable").setProperty(
                JCR_VERSIONHISTORY, UUID.randomUUID().toString(), Type.REFERENCE);
        merge(builder);
    }

    private void populateWithTestData() throws CommitFailedException {
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 5; i++) {
            NodeBuilder child = builder.child("node-" + i);
            for (int j = 0; j < 10; j++) {
                child.child("node-" + j);
            }
        }
        merge(builder);
    }

    private void merge(NodeBuilder builder) throws CommitFailedException {
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.runBackgroundOperations();
    }
}
