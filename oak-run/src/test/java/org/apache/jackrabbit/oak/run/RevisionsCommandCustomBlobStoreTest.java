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

import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

public class RevisionsCommandCustomBlobStoreTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @BeforeClass
    public static void assumeMongoDB() {
        assumeTrue(MongoUtils.isAvailable());
    }

    private DocumentNodeStore ns;

    @Before
    public void before() {
        ns = createDocumentNodeStore();
    }

    @Test
    public void info() throws Exception {
        RevisionsCommand cmd = new RevisionsCommand();
        cmd.execute(
                MongoUtils.URL,
                "info"
        );
    }

    @Test
    public void collect() throws Exception {
        RevisionsCommand cmd = new RevisionsCommand();
        cmd.execute(
                MongoUtils.URL,
                "collect"
        );
    }

    @Test
    public void reset() throws Exception {
        RevisionsCommand cmd = new RevisionsCommand();
        cmd.execute(
                MongoUtils.URL,
                "reset"
        );
    }

    @Test
    public void sweep() throws Exception {
        int clusterId = ns.getClusterId();
        ns.dispose();
        RevisionsCommand cmd = new RevisionsCommand();
        cmd.execute(
                "--clusterId",
                String.valueOf(clusterId),
                MongoUtils.URL,
                "sweep"
        );
    }

    private DocumentNodeStore createDocumentNodeStore() {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        MongoUtils.dropCollections(c.getDatabase());
        return builderProvider.newBuilder().setBlobStore(new MemoryBlobStore())
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
    }
}
