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
package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.FormatVersion;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.versionOf;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class UnlockUpgradeCommandTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;

    @BeforeClass
    public static void assumeMongoDB() {
        assumeTrue(MongoUtils.isAvailable());
    }

    @Before
    public void before() {
        ns = createDocumentNodeStore();
    }

    @Test
    public void unlockUpgrade() throws Exception {
        ns.dispose();
        // revert format version back to something old
        resetFormatVersion(FormatVersion.valueOf("0.0.1"));
        // execute command
        UnlockUpgradeCommand cmd = new UnlockUpgradeCommand();
        cmd.execute(MongoUtils.URL);
        // verify

        FormatVersion v = versionOf(createDocumentNodeStore().getDocumentStore());
        assertEquals(DocumentNodeStore.VERSION, v);
    }

    private DocumentNodeStore createDocumentNodeStore() {
        MongoConnection c = connectionFactory.getConnection();
        MongoUtils.dropCollections(c.getDB().getName());
        return builderProvider.newBuilder().setMongoDB(c.getDB()).getNodeStore();
    }

    private void resetFormatVersion(FormatVersion v) {
        MongoConnection c = connectionFactory.getConnection();
        DocumentStore s = new MongoDocumentStore(c.getDB(), newMongoDocumentNodeStoreBuilder());
        s.remove(Collection.SETTINGS, "version");
        assertTrue(v.writeTo(s));
        s.dispose();
    }

}
