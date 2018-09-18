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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.oak.plugins.document.CacheConsistencyTestBase;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;

public class MongoCacheConsistencyTest extends CacheConsistencyTestBase {

    @Rule
    public DocumentMKBuilderProvider provider = new DocumentMKBuilderProvider();

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private MongoTestClient client;

    @BeforeClass
    public static void checkMongoAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Override
    public DocumentStoreFixture getFixture() {
        return new DocumentStoreFixture.MongoFixture() {
            @Override
            public DocumentStore createDocumentStore(DocumentMK.Builder builder) {
                client = new MongoTestClient(MongoUtils.URL);
                MongoConnection c = new MongoConnection(MongoUtils.URL, client);
                connections.add(c);
                builder.setAsyncDelay(0).setMongoDB(client, MongoUtils.DB);
                return builder.getDocumentStore();
            }
        };
    }

    @Override
    public void setTemporaryUpdateException(String msg) {
        client.setExceptionAfterUpdate(msg);
    }
}
