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
package org.apache.jackrabbit.oak.upgrade.cli.node;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.Closer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;

public class MongoFactory implements NodeStoreFactory {

    private static final long MB = 1024 * 1024;

    private final MongoClientURI uri;

    private final int cacheSize;

    public MongoFactory(String repoDesc, int cacheSize) {
        this.uri = new MongoClientURI(repoDesc);
        this.cacheSize = cacheSize;
    }

    @Override
    public NodeStore create(BlobStore blobStore, Closer closer) throws UnknownHostException {
        String db;
        if (uri.getDatabase() == null) {
            db = "aem-author"; // assume an author instance
        } else {
            db = uri.getDatabase();
        }
        DocumentMK.Builder builder = getBuilder(cacheSize);
        MongoClient client = new MongoClient(uri);
        closer.register(asCloseable(client));
        builder.setMongoDB(client.getDB(db));
        if (blobStore != null) {
            builder.setBlobStore(blobStore);
        }
        DocumentNodeStore documentNodeStore = builder.getNodeStore();
        closer.register(asCloseable(documentNodeStore));
        return documentNodeStore;
    }

    static Closeable asCloseable(final DocumentNodeStore documentNodeStore) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                documentNodeStore.dispose();
            }
        };
    }

    private static Closeable asCloseable(final MongoClient client) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                client.close();
            }
        };
    }

    static DocumentMK.Builder getBuilder(int cacheSize) {
        boolean fastMigration = !Boolean.getBoolean("mongomk.disableFastMigration");
        DocumentMK.Builder builder = new DocumentMK.Builder();
        builder.memoryCacheSize(cacheSize * MB);
        if (fastMigration) {
            builder.disableBranches();
        }
        return builder;
    }

    @Override
    public String toString() {
        return String.format("DocumentNodeStore[%s]", uri.toString());
    }
}
