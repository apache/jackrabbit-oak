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

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.Closer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import java.io.IOException;

import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;

public class MongoFactory extends DocumentFactory {

    private final MongoClientURI uri;

    private final int cacheSize;

    private final boolean readOnly;

    public MongoFactory(String repoDesc, int cacheSize, boolean readOnly) {
        this.uri = new MongoClientURI(repoDesc);
        this.cacheSize = cacheSize;
        this.readOnly = readOnly;
    }

    @Override
    public NodeStore create(BlobStore blobStore, Closer closer) throws IOException {
        System.setProperty(DocumentNodeStore.SYS_PROP_DISABLE_JOURNAL, "true");
        MongoDocumentNodeStoreBuilder builder = baseConfiguration(newMongoDocumentNodeStoreBuilder(), cacheSize);
        builder.setMongoDB(createClient(closer), getDBName());
        if (blobStore != null) {
            builder.setBlobStore(blobStore);
        }
        if (readOnly) {
            builder.setReadOnlyMode();
        }
        DocumentNodeStore documentNodeStore = builder.build();

        // TODO probably we should disable all observers, see OAK-5651
        documentNodeStore.getBundlingConfigHandler().unregisterObserver();

        closer.register(documentNodeStore::dispose);
        return documentNodeStore;
    }

    private MongoClient createClient(Closer closer) {
        MongoClient client = new MongoClient(uri);
        closer.register(client::close);
        return client;
    }

    private String getDBName() {
        String db;
        if (uri.getDatabase() == null) {
            db = "aem-author"; // assume an author instance
        } else {
            db = uri.getDatabase();
        }
        return db;
    }

    @Override
    public boolean hasExternalBlobReferences() throws IOException {
        Closer closer = Closer.create();
        try {
            MongoBlobStore mongoBlobStore = new MongoBlobStore(createClient(closer).getDatabase(getDBName()));
            return !mongoBlobStore.getAllChunkIds(0).hasNext();
        } catch(Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    @Override
    public String toString() {
        return String.format("DocumentNodeStore[%s]", uri.toString());
    }
}
