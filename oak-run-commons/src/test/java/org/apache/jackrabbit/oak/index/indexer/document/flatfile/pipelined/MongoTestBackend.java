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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import java.io.Closeable;
import java.io.IOException;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;

class MongoTestBackend implements Closeable {

    final MongoClientURI mongoClientURI;
    final MongoDocumentStore mongoDocumentStore;
    final DocumentNodeStore documentNodeStore;
    final MongoDatabase mongoDatabase;

    public MongoTestBackend(MongoClientURI mongoClientURI, MongoDocumentStore mongoDocumentStore,
        DocumentNodeStore documentNodeStore, MongoDatabase mongoDatabase) {
        this.mongoClientURI = mongoClientURI;
        this.mongoDocumentStore = mongoDocumentStore;
        this.documentNodeStore = documentNodeStore;
        this.mongoDatabase = mongoDatabase;
    }

    @Override
    public void close() throws IOException {
        documentNodeStore.dispose();
        mongoDocumentStore.dispose();
    }

    @Override
    public String toString() {
        return "Backend{" +
            "mongoClientURI=" + mongoClientURI +
            ", mongoDocumentStore=" + mongoDocumentStore +
            ", documentNodeStore=" + documentNodeStore +
            ", mongoDatabase=" + mongoDatabase +
            '}';
    }
}
