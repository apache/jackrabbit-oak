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

package org.apache.jackrabbit.oak.fixture;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.MongoClientURI;

public class DocumentMongoFixture extends NodeStoreFixture {

    private static final Logger log = LoggerFactory.getLogger(DocumentMongoFixture.class);

    private final String uri;

    private final BlobStore blobStore;

    private final Map<NodeStore, String> suffixes = new ConcurrentHashMap<NodeStore, String>();

    private Boolean isAvailable;

    private final AtomicInteger sequence = new AtomicInteger();

    public DocumentMongoFixture(String uri, BlobStore blobStore) {
        this.uri = uri;
        this.blobStore = blobStore;
    }

    public DocumentMongoFixture() {
        this(MongoUtils.URL, null);
    }

    @Override
    public NodeStore createNodeStore() {
        try {
            String suffix = String.format("-%d-%d", System.currentTimeMillis(), sequence.incrementAndGet());

            DocumentMK.Builder builder = new DocumentMK.Builder();
            if (blobStore != null) {
                builder.setBlobStore(blobStore);
            }
            builder.setPersistentCache("target/persistentCache,time");
            builder.setMongoDB(getDb(suffix));
            DocumentNodeStore ns = builder.getNodeStore();
            suffixes.put(ns, suffix);
            return ns;
        } catch (Exception e) {
            throw new AssumptionViolatedException("Mongo instance is not available", e);
        }
    }

    protected DB getDb(String suffix) throws UnknownHostException {
        String dbName = new MongoClientURI(uri).getDatabase();
        MongoConnection connection = new MongoConnection(uri);
        return connection.getDB(dbName + "-" + suffix);
    }

    @Override
    public boolean isAvailable() {
        if (isAvailable == null) {
            isAvailable = MongoUtils.isAvailable();
        }
        return isAvailable;
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        if (nodeStore instanceof DocumentNodeStore) {
            ((DocumentNodeStore) nodeStore).dispose();
        }
        if (nodeStore == null) {
            return;
        }
        String suffix = suffixes.remove(nodeStore);
        if (suffix != null) {
            try {
                DB db = getDb(suffix);
                db.dropDatabase();
                db.getMongo().close();
            } catch (Exception e) {
                log.error("Can't close Mongo", e);
            }
        }
    }

    @Override
    public String toString() {
        return "DocumentNodeStore[Mongo] on " + this.uri;
    }
}