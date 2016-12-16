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
package org.apache.jackrabbit.oak.upgrade.cli.container;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.node.MongoFactory;
import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoNodeStoreContainer implements NodeStoreContainer {

    private static Boolean mongoAvailable;

    private static final Logger LOG = LoggerFactory.getLogger(MongoNodeStoreContainer.class);

    private static final String MONGO_URI = System.getProperty("oak.mongo.uri",
            "mongodb://localhost:27017/oak-migration");

    private static final AtomicInteger DATABASE_SUFFIX = new AtomicInteger(1);

    private final MongoFactory mongoFactory;

    private final BlobStoreContainer blob;

    private final String mongoUri;

    private Closer closer;

    public MongoNodeStoreContainer() throws IOException {
        this(new DummyBlobStoreContainer());
    }

    public MongoNodeStoreContainer(BlobStoreContainer blob) throws IOException {
        Assume.assumeTrue(isMongoAvailable());
        this.mongoUri = String.format("%s-%d", MONGO_URI, DATABASE_SUFFIX.getAndIncrement());
        this.mongoFactory = new MongoFactory(mongoUri, 2, false);
        this.blob = blob;
        clean();
    }

    public static boolean isMongoAvailable() {
        if (mongoAvailable != null) {
            return mongoAvailable;
        }

        mongoAvailable = testMongoAvailability();
        return mongoAvailable;
    }

    private static boolean testMongoAvailability() {
        Mongo mongo = null;
        try {
            MongoClientURI uri = new MongoClientURI(MONGO_URI + "?connectTimeoutMS=3000");
            mongo = new MongoClient(uri);
            mongo.getDatabaseNames();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            if (mongo != null) {
                mongo.close();
            }
        }
    }

    @Override
    public NodeStore open() throws IOException {
        this.closer = Closer.create();
        return mongoFactory.create(blob.open(), closer);
    }

    @Override
    public void close() {
        try {
            closer.close();
        } catch (IOException e) {
            LOG.error("Can't close document node store", e);
        }
    }

    @Override
    public void clean() throws IOException {
        MongoClientURI uri = new MongoClientURI(mongoUri);
        MongoClient client = new MongoClient(uri);
        client.dropDatabase(uri.getDatabase());
        blob.clean();
    }

    @Override
    public String getDescription() {
        return mongoUri;
    }
}
