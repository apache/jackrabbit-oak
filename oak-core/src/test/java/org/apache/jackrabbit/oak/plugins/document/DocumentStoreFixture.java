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
package org.apache.jackrabbit.oak.plugins.document;

import javax.sql.DataSource;

import com.mongodb.BasicDBObject;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;

public abstract class DocumentStoreFixture {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentStoreFixture.class);

    public static final DocumentStoreFixture MEMORY = new MemoryFixture();
    public static final DocumentStoreFixture RDB_H2 = new RDBFixture("RDB-H2(file)", "jdbc:h2:file:./target/ds-test", "sa", "");
    public static final DocumentStoreFixture RDB_PG = new RDBFixture("RDB-Postgres", "jdbc:postgresql:oak", "postgres", "geheim");
    public static final DocumentStoreFixture RDB_DB2 = new RDBFixture("RDB-DB2", "jdbc:db2://localhost:50000/OAK", "oak", "geheim");
    public static final DocumentStoreFixture RDB_MYSQL = new RDBFixture("RDB-MySQL", "jdbc:mysql://localhost:3306/oak", "root", "geheim");
    public static final DocumentStoreFixture RDB_ORACLE = new RDBFixture("RDB-Oracle", "jdbc:oracle:thin:@localhost:1521:orcl", "system", "geheim");
    public static final DocumentStoreFixture MONGO = new MongoFixture("mongodb://localhost:27017/oak");

    public abstract String getName();

    public abstract DocumentStore createDocumentStore(int clusterId);

    public DocumentStore createDocumentStore() {
        return createDocumentStore(1);
    }

    public boolean isAvailable() {
        return true;
    }

    // return false if the multiple instances will not share the same persistence
    public boolean hasSinglePersistence() {
        return true;
    }

    public void dispose() throws Exception {
    }

    public static class MemoryFixture extends DocumentStoreFixture {

        @Override
        public String getName() {
            return "Memory";
        }

        @Override
        public DocumentStore createDocumentStore(int clusterId) {
            return new MemoryDocumentStore();
        }

        @Override
        public boolean hasSinglePersistence() {
            return false;
        }
    }

    public static class RDBFixture extends DocumentStoreFixture {

        DataSource dataSource;
        DocumentStore store1, store2;
        String name;

        public RDBFixture(String name, String url, String username, String passwd) {
            this.name = name;
            try {
                dataSource = RDBDataSourceFactory.forJdbcUrl(url, username, passwd);
            } catch (Exception ex) {
                LOG.info("Database instance not available at " + url + ", skipping tests...", ex);
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public DocumentStore createDocumentStore(int clusterId) {
            if (clusterId == 1) {
                store1 = new RDBDocumentStore(dataSource, new DocumentMK.Builder().setClusterId(1));
                return store1;
            } else if (clusterId == 2) {
                store2 = new RDBDocumentStore(dataSource, new DocumentMK.Builder().setClusterId(2));
                return store2;
            } else {
                throw new RuntimeException("expect clusterId == 1 or == 2");
            }
        }

        @Override
        public boolean isAvailable() {
            return dataSource != null;
        }

        @Override
        public void dispose() {
            if (this.store1 != null) {
                this.store1.dispose();
                this.store1 = null;
            }
            if (this.store2 != null) {
                this.store2.dispose();
                this.store2 = null;
            }
        }
    }

    public static class MongoFixture extends DocumentStoreFixture {
        public static final String DEFAULT_URI = "mongodb://localhost:27017/oak-test";
        private String uri;

        public MongoFixture() {
            this(DEFAULT_URI);
        }

        public MongoFixture(String dbUri) {
            this.uri = dbUri;
        }

        @Override
        public String getName() {
            return "MongoDB";
        }

        @Override
        public DocumentStore createDocumentStore(int clusterId) {
            try {
                MongoConnection connection = new MongoConnection(uri);
                DB db = connection.getDB();
                MongoUtils.dropCollections(db);
                return new MongoDocumentStore(db, new DocumentMK.Builder().setClusterId(clusterId));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isAvailable() {
            try {
                MongoConnection connection = new MongoConnection(uri);
                connection.getDB().command(new BasicDBObject("ping", 1));
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        @Override
        public void dispose() {
            try {
                MongoConnection connection = new MongoConnection(uri);
                connection.getDB().dropDatabase();
            } catch (Exception ignore) {
            }
        }
    }
}
