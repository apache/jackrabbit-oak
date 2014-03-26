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
    public static final DocumentStoreFixture RDB_H2 = new RDBFixture("RDB-Postgres", "jdbc:h2:file:./target/ds-test", "sa", "");
    public static final DocumentStoreFixture RDB_PG = new RDBFixture("RDB-Postgres", "jdbc:postgresql:oak", "postgres", "geheim");
    public static final DocumentStoreFixture RDB_DB2 = new RDBFixture("RDB-DB2", "jdbc:db2://localhost:50000/OAK", "oak", "geheim");
    public static final DocumentStoreFixture MONGO = new MongoFixture("mongodb://localhost:27017/oak");

    public abstract String getName();

    public abstract DocumentStore getDocumentStore();

    public boolean isAvailable() {
        return true;
    }

    private static class MemoryFixture extends DocumentStoreFixture {

        DocumentStore ds = new MemoryDocumentStore();

        @Override
        public String getName() {
            return "Memory";
        }

        @Override
        public DocumentStore getDocumentStore() {
            return ds;
        }
    }

    private static class RDBFixture extends DocumentStoreFixture {

        DocumentStore ds;
        String name;

        public RDBFixture(String name, String url, String username, String passwd) {
            this.name = name;
            try {
                DataSource datas = RDBDataSourceFactory.forJdbcUrl(url, username, passwd);
                this.ds = new RDBDocumentStore(datas, new DocumentMK.Builder());
            } catch (Exception ex) {
                LOG.info("Database instance not available at " + url + ", skipping tests...", ex);
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public DocumentStore getDocumentStore() {
            return ds;
        }

        @Override
        public boolean isAvailable() {
            return this.ds != null;
        }
    }

    private static class MongoFixture extends DocumentStoreFixture {

        DocumentStore ds;

        public MongoFixture(String db) {
            try {
                MongoConnection connection = new MongoConnection(db);
                DB mongoDB = connection.getDB();
                this.ds = new MongoDocumentStore(mongoDB, new DocumentMK.Builder());
            } catch (Exception e) {
                LOG.info("Mongo instance not available at " + db + ", skipping tests...");
            }
        }

        @Override
        public String getName() {
            return "MongoDB";
        }

        @Override
        public DocumentStore getDocumentStore() {
            return ds;
        }

        @Override
        public boolean isAvailable() {
            return this.ds != null;
        }
    }
}
