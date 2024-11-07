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

import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_MEM;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_NS;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_RDB;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBConnectionHandler;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceWrapper;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBTestPropSupplier;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DocumentStoreFixture {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentStoreFixture.class);

    public static final DocumentStoreFixture MEMORY = new MemoryFixture();
    public static final DocumentStoreFixture MONGO = new MongoFixture();

    public static final DocumentStoreFixture RDB_DB2 = new RDBFixture("RDB-DB2", RDBTestPropSupplier.DB2);

    public static final DocumentStoreFixture RDB_DERBY = new RDBFixture("RDB-Derby(embedded)", RDBTestPropSupplier.DERBY);

    public static final DocumentStoreFixture RDB_H2 = new RDBFixture("RDB-H2(file)", RDBTestPropSupplier.H2);

    public static final DocumentStoreFixture RDB_MSSQL = new RDBFixture("RDB-MSSql", RDBTestPropSupplier.MSSQL);

    public static final DocumentStoreFixture RDB_MYSQL = new RDBFixture("RDB-MySQL", RDBTestPropSupplier.MYSQL);

    public static final DocumentStoreFixture RDB_ORACLE = new RDBFixture("RDB-Oracle", RDBTestPropSupplier.ORACLE);

    public static final DocumentStoreFixture RDB_PG = new RDBFixture("RDB-Postgres", RDBTestPropSupplier.POSTGRES);

    public static final String TABLEPREFIX = "dstest_";

    public static List<Object[]> getFixtures() {
        List<Object[]> fixtures = new ArrayList<>();
        if (FixturesHelper.getFixtures().contains(DOCUMENT_MEM)) {
            fixtures.add(new Object[] { new DocumentStoreFixture.MemoryFixture() });
        }

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if (FixturesHelper.getFixtures().contains(DOCUMENT_NS) && mongo.isAvailable()) {
            fixtures.add(new Object[] { mongo });
        }

        DocumentStoreFixture rdb = new DocumentStoreFixture.RDBFixture();
        if (FixturesHelper.getFixtures().contains(DOCUMENT_RDB) && rdb.isAvailable()) {
            fixtures.add(new Object[] { rdb });
        }
        return fixtures;
    }

    public abstract String getName();

    public abstract DocumentStore createDocumentStore(DocumentMK.Builder builder);

    public DocumentStore createDocumentStore(int clusterId) {
        return createDocumentStore(new DocumentMK.Builder().setClusterId(clusterId));
    }

    public DocumentStore createDocumentStore() {
        return createDocumentStore(new DocumentMK.Builder().setClusterId(1));
    }

    public boolean isAvailable() {
        return true;
    }

    // get underlying datasource if RDB persistence
    public DataSource getRDBDataSource() {
        return null;
    }

    // return false if the multiple instances will not share the same persistence
    public boolean hasSinglePersistence() {
        return true;
    }

    public String toString() {
        return getClass().getSimpleName() + ": "+ getName();
    }

    public void dispose() throws Exception {
    }

    public static class ClusterlikeMemoryFixture extends DocumentStoreFixture {

        private static MemoryDocumentStore sharedStore;

        @Override
        public String getName() {
            return "ClusterlikeMemory";
        }

        @Override
        public DocumentStore createDocumentStore(DocumentMK.Builder builder) {
            if (sharedStore == null) {
                sharedStore = new MemoryDocumentStore();
            }
            return new DocumentStoreWrapper(sharedStore);
        }

        @Override
        public boolean hasSinglePersistence() {
            return true;
        }

        @Override
        public void dispose() throws Exception {
            sharedStore = null;
            super.dispose();
        }

    }

    public static class MemoryFixture extends DocumentStoreFixture {

        @Override
        public String getName() {
            return "Memory";
        }

        @Override
        public DocumentStore createDocumentStore(DocumentMK.Builder builder) {
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
        RDBOptions options = new RDBOptions().tablePrefix(TABLEPREFIX).dropTablesOnClose(true);

        public RDBFixture() {
            // default RDB fixture
            this("RDB-H2(file)", "jdbc:h2:file:./target/ds-test2", "sa", "");
        }

        public RDBFixture(String name, String url, String username, String passwd) {
            this.name = name;
            try {
                dataSource = new RDBDataSourceWrapper(RDBDataSourceFactory.forJdbcUrl(url, username, passwd));
            } catch (Exception ex) {
                LOG.info("Database instance not available at {} because of '{}', skipping tests...", url, ex.getMessage());
            }
        }

        public RDBFixture(String name, RDBTestPropSupplier db) {
            this(name, db.url.loggingTo(LOG).get(), db.username.loggingTo(LOG).get(), db.passwd.loggingTo(LOG).get());
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public DocumentStore createDocumentStore(DocumentMK.Builder builder) {
            if (builder.getClusterId() == 1) {
                store1 = new RDBDocumentStore(dataSource, builder, options);
                return store1;
            } else if (builder.getClusterId() == 2) {
                store2 = new RDBDocumentStore(dataSource, builder, options);
                return store2;
            } else {
                throw new RuntimeException("expect clusterId == 1 or == 2");
            }
        }

        @Override
        public boolean isAvailable() {
            if (dataSource == null) {
                return false;
            } else {
                try (RDBConnectionHandler ch = new RDBConnectionHandler(dataSource); Connection c = ch.getRWConnection()) {
                    ch.closeConnection(c);
                    return true;
                } catch (Throwable t) {
                    LOG.info("Datasource failure for {} because of {}, skipping tests...", name,
                            t.getMessage() == null ? t : t.getMessage());
                    return false;
                }
            }
        }

        @Override
        public DataSource getRDBDataSource() {
            return dataSource;
        }

        @Override
        public void dispose() {
            if (dataSource instanceof Closeable) {
                try {
                    ((Closeable)dataSource).close();
                }
                catch (IOException ignored) {
                }
            }
        }

        public void setRDBOptions(RDBOptions options) {
            this.options = options;
        }
    }

    public static class MongoFixture extends DocumentStoreFixture {

        public static final boolean SKIP_MONGO = SystemPropertySupplier.create("oak.skipMongo", false).loggingTo(LOG).get();

        protected List<MongoConnection> connections = new ArrayList<>();

        @Override
        public String getName() {
            return "MongoDB";
        }

        @Override
        public DocumentStore createDocumentStore(DocumentMK.Builder builder) {
            try {
                MongoConnection connection = MongoUtils.getConnection();
                connections.add(connection);
                return new MongoDocumentStore(connection.getMongoClient(),
                        connection.getDatabase(), builder);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isAvailable() {
            return !SKIP_MONGO && MongoUtils.isAvailable();
        }

        @Override
        public void dispose() {
            try {
                MongoUtils.dropCollections(MongoUtils.DB);
            } catch (Exception ignore) {
            }
            for (MongoConnection c : connections) {
                try {
                    c.close();
                } catch (IllegalStateException e) {
                    // may happen when connection is already closed (OAK-7447)
                }
            }
            connections.clear();
        }
    }
}
