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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceWrapper;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.mongodb.DB;

import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_MEM;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_NS;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_RDB;

public abstract class DocumentStoreFixture {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentStoreFixture.class);

    public static final DocumentStoreFixture MEMORY = new MemoryFixture();
    public static final DocumentStoreFixture MONGO = new MongoFixture();

    public static final DocumentStoreFixture RDB_DB2 = new RDBFixture("RDB-DB2", System.getProperty("rdb-db2-jdbc-url",
            "jdbc:db2://localhost:50000/OAK"), System.getProperty("rdb-db2-jdbc-user", "oak"), System.getProperty(
            "rdb-db2-jdbc-passwd", "geheim"));
    public static final DocumentStoreFixture RDB_DERBY = new RDBFixture("RDB-Derby(embedded)", System.getProperty(
            "rdb-derby-jdbc-url", "jdbc:derby:./target/derby-ds-test;create=true"),
            System.getProperty("rdb-derby-jdbc-user", "sa"), System.getProperty("rdb-derby-jdbc-passwd", ""));
    public static final DocumentStoreFixture RDB_H2 = new RDBFixture("RDB-H2(file)", System.getProperty("rdb-h2-jdbc-url",
            "jdbc:h2:file:./target/h2-ds-test"), System.getProperty("rdb-h2-jdbc-user", "sa"), System.getProperty(
            "rdb-h2-jdbc-passwd", ""));
    public static final DocumentStoreFixture RDB_MSSQL = new RDBFixture("RDB-MSSql", System.getProperty("rdb-mssql-jdbc-url",
            "jdbc:sqlserver://localhost:1433;databaseName=OAK"), System.getProperty("rdb-mssql-jdbc-user", "sa"),
            System.getProperty("rdb-mssql-jdbc-passwd", "geheim"));
    public static final DocumentStoreFixture RDB_MYSQL = new RDBFixture("RDB-MySQL", System.getProperty("rdb-mysql-jdbc-url",
            "jdbc:mysql://localhost:3306/oak"), System.getProperty("rdb-mysql-jdbc-user", "root"), System.getProperty(
            "rdb-mysql-jdbc-passwd", "geheim"));
    public static final DocumentStoreFixture RDB_ORACLE = new RDBFixture("RDB-Oracle", System.getProperty("rdb-oracle-jdbc-url",
            "jdbc:oracle:thin:@localhost:1521:orcl"), System.getProperty("rdb-oracle-jdbc-user", "system"), System.getProperty(
            "rdb-oracle-jdbc-passwd", "geheim"));
    public static final DocumentStoreFixture RDB_PG = new RDBFixture("RDB-Postgres", System.getProperty("rdb-postgres-jdbc-url",
            "jdbc:postgresql:oak"), System.getProperty("rdb-postgres-jdbc-user", "postgres"), System.getProperty(
            "rdb-postgres-jdbc-passwd", "geheim"));

    public static final String TABLEPREFIX = "dstest_";

    public static List<Object[]> getFixtures() {
        List<Object[]> fixtures = Lists.newArrayList();
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

    public abstract DocumentStore createDocumentStore(int clusterId);

    public DocumentStore createDocumentStore() {
        return createDocumentStore(1);
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
                store1 = new RDBDocumentStore(dataSource, new DocumentMK.Builder().setClusterId(1), options);
                return store1;
            } else if (clusterId == 2) {
                store2 = new RDBDocumentStore(dataSource, new DocumentMK.Builder().setClusterId(2), options);
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
    }

    public static class MongoFixture extends DocumentStoreFixture {
        private List<MongoConnection> connections = Lists.newArrayList();

        @Override
        public String getName() {
            return "MongoDB";
        }

        @Override
        public DocumentStore createDocumentStore(int clusterId) {
            try {
                MongoConnection connection = MongoUtils.getConnection();
                connections.add(connection);
                DB db = connection.getDB();
                return new MongoDocumentStore(db, new DocumentMK.Builder().setClusterId(clusterId));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isAvailable() {
            return MongoUtils.isAvailable();
        }

        @Override
        public void dispose() {
            try {
                MongoUtils.dropCollections(MongoUtils.DB);
            } catch (Exception ignore) {
            }
            for (MongoConnection c : connections) {
                c.close();
            }
            connections.clear();
        }
    }
}
