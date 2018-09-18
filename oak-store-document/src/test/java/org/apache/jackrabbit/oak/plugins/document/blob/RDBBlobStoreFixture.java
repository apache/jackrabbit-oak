/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.blob;

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceWrapper;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RDBBlobStoreFixture {

    private static final Logger LOG = LoggerFactory.getLogger(RDBBlobStoreFixture.class);
    public static final String TABLEPREFIX = "bstest_";

    public abstract RDBBlobStore createRDBBlobStore();

    public abstract RDBDataSourceWrapper getDataSource();

    public abstract String getName();

    public abstract void dispose();

    public abstract boolean isAvailable();

    public static final RDBBlobStoreFixture RDB_DB2 = new MyFixture("RDB-DB2", System.getProperty("rdb-db2-jdbc-url",
            "jdbc:db2://localhost:50000/OAK"), System.getProperty("rdb-db2-jdbc-user", "oak"), System.getProperty(
            "rdb-db2-jdbc-passwd", "geheim"));
    public static final RDBBlobStoreFixture RDB_MYSQL = new MyFixture("RDB-MySQL", System.getProperty("rdb-mysql-jdbc-url",
            "jdbc:mysql://localhost:3306/oak"), System.getProperty("rdb-mysql-jdbc-user", "root"), System.getProperty(
            "rdb-mysql-jdbc-passwd", "geheim"));
    public static final RDBBlobStoreFixture RDB_ORACLE = new MyFixture("RDB-Oracle", System.getProperty("rdb-oracle-jdbc-url",
            "jdbc:oracle:thin:@localhost:1521:orcl"), System.getProperty("rdb-oracle-jdbc-user", "system"), System.getProperty(
            "rdb-oracle-jdbc-passwd", "geheim"));
    public static final RDBBlobStoreFixture RDB_MSSQL = new MyFixture("RDB-MSSql", System.getProperty("rdb-mssql-jdbc-url",
            "jdbc:sqlserver://localhost:1433;databaseName=OAK"), System.getProperty("rdb-mssql-jdbc-user", "sa"),
            System.getProperty("rdb-mssql-jdbc-passwd", "geheim"));
    public static final RDBBlobStoreFixture RDB_H2 = new MyFixture("RDB-H2(file)", System.getProperty("rdb-h2-jdbc-url",
            "jdbc:h2:file:./target/hs-bs-test"), System.getProperty("rdb-h2-jdbc-user", "sa"), System.getProperty(
            "rdb-h2-jdbc-passwd", ""));
    public static final RDBBlobStoreFixture RDB_DERBY = new MyFixture("RDB-Derby(embedded)", System.getProperty(
            "rdb-derby-jdbc-url", "jdbc:derby:./target/derby-bs-test;create=true"),
            System.getProperty("rdb-derby-jdbc-user", "sa"), System.getProperty("rdb-derby-jdbc-passwd", ""));
    public static final RDBBlobStoreFixture RDB_PG = new MyFixture("RDB-Postgres", System.getProperty("rdb-postgres-jdbc-url",
            "jdbc:postgresql:oak"), System.getProperty("rdb-postgres-jdbc-user", "postgres"), System.getProperty(
            "rdb-postgres-jdbc-passwd", "geheim"));

    public String toString() {
        return getClass().getSimpleName() + ": "+ getName();
    }

    private static class MyFixture extends RDBBlobStoreFixture {

        private String name;
        private RDBDataSourceWrapper dataSource;
        private RDBBlobStore bs;
        private RDBOptions options = new RDBOptions().tablePrefix(TABLEPREFIX).dropTablesOnClose(true);

        public MyFixture(String name, String url, String username, String passwd) {
            this.name = name;
            try {
                dataSource = new RDBDataSourceWrapper(RDBDataSourceFactory.forJdbcUrl(url, username, passwd));
            } catch (Exception ex) {
                LOG.info("Database instance not available at " + url + ", skipping tests...", ex);
            }
        }

        public RDBDataSourceWrapper getDataSource() {
            return dataSource;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public RDBBlobStore createRDBBlobStore() {
            bs = new RDBBlobStore(dataSource, options);
            return bs;
        }

        @Override
        public boolean isAvailable() {
            return dataSource != null;
        }

        @Override
        public void dispose() {
            if (this.bs != null) {
                this.bs.close();
                this.bs = null;
            }
        }
    }
}
