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

import java.sql.Connection;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBConnectionHandler;
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

    private static final String rdbDB2URL = SystemPropertySupplier.create("rdb-db2-jdbc-url", "jdbc:db2://localhost:50000/OAK")
            .loggingTo(LOG).get();
    private static final String rdbDB2User = SystemPropertySupplier.create("rdb-db2-jdbc-user", "oak").loggingTo(LOG).get();
    private static final String rdbDB2Passwd = SystemPropertySupplier.create("rdb-db2-jdbc-passwd", "geheim").hideValue()
            .loggingTo(LOG).get();

    public static final MyFixture RDB_DB2 = new MyFixture("RDB-DB2", rdbDB2URL, rdbDB2User, rdbDB2Passwd);

    private static final String rdbDerbyURL = SystemPropertySupplier
            .create("rdb-derby-jdbc-url", "jdbc:derby:./target/derby-ds-test;create=true").loggingTo(LOG).get();
    private static final String rdbDerbyUser = SystemPropertySupplier.create("rdb-derby-jdbc-user", "sa").loggingTo(LOG).get();
    private static final String rdbDerbyPasswd = SystemPropertySupplier.create("rdb-derby-jdbc-passwd", "").hideValue()
            .loggingTo(LOG).get();

    public static final MyFixture RDB_DERBY = new MyFixture("RDB-Derby(embedded)", rdbDerbyURL, rdbDerbyUser, rdbDerbyPasswd);

    private static final String rdbH2URL = SystemPropertySupplier.create("rdb-h2-jdbc-url", "jdbc:h2:file:./target/h2-ds-test")
            .loggingTo(LOG).get();
    private static final String rdbH2User = SystemPropertySupplier.create("rdb-h2-jdbc-user", "sa").loggingTo(LOG).get();
    private static final String rdbH2Passwd = SystemPropertySupplier.create("rdb-h2-jdbc-passwd", "").hideValue().loggingTo(LOG)
            .get();

    public static final MyFixture RDB_H2 = new MyFixture("RDB-H2(file)", rdbH2URL, rdbH2User, rdbH2Passwd);

    private static final String rdbMsSQLURL = SystemPropertySupplier
            .create("rdb-mssql-jdbc-url", "jdbc:sqlserver://localhost:1433;databaseName=OAK").loggingTo(LOG).get();
    private static final String rdbMsSQLUser = SystemPropertySupplier.create("rdb-mssql-jdbc-user", "sa").loggingTo(LOG).get();
    private static final String rdbMsSQLPasswd = SystemPropertySupplier.create("rdb-mssql-jdbc-passwd", "geheim").hideValue()
            .loggingTo(LOG).get();

    public static final MyFixture RDB_MSSQL = new MyFixture("RDB-MSSql", rdbMsSQLURL, rdbMsSQLUser, rdbMsSQLPasswd);

    private static final String rdbMySQLURL = SystemPropertySupplier.create("rdb-mysql-jdbc-url", "jdbc:mysql://localhost:3306/oak")
            .loggingTo(LOG).get();
    private static final String rdbMySQLUser = SystemPropertySupplier.create("rdb-mysql-jdbc-user", "root").loggingTo(LOG).get();
    private static final String rdbMySQLPasswd = SystemPropertySupplier.create("rdb-mysql-jdbc-passwd", "geheim").hideValue()
            .loggingTo(LOG).get();

    public static final MyFixture RDB_MYSQL = new MyFixture("RDB-MySQL", rdbMySQLURL, rdbMySQLUser, rdbMySQLPasswd);

    private static final String rdbOracleURL = SystemPropertySupplier
            .create("rdb-oracle-jdbc-url", "jdbc:oracle:thin:@localhost:1521:orcl").loggingTo(LOG).get();
    private static final String rdbOracleUser = SystemPropertySupplier.create("rdb-oracle-jdbc-user", "system").loggingTo(LOG)
            .get();
    private static final String rdbOraclePasswd = SystemPropertySupplier.create("rdb-oracle-jdbc-passwd", "geheim").hideValue()
            .loggingTo(LOG).get();

    public static final MyFixture RDB_ORACLE = new MyFixture("RDB-Oracle", rdbOracleURL, rdbOracleUser, rdbOraclePasswd);

    private static final String rdbPostgresURL = SystemPropertySupplier.create("rdb-postgres-jdbc-url", "jdbc:postgresql:oak")
            .loggingTo(LOG).get();
    private static final String rdbPostgresUser = SystemPropertySupplier.create("rdb-postgres-jdbc-user", "postgres").loggingTo(LOG)
            .get();
    private static final String rdbPostgresPasswd = SystemPropertySupplier.create("rdb-postgres-jdbc-passwd", "geheim").hideValue()
            .loggingTo(LOG).get();

    public static final MyFixture RDB_PG = new MyFixture("RDB-Postgres", rdbPostgresURL, rdbPostgresUser, rdbPostgresPasswd);

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
                LOG.info("Database instance not available at {} because of '{}', skipping tests...", url, ex.getMessage());
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
        public void dispose() {
            if (this.bs != null) {
                this.bs.close();
                this.bs = null;
            }
        }
    }
}
