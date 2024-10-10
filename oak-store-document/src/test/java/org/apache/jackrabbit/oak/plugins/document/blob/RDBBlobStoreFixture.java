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

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBConnectionHandler;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceWrapper;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBTestPropSupplier;
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

    public static final MyFixture RDB_DB2 = new MyFixture("RDB-DB2", RDBTestPropSupplier.DB2);

    public static final MyFixture RDB_DERBY = new MyFixture("RDB-Derby(embedded)", RDBTestPropSupplier.DERBY);

    public static final MyFixture RDB_H2 = new MyFixture("RDB-H2(file)", RDBTestPropSupplier.H2);

    public static final MyFixture RDB_MSSQL = new MyFixture("RDB-MSSql", RDBTestPropSupplier.MSSQL);

    public static final MyFixture RDB_MYSQL = new MyFixture("RDB-MySQL", RDBTestPropSupplier.MYSQL);

    public static final MyFixture RDB_ORACLE = new MyFixture("RDB-Oracle", RDBTestPropSupplier.ORACLE);

    public static final MyFixture RDB_PG = new MyFixture("RDB-Postgres", RDBTestPropSupplier.POSTGRES);

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

        public MyFixture(String name, RDBTestPropSupplier db) {
            this(name, db.url.loggingTo(LOG).get(), db.username.loggingTo(LOG).get(), db.passwd.loggingTo(LOG).get());
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
