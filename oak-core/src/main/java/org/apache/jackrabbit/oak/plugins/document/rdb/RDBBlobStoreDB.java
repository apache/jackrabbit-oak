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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines variation in the capabilities of different RDBs.
 */
public enum RDBBlobStoreDB {
    H2("H2") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 1, 4, description);
        }
    },

    DERBY("Apache Derby") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 10, 11, description);
        }
    },

    DB2("DB2") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 10, 1, description);
        }

        @Override
        public String getDataTableCreationStatement(String tableName) {
            return "create table " + tableName + " (ID varchar(" + RDBBlobStore.IDSIZE + ") not null primary key, DATA blob("
                    + MINBLOB + "))";
        }
    },

    MSSQL("Microsoft SQL Server") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 11, 0, description);
        }

        @Override
        public String getDataTableCreationStatement(String tableName) {
            return "create table " + tableName + " (ID varchar(" + RDBBlobStore.IDSIZE
                    + ") not null primary key, DATA varbinary(max))";
        }
    },

    MYSQL("MySQL") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 5, 5, description);
        }

        @Override
        public String getDataTableCreationStatement(String tableName) {
            return "create table " + tableName + " (ID varchar(" + RDBBlobStore.IDSIZE + ") not null primary key, DATA mediumblob)";
        }
    },

    ORACLE("Oracle") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 12, 1, 12, 1, description);
        }

        @Override
        public String getMetaTableCreationStatement(String tableName) {
            return "create table " + tableName + " (ID varchar(" + RDBBlobStore.IDSIZE
                    + ") not null primary key, LVL number, LASTMOD number)";
        }
    },

    POSTGRES("PostgreSQL") {
        @Override
        public String checkVersion(DatabaseMetaData md) throws SQLException {
            return RDBJDBCTools.versionCheck(md, 9, 5, 9, 4, description);
        }

        @Override
        public String getDataTableCreationStatement(String tableName) {
            return "create table " + tableName + " (ID varchar(" + RDBBlobStore.IDSIZE + ") not null primary key, DATA bytea)";
        }
    },

    DEFAULT("default") {
    };

    private static final Logger LOG = LoggerFactory.getLogger(RDBBlobStoreDB.class);

    // blob size we need to support
    private static final int MINBLOB = 2 * 1024 * 1024;

    public String checkVersion(DatabaseMetaData md) throws SQLException {
        return "Unknown database type: " + md.getDatabaseProductName();
    }

    public String getDataTableCreationStatement(String tableName) {
        return "create table " + tableName + " (ID varchar(" + RDBBlobStore.IDSIZE + ") not null primary key, DATA blob)";
    }

    public String getMetaTableCreationStatement(String tableName) {
        return "create table " + tableName + " (ID varchar(" + RDBBlobStore.IDSIZE
                + ") not null primary key, LVL int, LASTMOD bigint)";
    }

    protected String description;

    private RDBBlobStoreDB(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return this.description;
    }

    @Nonnull
    public static RDBBlobStoreDB getValue(String desc) {
        for (RDBBlobStoreDB db : RDBBlobStoreDB.values()) {
            if (db.description.equals(desc)) {
                return db;
            } else if (db == DB2 && desc.startsWith("DB2/")) {
                return db;
            }
        }

        LOG.error("DB type " + desc + " unknown, trying default settings");
        DEFAULT.description = desc + " - using default settings";
        return DEFAULT;
    }
}
