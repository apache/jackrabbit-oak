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

import java.util.Arrays;

/**
 * Convenience class that dumps the table creation statements for various
 * database types.
 * <p>
 * Run with:
 * 
 * <pre>
 * java -cp oak-run-<i>version</i>.jar org.apache.jackrabbit.oak.plugins.document.rdb.RDBHelper
 * </pre>
 */
public class RDBHelper {

    private static String[] DATABASES = { "Apache Derby", "DB2", "H2", "Microsoft SQL Server", "MySQL", "Oracle", "PostgreSQL",
            "default" };

    public static void main(String[] args) {

        RDBOptions defaultOpts = new RDBOptions();
        int initial = defaultOpts.getInitialSchema();
        int upgradeTo = defaultOpts.getUpgradeToSchema();
        System.out.println("Table Creation Statements for RDBBlobStore and RDBDocumentStore");
        System.out.println("RDBDocumentStore initial version: " + initial + ", with modifications up to version: " + upgradeTo);
        System.out.println(
                "(use system properties org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions.INITIALSCHEMA and org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions.UPGRADETOSCHEMA to specify initial DB schema, and schema to upgrade to)");
        System.out.println();

        for (String database : DATABASES) {
            internalDump(database, initial, upgradeTo);
        }
    }

    public static String getSupportedDatabases() {
        return Arrays.asList(DATABASES).toString();
    }

    public static void dump(String database, Integer initial, Integer upgrade) {
        RDBOptions defaultOpts = new RDBOptions();

        if (database == null) {
            internalDump(initial == null ? defaultOpts.getInitialSchema() : initial,
                    upgrade == null ? defaultOpts.getUpgradeToSchema() : upgrade);
        } else {
            internalDump(database, initial == null ? defaultOpts.getInitialSchema() : initial,
                    upgrade == null ? defaultOpts.getUpgradeToSchema() : upgrade);
        }
    }

    private static void internalDump(int initial, int upgradeTo) {
        for (String database : DATABASES) {
            internalDump(database, initial, upgradeTo);
            System.out.println();
        }
    }

    private static void internalDump(String database, int initial, int upgradeTo) {
        System.out.println("-- " + database);

        RDBDocumentStoreDB ddb = RDBDocumentStoreDB.getValue(database);
        RDBBlobStoreDB bdb = RDBBlobStoreDB.getValue(database);

        for (String table : RDBDocumentStore.getTableNames()) {
            System.out.println();
            System.out.println("  -- creating table " + table + " for schema version " + initial);
            System.out.println("  " + ddb.getTableCreationStatement(table, initial));
            for (String s : ddb.getIndexCreationStatements(table, initial)) {
                System.out.println("  " + s);
            }
            for (int level = initial + 1; level <= upgradeTo; level++) {
                System.out.println("  -- upgrading table " + table + " to schema version " + level);
                for (String statement : ddb.getTableUpgradeStatements(table, level)) {
                    System.out.println("  " + statement);
                }
            }
        }
        System.out.println();

        System.out.println("   -- creating blob store tables");
        System.out.println("  " + bdb.getMetaTableCreationStatement("DATASTORE_META"));
        System.out.println("  " + bdb.getDataTableCreationStatement("DATASTORE_DATA"));
        System.out.println();
        System.out.println();
    }
}
