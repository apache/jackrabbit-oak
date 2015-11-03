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

/**
 * Convenience class that dumps the table creation statements for various
 * database types.
 */
public class RDBHelper {

    private static String[] databases = { "Apache Derby", "DB2", "H2", "Microsoft SQL Server", "MySQL", "Oracle", "PostgreSQL",
            "default" };

    public static void main(String[] args) {
        for (String database : databases) {
            System.out.println(database);
            System.out.println();

            RDBDocumentStoreDB ddb = RDBDocumentStoreDB.getValue(database);
            RDBBlobStoreDB bdb = RDBBlobStoreDB.getValue(database);

            for (String table : RDBDocumentStore.getTableNames()) {
                System.out.println("  " + ddb.getTableCreationStatement(table));
                for (String s : ddb.getIndexCreationStatements(table)) {
                    System.out.println("    " + s);
                }
            }
            System.out.println();

            System.out.println("  " + bdb.getMetaTableCreationStatement("DATASTORE_META"));
            System.out.println("  " + bdb.getDataTableCreationStatement("DATASTORE_DATA"));
            System.out.println();
            System.out.println();
        }
    }
}
