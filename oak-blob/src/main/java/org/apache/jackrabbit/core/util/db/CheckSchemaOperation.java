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
package org.apache.jackrabbit.core.util.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.util.Text;

/**
 * An operation which synchronously checks the DB schema in the {@link #run()} method. The
 * {@link #addVariableReplacement(String, String)} method return the instance to enable method chaining.
 */
public class CheckSchemaOperation {

    public static final String SCHEMA_OBJECT_PREFIX_VARIABLE = "${schemaObjectPrefix}";

    public static final String TABLE_SPACE_VARIABLE = "${tableSpace}";

    private final ConnectionHelper conHelper;

    private final InputStream ddl;

    private final String table;

    private final Map<String, String> varReplacement = new HashMap<String, String>();

    /**
     * @param connectionhelper the connection helper
     * @param ddlStream the stream of the DDL to use to create the schema if necessary (closed by the
     *            {@link #run()} method)
     * @param tableName the name of the table to use for the schema-existence-check
     */
    public CheckSchemaOperation(ConnectionHelper connectionhelper, InputStream ddlStream, String tableName) {
        conHelper = connectionhelper;
        ddl = ddlStream;
        table = tableName;
    }

    /**
     * Adds a variable replacement mapping.
     * 
     * @param var the variable
     * @param replacement the replacement value
     * @return this
     */
    public CheckSchemaOperation addVariableReplacement(String var, String replacement) {
        varReplacement.put(var, replacement);
        return this;
    }

    /**
     * Checks if the required schema objects exist and creates them if they don't exist yet.
     * 
     * @throws SQLException if an error occurs
     * @throws IOException if an error occurs
     */
    public void run() throws SQLException, IOException {
        try {
            if (!conHelper.tableExists(table)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(ddl));
                String sql = reader.readLine();
                while (sql != null) {
                    // Skip comments and empty lines
                    if (!sql.startsWith("#") && sql.length() > 0) {
                        // replace prefix variable
                        sql = replace(sql);
                        // execute sql stmt
                        conHelper.exec(sql);
                    }
                    // read next sql stmt
                    sql = reader.readLine();
                }
            }
        } finally {
            IOUtils.closeQuietly(ddl);
        }
    }

    /**
     * Applies the variable replacement to the given string.
     * 
     * @param sql the string in which to replace variables
     * @return the new string
     */
    private String replace(String sql) {
        String result = sql;
        for (Map.Entry<String, String> entry : varReplacement.entrySet()) {
            result = Text.replace(result, entry.getKey(), entry.getValue()).trim();
        }
        return result;
    }
}
