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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class RDBCreator {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String url = null, user = null, pw = null, db = null;

        try {
            url = args[0];
            user = args[1];
            pw = args[2];
            db = args[3];
        } catch (IndexOutOfBoundsException ex) {
            System.err.println("Usage: ... " + RDBCreator.class.getName() + " JDBC-URL username password databasename");
            System.exit(2);
        }

        String driver = RDBJDBCTools.driverForDBType(RDBJDBCTools.jdbctype(url));
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException ex) {
            System.err.println("Attempt to load class " + driver + " failed.");
        }
        Connection c = DriverManager.getConnection(url, user, pw);
        Statement stmt = c.createStatement();
        stmt.execute("create database " + db);
        stmt.close();
        c.close();
        System.out.println("Database " + db + " created @ " + url + " using " + driver);
    }
}
