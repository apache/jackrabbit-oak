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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;

public class RDBExport {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String url = null, user = null, pw = null, table = null, query = null;
        RDBDocumentSerializer ser = new RDBDocumentSerializer(new MemoryDocumentStore(), new HashSet<String>());

        try {
            url = args[0];
            user = args[1];
            pw = args[2];
            table = args[3];
            query = args.length >= 5 ? args[4] : null;
        } catch (IndexOutOfBoundsException ex) {
            System.err.println("Usage: ... " + RDBCreator.class.getName() + " JDBC-URL username password table [query]");
            System.exit(2);
        }

        String driver = RDBJDBCTools.driverForDBType(RDBJDBCTools.jdbctype(url));
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException ex) {
            System.err.println("Attempt to load class " + driver + " failed.");
        }
        Connection c = DriverManager.getConnection(url, user, pw);
        c.setReadOnly(true);
        Statement stmt = c.createStatement();
        String sql = "select ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA  from " + table;
        if (query != null) {
            sql += " where " + query;
        }
        sql += " order by id";
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            String id = rs.getString(1);
            long modified = rs.getLong(2);
            long modcount = rs.getLong(3);
            long cmodcount = rs.getLong(4);
            long hasBinary = rs.getLong(5);
            long deletedOnce = rs.getLong(6);
            String data = rs.getString(7);
            byte[] bdata = rs.getBytes(8);
            RDBRow row = new RDBRow(id, hasBinary == 1, deletedOnce == 1, modified, modcount, cmodcount, data, bdata);
            NodeDocument doc = ser.fromRow(Collection.NODES, row);
            System.out.println(ser.asString(doc));
        }
        rs.close();
        stmt.close();
        c.close();
    }
}
