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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;

/**
 * Utility for dumping contents from {@link RDBDocumentStore}'s tables.
 */
public class RDBExport {

    public static void main(String[] args)
            throws ClassNotFoundException, SQLException, UnsupportedEncodingException, FileNotFoundException {

        String url = null, user = null, pw = null, table = "nodes", query = null;
        boolean asArray = false;
        PrintStream out = System.out;
        Set<String> excl = new HashSet<String>();
        excl.add(Document.ID);
        RDBDocumentSerializer ser = new RDBDocumentSerializer(new MemoryDocumentStore(), excl);

        String param = null;
        try {
            for (int i = 0; i < args.length; i++) {
                param = args[i];
                if ("-u".equals(param) || "--username".equals(param)) {
                    user = args[++i];
                } else if ("-p".equals(param) || "--password".equals(param)) {
                    pw = args[++i];
                } else if ("-c".equals(param) || "--collection".equals(param)) {
                    table = args[++i];
                } else if ("-j".equals(param) || "--jdbc-url".equals(param)) {
                    url = args[++i];
                } else if ("-q".equals(param) || "--query".equals(param)) {
                    query = args[++i];
                } else if ("-o".equals(param) || "--out".equals(param)) {
                    OutputStream os = new FileOutputStream(args[++i]);
                    out = new PrintStream(os, true, "UTF-8");
                } else if ("--jsonArray".equals(param)) {
                    asArray = true;
                } else {
                    System.err.println(RDBExport.class.getName() + ": invalid parameter " + args[i]);
                    printUsage();
                    System.exit(2);
                }
            }
        } catch (IndexOutOfBoundsException ex) {
            System.err.println(RDBExport.class.getName() + ": value missing for parameter " + param);
            printUsage();
            System.exit(2);
        }

        String driver = RDBJDBCTools.driverForDBType(RDBJDBCTools.jdbctype(url));
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException ex) {
            System.err.println(RDBExport.class.getName() + ":attempt to load class " + driver + " failed:" + ex.getMessage());
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

        if (asArray) {
            out.println("[");
        }
        boolean needComma = asArray;
        while (rs.next()) {
            String id = rs.getString("ID");
            long modified = rs.getLong("MODIFIED");
            long modcount = rs.getLong("MODCOUNT");
            long cmodcount = rs.getLong("CMODCOUNT");
            long hasBinary = rs.getLong("HASBINARY");
            long deletedOnce = rs.getLong("DELETEDONCE");
            String data = rs.getString("DATA");
            byte[] bdata = rs.getBytes("BDATA");
            RDBRow row = new RDBRow(id, hasBinary == 1, deletedOnce == 1, modified, modcount, cmodcount, data, bdata);
            NodeDocument doc = ser.fromRow(Collection.NODES, row);

            String docjson = ser.asString(doc);
            StringBuilder fulljson = new StringBuilder();
            fulljson.append("{\"_id\":\"");
            JsopBuilder.escape(id, fulljson);
            fulljson.append("\",");
            fulljson.append(docjson);
            fulljson.append("}");
            if (asArray && needComma && !rs.isLast()) {
                fulljson.append(",");
            }
            out.println(fulljson);
            needComma = true;
        }
        if (asArray) {
            out.println("]");
        }
        out.close();
        rs.close();
        stmt.close();
        c.close();
    }

    private static void printUsage() {
        System.err.println("Usage: " + RDBExport.class.getName()
                + " [-j/--jdbc-url JDBC-URL] [-u/--username username] [-p/--password password] [-c/--collection table] [-q/--query query][--jsonArray]");
    }
}
