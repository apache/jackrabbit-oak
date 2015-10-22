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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;

/**
 * Utility for dumping contents from {@link RDBDocumentStore}'s tables.
 */
public class RDBExport {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

        String url = null, user = null, pw = null, table = "nodes", query = null, dumpfile = null;
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
                } else if ("--from-db2-dump".equals(param)) {
                    dumpfile = args[++i];
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

        if (dumpfile != null && url != null) {
            System.err.println(RDBExport.class.getName() + ": must use either dump file or JDBC URL");
            printUsage();
            System.exit(2);
        } else if (dumpfile != null) {
            dumpFile(dumpfile, asArray, out, ser);
        } else {
            dumpJDBC(url, user, pw, table, query, asArray, out, ser);
        }

        out.flush();
        out.close();
    }

    private static void dumpFile(String filename, boolean asArray, PrintStream out, RDBDocumentSerializer ser) throws IOException {
        FileInputStream fis = new FileInputStream(new File(filename));
        InputStreamReader ir = new InputStreamReader(fis, "UTF-8");
        BufferedReader br = new BufferedReader(ir);
        // scan for column names
        String prev = null;
        String line = br.readLine();
        String columns = null;
        while (line != null && columns == null) {
            prev = line;
            line = br.readLine();
            if (line != null) {
                // remove spaces
                String stripped = line.replace(" ", "");
                if (stripped.length() != 0 && stripped.replace("-", "").length() == 0) {
                    columns = prev;
                }
            }
        }
        Map<String, Integer> starts = new HashMap<String, Integer>();
        Map<String, Integer> ends = new HashMap<String, Integer>();
        String cname = "";
        int cstart = 0;
        boolean inName = true;
        for (int i = 0; i < columns.length(); i++) {
            char c = columns.charAt(i);
            if (c == ' ') {
                if (inName == true) {
                    starts.put(cname, cstart);
                }
                inName = false;
            } else {
                if (inName == false) {
                    ends.put(cname, i - 1);
                    cname = "";
                    cstart = i;
                }
                cname += c;
                inName = true;
            }
        }
        // System.out.println("Found columns: " + starts + " " + ends + " " +
        // columns.length());
        if (asArray) {
            out.println("[");
        }
        boolean needComma = asArray;
        line = br.readLine();
        while (line != null) {
            try {
                String id = line.substring(starts.get("ID"), ends.get("ID")).trim();
                String smodified = line.substring(starts.get("MODIFIED"), ends.get("MODIFIED")).trim();
                String shasbinary = line.substring(starts.get("HASBINARY"), ends.get("HASBINARY")).trim();
                String sdeletedonce = line.substring(starts.get("DELETEDONCE"), ends.get("DELETEDONCE")).trim();
                String smodcount = line.substring(starts.get("MODCOUNT"), ends.get("MODCOUNT")).trim();
                String scmodcount = line.substring(starts.get("CMODCOUNT"), ends.get("CMODCOUNT")).trim();
                String sdata = line.substring(starts.get("DATA"), ends.get("DATA")).trim();
                String sbdata = line.substring(starts.get("BDATA")).trim(); // assumed
                                                                            // to
                                                                            // be
                                                                            // last
                byte[] bytes = null;
                if (sbdata.length() != 0 && !"-".equals(sbdata)) {
                    bytes = StringUtils.convertHexToBytes(sbdata.substring(1).replace("'", ""));
                }
                try {
                    RDBRow row = new RDBRow(id, "1".equals(shasbinary), "1".equals(sdeletedonce), Long.parseLong(smodified),
                            Long.parseLong(smodcount), Long.parseLong(scmodcount), sdata, bytes);
                    StringBuilder fulljson = dumpRow(ser, id, row);
                    if (asArray && needComma) {
                        fulljson.append(",");
                    }
                    out.println(fulljson);
                    needComma = true;
                } catch (DocumentStoreException ex) {
                    System.err.println("Error: skipping line for ID " + id + " because of " + ex.getMessage());
                }
            } catch (IndexOutOfBoundsException ex) {
                // ignored
            }
            line = br.readLine();
        }
        if (asArray) {
            out.println("]");
        }
    }

    private static void dumpJDBC(String url, String user, String pw, String table, String query, boolean asArray, PrintStream out,
            RDBDocumentSerializer ser) throws SQLException {
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
            StringBuilder fulljson = dumpRow(ser, id, row);
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

    private static StringBuilder dumpRow(RDBDocumentSerializer ser, String id, RDBRow row) {
        NodeDocument doc = ser.fromRow(Collection.NODES, row);
        String docjson = ser.asString(doc);
        StringBuilder fulljson = new StringBuilder();
        fulljson.append("{\"_id\":\"");
        JsopBuilder.escape(id, fulljson);
        fulljson.append("\",");
        fulljson.append(docjson.substring(1));
        return fulljson;
    }

    private static void printUsage() {
        System.err.println("Usage: " + RDBExport.class.getName()
                + " [-j/--jdbc-url JDBC-URL] [-u/--username username] [-p/--password password] [-c/--collection table] [-q/--query query][--jsonArray]");
    }
}
