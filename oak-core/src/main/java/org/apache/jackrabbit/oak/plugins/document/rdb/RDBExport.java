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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.util.OakVersion;

/**
 * Utility for dumping contents from {@link RDBDocumentStore}'s tables.
 */
public class RDBExport {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private enum Format {
        JSON, JSONARRAY, CSV
    };

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

        String url = null, user = null, pw = null, table = "nodes", query = null, dumpfile = null, lobdir = null;
        List<String> fieldList = Collections.emptyList();
        Format format = Format.JSON;
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
                } else if ("--lobdir".equals(param)) {
                    lobdir = args[++i];
                } else if ("--jsonArray".equals(param)) {
                    format = Format.JSONARRAY;
                } else if ("--csv".equals(param)) {
                    format = Format.CSV;
                } else if ("--fields".equals(param)) {
                    String fields = args[++i];
                    fieldList = Arrays.asList(fields.split(","));
                } else if ("--version".equals(param)) {
                    System.out.println(RDBExport.class.getName() + " version " + OakVersion.getVersion());
                    System.exit(0);
                } else if ("--help".equals(param)) {
                    printHelp();
                    System.exit(0);
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

        if (format == Format.CSV && fieldList.isEmpty()) {
            System.err.println(RDBExport.class.getName() + ": csv output requires specification of field list");
            System.exit(2);
        }

        // JSON output with fieldList missing "_id"
        if ((format == Format.JSON || format == Format.JSONARRAY) && !fieldList.isEmpty() && !fieldList.contains("_id")) {
            fieldList = new ArrayList<String>(fieldList);
            fieldList.add(0, "_id");
        }

        if (dumpfile == null && url == null) {
            System.err.println(RDBExport.class.getName() + ": must use either dump file or JDBC URL");
            printUsage();
            System.exit(2);
        } else if (dumpfile != null) {
            dumpFile(dumpfile, lobdir, format, out, fieldList, ser);
        } else {
            dumpJDBC(url, user, pw, table, query, format, out, fieldList, ser);
        }

        out.flush();
        out.close();
    }

    private static void dumpFile(String filename, String lobdir, Format format, PrintStream out, List<String> fieldNames,
            RDBDocumentSerializer ser) throws IOException {
        File f = new File(filename);
        File lobDirectory = lobdir == null ? new File(f.getParentFile(), "lobdir") : new File(lobdir);
        FileInputStream fis = new FileInputStream(f);
        InputStreamReader ir = new InputStreamReader(fis, UTF8);
        BufferedReader br = new BufferedReader(ir);

        if (format == Format.JSONARRAY) {
            out.println("[");
        } else if (format == Format.CSV) {
            out.println(dumpFieldNames(fieldNames));
        }
        boolean needComma = format == Format.JSONARRAY;
        String line = br.readLine();
        while (line != null) {
            ArrayList<String> fields = parseDel(line);
            String id = fields.get(0);
            String smodified = fields.get(1);
            String shasbinary = fields.get(2);
            String sdeletedonce = fields.get(3);
            String smodcount = fields.get(4);
            String scmodcount = fields.get(5);
            String sdata = fields.get(7);
            String sbdata = fields.get(8);

            byte[] bytes = null;
            if (sbdata.length() != 0) {
                String lobfile = sbdata.replace("/", "");
                int lastdot = lobfile.lastIndexOf('.');
                String length = lobfile.substring(lastdot + 1);
                lobfile = lobfile.substring(0, lastdot);
                lastdot = lobfile.lastIndexOf('.');
                String startpos = lobfile.substring(lastdot + 1);
                lobfile = lobfile.substring(0, lastdot);
                int s = Integer.valueOf(startpos);
                int l = Integer.valueOf(length);
                File lf = new File(lobDirectory, lobfile);
                InputStream is = new FileInputStream(lf);
                bytes = new byte[l];
                IOUtils.skip(is, s);
                IOUtils.read(is, bytes, 0, l);
                IOUtils.closeQuietly(is);
            }
            try {
                RDBRow row = new RDBRow(id, "1".equals(shasbinary), "1".equals(sdeletedonce),
                        smodified.length() == 0 ? 0 : Long.parseLong(smodified), Long.parseLong(smodcount),
                        Long.parseLong(scmodcount), sdata, bytes);
                StringBuilder fulljson = dumpRow(ser, id, row);
                if (format == Format.CSV) {
                    out.println(asCSV(fieldNames, fulljson));
                } else {
                    fulljson = asJSON(fieldNames, fulljson);
                    if (format == Format.JSONARRAY && needComma) {
                        fulljson.append(",");
                    }
                    out.println(fulljson);
                    needComma = true;
                }
            } catch (DocumentStoreException ex) {
                System.err.println("Error: skipping line for ID " + id + " because of " + ex.getMessage());
            }
            line = br.readLine();
        }
        br.close();
        if (format == Format.JSONARRAY) {
            out.println("]");
        }
    }

    private static ArrayList<String> parseDel(String line) {
        ArrayList<String> result = new ArrayList<String>();

        boolean inQuoted = false;
        char quotechar = '"';
        char fielddelim = ',';
        StringBuilder value = new StringBuilder();
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (!inQuoted) {
                if (c == fielddelim) {
                    result.add(value.toString());
                    value = new StringBuilder();
                } else {
                    if (value.length() == 0 && c == quotechar) {
                        inQuoted = true;
                    } else {
                        value.append(c);
                    }
                }
            } else {
                if (c == quotechar) {
                    if (i + 1 != line.length() && line.charAt(i + 1) == quotechar) {
                        // quoted quote char
                        value.append(c);
                        i += 1;
                    } else {
                        inQuoted = false;
                    }
                } else {
                    value.append(c);
                }
            }
        }
        result.add(value.toString());

        return result;
    }

    private static void dumpJDBC(String url, String user, String pw, String table, String query, Format format, PrintStream out,
            List<String> fieldNames, RDBDocumentSerializer ser) throws SQLException {
        String driver = RDBJDBCTools.driverForDBType(RDBJDBCTools.jdbctype(url));
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException ex) {
            System.err.println(RDBExport.class.getName() + ":attempt to load class " + driver + " failed:" + ex.getMessage());
        }
        Connection c = DriverManager.getConnection(url, user, pw);
        c.setReadOnly(true);
        Statement stmt = c.createStatement();
        String sql = "select ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA from " + table;
        if (query != null) {
            sql += " where " + query;
        }
        sql += " order by id";
        ResultSet rs = stmt.executeQuery(sql);

        if (format == Format.JSONARRAY) {
            out.println("[");
        } else if (format == Format.CSV) {
            out.println(dumpFieldNames(fieldNames));
        }
        boolean needComma = format == Format.JSONARRAY;
        ResultSetMetaData rsm = null;
        boolean idIsAscii = true;
        while (rs.next()) {
            if (rsm == null) {
                rsm = rs.getMetaData();
                idIsAscii = !isBinaryType(rsm.getColumnType(1));
            }
            String id = idIsAscii ? rs.getString("ID") : new String(rs.getBytes("ID"), UTF8);
            long modified = rs.getLong("MODIFIED");
            long modcount = rs.getLong("MODCOUNT");
            long cmodcount = rs.getLong("CMODCOUNT");
            long hasBinary = rs.getLong("HASBINARY");
            long deletedOnce = rs.getLong("DELETEDONCE");
            String data = rs.getString("DATA");
            byte[] bdata = rs.getBytes("BDATA");

            RDBRow row = new RDBRow(id, hasBinary == 1, deletedOnce == 1, modified, modcount, cmodcount, data, bdata);
            StringBuilder fulljson = dumpRow(ser, id, row);
            if (format == Format.CSV) {
                out.println(asCSV(fieldNames, fulljson));
            } else {
                fulljson = asJSON(fieldNames, fulljson);
                if (format == Format.JSONARRAY && needComma && !rs.isLast()) {
                    fulljson.append(",");
                }
                out.println(fulljson);
                needComma = true;
            }
        }
        if (format == Format.JSONARRAY) {
            out.println("]");
        }
        out.close();
        rs.close();
        stmt.close();
        c.close();
    }

    @Nonnull
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

    @Nonnull
    private static String dumpFieldNames(List<String> fieldNames) {
        StringBuilder result = new StringBuilder();
        for (String f : fieldNames) {
            if (result.length() != 0) {
                result.append(',');
            }
            result.append(f);
        }
        return result.toString();
    }

    @Nonnull
    private static StringBuilder asJSON(List<String> fieldNames, StringBuilder fulljson) {
        if (fieldNames.isEmpty()) {
            return fulljson;
        } else {
            JsopTokenizer t = new JsopTokenizer(fulljson.toString());
            Map<String, Object> doc = (Map<String, Object>) readValueFromJson(t);
            StringBuilder buf = new StringBuilder();
            buf.append('{');
            String delim = "";
            for (String field : fieldNames) {
                buf.append(delim);
                delim = ",";
                String[] fn = field.split("\\.");
                if (doc.containsKey(fn[0])) {
                    Object o = doc.get(fn[0]);
                    appendJsonMember(buf, fn[0], o);
                }
            }
            buf.append('}');
            return buf;
        }
    }

    private static void appendJsonMember(StringBuilder sb, String key, Object value) {
        appendJsonString(sb, key);
        sb.append(":");
        appendJsonValue(sb, value);
    }

    private static void appendJsonString(StringBuilder sb, String s) {
        sb.append('"');
        JsopBuilder.escape(s, sb);
        sb.append('"');
    }

    private static void appendJsonMap(StringBuilder sb, Map<Object, Object> map) {
        sb.append("{");
        boolean needComma = false;
        for (Map.Entry<Object, Object> e : map.entrySet()) {
            if (needComma) {
                sb.append(",");
            }
            appendJsonMember(sb, e.getKey().toString(), e.getValue());
            needComma = true;
        }
        sb.append("}");
    }

    private static void appendJsonValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Number) {
            sb.append(value.toString());
        } else if (value instanceof Boolean) {
            sb.append(value.toString());
        } else if (value instanceof String) {
            appendJsonString(sb, (String) value);
        } else if (value instanceof Map) {
            appendJsonMap(sb, (Map<Object, Object>) value);
        } else {
            throw new IllegalArgumentException("unexpected type: " + value.getClass());
        }
    }

    @Nonnull
    private static StringBuilder asCSV(List<String> csvFieldNames, StringBuilder fulljson) {
        JsopTokenizer t = new JsopTokenizer(fulljson.toString());
        Map<String, Object> doc = (Map<String, Object>) readValueFromJson(t);
        StringBuilder buf = new StringBuilder();
        String delim = "";
        for (String field : csvFieldNames) {
            buf.append(delim);
            delim = ",";
            String[] fn = field.split("\\.");
            boolean checkMember = fn.length > 1;
            if (doc.containsKey(fn[0])) {
                Object o = doc.get(fn[0]);
                if (checkMember) {
                    if (o instanceof Map) {
                        Map<String, Object> m = (Map<String, Object>) o;
                        if (m.containsKey(fn[1])) {
                            dumpJsonValuetoCsv(buf, m.get(fn[1]));
                        }
                    }
                } else {
                    dumpJsonValuetoCsv(buf, o);
                }
            }
        }
        return buf;
    }

    private static void dumpJsonValuetoCsv(StringBuilder buf, Object o) {
        if (o == null) {
            buf.append("null");
        } else if (o instanceof Boolean) {
            buf.append(o.toString());
        } else if (o instanceof Long) {
            buf.append(((Long) o).longValue());
        } else {
            buf.append('"');
            buf.append(o.toString().replace("\"", "\"\""));
            buf.append('"');
        }
    }

    @Nullable
    private static Object readValueFromJson(@Nonnull JsopTokenizer json) {
        switch (json.read()) {
            case JsopReader.NULL:
                return null;
            case JsopReader.TRUE:
                return true;
            case JsopReader.FALSE:
                return false;
            case JsopReader.NUMBER:
                return Long.parseLong(json.getToken());
            case JsopReader.STRING:
                return json.getToken();
            case '{':
                Map<String, Object> map = new HashMap<String, Object>();
                while (true) {
                    if (json.matches('}')) {
                        break;
                    }
                    String k = json.readString();
                    if (k == null) {
                        throw new IllegalArgumentException();
                    }
                    json.read(':');
                    map.put(k, readValueFromJson(json));
                    json.matches(',');
                }
                return map;
            case '[':
                List<Object> list = new ArrayList<Object>();
                while (true) {
                    if (json.matches(']')) {
                        break;
                    }
                    list.add(readValueFromJson(json));
                    json.matches(',');
                }
                return list;
            default:
                throw new IllegalArgumentException(json.readRawValue());
        }
    }

    private static boolean isBinaryType(int sqlType) {
        return sqlType == Types.VARBINARY || sqlType == Types.BINARY || sqlType == Types.LONGVARBINARY;
    }

    private static void printUsage() {
        System.err.println("Usage: " + RDBExport.class.getName()
                + " -j/--jdbc-url JDBC-URL [-u/--username username] [-p/--password password] [-c/--collection table] [-q/--query query] [-o/--out file] [--fields list] [--csv] [--jsonArray]");
        System.err.println(
                "Usage: " + RDBExport.class.getName() + " --from-db2-dump file [--lobdir lobdir] [-o/--out file] [--fields list] [--csv] [--jsonArray]");
        System.err.println("Usage: " + RDBExport.class.getName() + " --version");
        System.err.println("Usage: " + RDBExport.class.getName() + " --help");
    }

    private static void printHelp() {
        System.err.println("Export Apache OAK RDB data to JSON files");
        System.err.println("");
        System.err.println("Generic options:");
        System.err.println("  --help                             produce this help message");
        System.err.println("  --version                          show version information");
        System.err.println("");
        System.err.println("JDBC options:");
        System.err.println("  -j/--jdbc-url JDBC-URL             JDBC URL of database to connect to");
        System.err.println("  -u/--username username             database username");
        System.err.println("  -p/--password password             database password");
        System.err.println("  -c/--collection table              table name (defaults to 'nodes')");
        System.err.println("  -q/--query query                   SQL where clause (minus 'where')");
        System.err.println("");
        System.err.println("Dump file options:");
        System.err.println("  --from-db2-dump file               name of DB2 DEL export file");
        System.err.println("  --lobdir dir                       name of DB2 DEL export file LOB directory");
        System.err.println("                                     (defaults to ./lobdir under the dump file)");
        System.err.println("");
        System.err.println("Output options:");
        System.err.println("  -o/--out file                      Output to name file (instead of stdout)");
        System.err.println("  --jsonArray                        Output a JSON array (instead of one");
        System.err.println("                                     JSON doc per line)");
        System.err.println("  --csv                              Output in CSV format (requires --fields");
        System.err.println("  --fields names                     field names (comma separated); required");
        System.err.println("                                     for CSV output");
    }
}
