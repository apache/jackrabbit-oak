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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getModuleVersion;

/**
 * Utility for dumping contents from {@link RDBDocumentStore}'s tables.
 */
public class RDBExport {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private enum Format {
        JSON, JSONARRAY, CSV
    };

    private static final RDBJSONSupport JSON = new RDBJSONSupport(false);

    private static final Set<String> EXCLUDE_COLUMNS = new HashSet<String>();
    static {
        EXCLUDE_COLUMNS.add(Document.ID);
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

        String url = null, user = null, pw = null, table = "nodes", query = null, dumpfile = null, lobdir = null;
        List<String> fieldList = Collections.emptyList();
        Format format = Format.JSON;
        PrintStream out = System.out;
        RDBDocumentSerializer ser = new RDBDocumentSerializer(new MemoryDocumentStore());
        String columns = null;

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
                } else if ("--columns".equals(param)) {
                    columns = args[++i];
                } else if ("--fields".equals(param)) {
                    String fields = args[++i];
                    fieldList = Arrays.asList(fields.split(","));
                } else if ("--version".equals(param)) {
                    System.out.println(RDBExport.class.getName() + " version " + getModuleVersion());
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
            columns = (columns == null) ? "id, modified, hasbinary, deletedonce, cmodcount, modcount, dsize, data, bdata" : columns;
            List<String> columnList = Arrays.asList(columns.toLowerCase(Locale.ENGLISH).replace(" ", "").split(","));
            dumpFile(dumpfile, lobdir, format, out, fieldList, columnList, ser);
        } else {
            if (columns != null) {
                System.err.println(RDBExport.class.getName() + ": column names ignored when using JDBC");
            }
            dumpJDBC(url, user, pw, table, query, format, out, fieldList, ser);
        }

        out.flush();
        out.close();
    }

    private static void dumpFile(String filename, String lobdir, Format format, PrintStream out, List<String> fieldNames,
            List<String> columnNames, RDBDocumentSerializer ser) throws IOException {
        File f = new File(filename);
        File lobDirectory = lobdir == null ? new File(f.getParentFile(), "lobdir") : new File(lobdir);

        int iId = columnNames.indexOf("id");
        int iModified = columnNames.indexOf("modified");
        int iHasBinary = columnNames.indexOf("hasbinary");
        int iDeletedOnce = columnNames.indexOf("deletedonce");
        int iModCount = columnNames.indexOf("modcount");
        int iCModCount = columnNames.indexOf("cmodcount");
        int iData = columnNames.indexOf("data");
        int iBData = columnNames.indexOf("bdata");

        if (iId < 0 || iModified < 0 || iHasBinary < 0 || iDeletedOnce < 0 || iModCount < 0 || iCModCount < 0 || iData < 0
                || iBData < 0) {
            throw new IOException("required columns: id, modified, hasbinary, deletedonce, modcount, cmodcount, data, bdata");
        }

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
            List<String> fields = parseDel(line);
            String id = fields.get(iId);
            String smodified = fields.get(iModified);
            String shasbinary = fields.get(iHasBinary);
            String sdeletedonce = fields.get(iDeletedOnce);
            String smodcount = fields.get(iModCount);
            String scmodcount = fields.get(iCModCount);
            String sdata = fields.get(iData);
            String sbdata = fields.get(iBData);

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
                RDBRow row = new RDBRow(id, "1".equals(shasbinary) ? 1L : 0L, "1".equals(sdeletedonce),
                        smodified.length() == 0 ? 0 : Long.parseLong(smodified), Long.parseLong(smodcount),
                        Long.parseLong(scmodcount), -1L, -1L, -1L, sdata, bytes);
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

    protected static List<String> parseDel(String line) {
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
            Long hasBinary = readLongOrNullFromResultSet(rs, "HASBINARY");
            Boolean deletedOnce = readBooleanOrNullFromResultSet(rs, "DELETEDONCE");
            String data = rs.getString("DATA");
            byte[] bdata = rs.getBytes("BDATA");

            RDBRow row = new RDBRow(id, hasBinary, deletedOnce, modified, modcount, cmodcount, -1L, -1L, -1L, data, bdata);
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

    @CheckForNull
    private static Boolean readBooleanOrNullFromResultSet(ResultSet res, String field) throws SQLException {
        long v = res.getLong(field);
        return res.wasNull() ? null : Boolean.valueOf(v != 0);
    }

    @CheckForNull
    private static Long readLongOrNullFromResultSet(ResultSet res, String field) throws SQLException {
        long v = res.getLong(field);
        return res.wasNull() ? null : Long.valueOf(v);
    }

    @Nonnull
    private static StringBuilder dumpRow(RDBDocumentSerializer ser, String id, RDBRow row) {
        NodeDocument doc = ser.fromRow(Collection.NODES, row);
        String docjson = ser.asString(doc, EXCLUDE_COLUMNS);
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
            Map<String, Object> doc = (Map<String, Object>) JSON.parse(fulljson.toString());
            StringBuilder buf = new StringBuilder();
            buf.append('{');
            String delim = "";
            for (String field : fieldNames) {
                buf.append(delim);
                delim = ",";
                String[] fn = field.split("\\.");
                if (doc.containsKey(fn[0])) {
                    Object o = doc.get(fn[0]);
                    RDBJSONSupport.appendJsonMember(buf, fn[0], o);
                }
            }
            buf.append('}');
            return buf;
        }
    }

    @Nonnull
    private static StringBuilder asCSV(List<String> csvFieldNames, StringBuilder fulljson) {
        Map<String, Object> doc = (Map<String, Object>) JSON.parse(fulljson.toString());
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
        System.err.println("  --columns column-names             column names (comma separated)");
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
