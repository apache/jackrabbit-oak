/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.index.IndexWrapper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.plugins.index.old.Indexer;
import org.apache.jackrabbit.oak.plugins.index.old.PropertyIndexer;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the query feature.
 */
public class QueryTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {

        // the property and prefix index currently require the index wrapper
        IndexWrapper mk = new IndexWrapper(new MicroKernelImpl());
        Indexer indexer = mk.getIndexer();

        // MicroKernel mk = new MicroKernelImpl();
        // Indexer indexer = new Indexer(mk);

        PropertyIndexer pi = new PropertyIndexer(indexer);
        QueryIndexProvider qip = new CompositeQueryIndexProvider(pi);
        CompositeHook hook = new CompositeHook(pi);
        createDefaultKernelTracker().available(mk);
        return new Oak(mk).with(qip).with(hook).createContentRepository();
    }

    @Test
    public void sql1() throws Exception {
        test("sql1.txt");
    }

    @Test
    public void sql2() throws Exception {
        test("sql2.txt");
    }

    @Test
    public void sql2Explain() throws Exception {
        test("sql2_explain.txt");
    }

    @Test
    public void sql2_measure() throws Exception {
        test("sql2_measure.txt");
    }

    @Test
    public void xpath() throws Exception {
        test("xpath.txt");
    }

    @Test
    public void bindVariableTest() throws Exception {
        JsopUtil.apply(
                root,
                "/ + \"test\": { \"hello\": {\"id\": \"1\"}, \"world\": {\"id\": \"2\"}}",
                vf);
        root.commit();

        HashMap<String, CoreValue> sv = new HashMap<String, CoreValue>();
        sv.put("id", vf.createValue("1"));
        Iterator<? extends ResultRow> result;
        result = executeQuery("select * from [nt:base] where id = $id",
                QueryEngineImpl.SQL2, sv).getRows().iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/hello", result.next().getPath());

        sv.put("id", vf.createValue("2"));
        result = executeQuery("select * from [nt:base] where id = $id",
                QueryEngineImpl.SQL2, sv).getRows().iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/world", result.next().getPath());

        result = executeQuery("explain select * from [nt:base] where id = 1 order by id",
                QueryEngineImpl.SQL2, null).getRows().iterator();
        assertTrue(result.hasNext());
        assertEquals("[nt:base] as [nt:base] " +
                "/* traverse \"//*\" where [nt:base].[id] = cast('1' as long) */",
                result.next().getValue("plan").getString());

    }

    private void test(String file) throws Exception {
        InputStream in = getClass().getResourceAsStream(file);
        LineNumberReader r = new LineNumberReader(new InputStreamReader(in));
        PrintWriter w = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream("target/" + file)));
        HashSet<String> knownQueries = new HashSet<String>();
        boolean errors = false;
        try {
            while (true) {
                String line = r.readLine();
                if (line == null) {
                    break;
                }
                line = line.trim();
                if (line.startsWith("#") || line.length() == 0) {
                    w.println(line);
                } else if (line.startsWith("xpath2sql")) {
                    line = line.substring("xpath2sql".length()).trim();
                    w.println("xpath2sql " + line);
                    XPathToSQL2Converter c = new XPathToSQL2Converter();
                    String got;
                    try {
                        got = c.convert(line);
                        executeQuery(got, QueryEngineImpl.SQL2, null);
                    } catch (ParseException e) {
                        got = "invalid: " + e.getMessage().replace('\n', ' ');
                    } catch (Exception e) {
                        // e.printStackTrace();
                        got = "error: " + e.toString().replace('\n', ' ');
                    }
                    if (!knownQueries.add(line)) {
                        got = "duplicate xpath2sql query";
                    }
                    line = r.readLine().trim();
                    w.println(got);
                    if (!line.equals(got)) {
                        errors = true;
                    }
                } else if (line.startsWith("select") ||
                        line.startsWith("explain") ||
                        line.startsWith("measure") ||
                        line.startsWith("sql1") ||
                        line.startsWith("xpath")) {
                    w.println(line);
                    String language = QueryEngineImpl.SQL2;
                    if (line.startsWith("sql1 ")) {
                        language = QueryEngineImpl.SQL;
                        line = line.substring("sql1 ".length());
                    } else if (line.startsWith("xpath ")) {
                        language = QueryEngineImpl.XPATH;
                        line = line.substring("xpath ".length());
                    }
                    boolean readEnd = true;
                    for (String resultLine : executeQuery(line, language)) {
                        w.println(resultLine);
                        if (readEnd) {
                            line = r.readLine();
                            if (line == null) {
                                errors = true;
                                readEnd = false;
                            } else {
                                line = line.trim();
                                if (line.length() == 0) {
                                    errors = true;
                                    readEnd = false;
                                } else {
                                    if (!line.equals(resultLine)) {
                                        errors = true;
                                    }
                                }
                            }
                        }
                    }
                    w.println("");
                    if (readEnd) {
                        while (true) {
                            line = r.readLine();
                            if (line == null) {
                                break;
                            }
                            line = line.trim();
                            if (line.length() == 0) {
                                break;
                            }
                            errors = true;
                        }
                    }
                } else if (line.startsWith("commit")) {
                    w.println(line);
                    line = line.substring("commit".length()).trim();
                    JsopUtil.apply(root, line, vf);
                    root.commit();
                }
                w.flush();
            }
        } finally {
            w.close();
            r.close();
        }
        if (errors) {
            throw new Exception("Results in target/" + file + " don't match expected " +
                    "results in src/test/resources/" + file + "; compare the files for details");
        }
    }

    private List<String> executeQuery(String query, String language) {
        long time = System.currentTimeMillis();
        List<String> lines = new ArrayList<String>();
        try {
            Result result = executeQuery(query, language, null);
            for (ResultRow row : result.getRows()) {
                lines.add(readRow(row));
            }
            if (!query.contains("order by")) {
                Collections.sort(lines);
            }
        } catch (ParseException e) {
            lines.add(e.toString());
        } catch (IllegalArgumentException e) {
            lines.add(e.toString());
        }
        time = System.currentTimeMillis() - time;
        if (time > 3000 && !isDebugModeEnabled()) {
            fail("Query took too long: " + query + " took " + time + " ms");
        }
        return lines;
    }

    private static String readRow(ResultRow row) {
        StringBuilder buff = new StringBuilder();
        CoreValue[] values = row.getValues();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            CoreValue v = values[i];
            buff.append(v == null ? "null" : v.getString());
        }
        return buff.toString();
    }

    /**
     * Check whether the test is running in debug mode.
     * 
     * @return true if debug most is (most likely) enabled
     */
    private static boolean isDebugModeEnabled() {
        return java.lang.management.ManagementFactory.getRuntimeMXBean().
                getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;
    }

}
