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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.jackrabbit.mk.MicroKernelFactory;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the query feature.
 */
public class QueryTest {

    MicroKernel mk;
    String head;
    QueryEngineImpl qe;

    @Before
    public void setUp() {
        mk = MicroKernelFactory.getInstance("simple:/target/temp;clear");
        head = mk.getHeadRevision();
        qe = new QueryEngineImpl(mk);
    }

    @After
    public void tearDown() {
        mk.dispose();
    }

    @Test
    public void script() throws Exception {
        test("queryTest.txt");
    }

    @Test
    public void xpath() throws Exception {
        test("queryXpathTest.txt");
    }

    @Test
    public void bindVariableTest() throws Exception {
        head = mk.commit("/", "+ \"test\": { \"hello\": {\"id\": \"1\"}, \"world\": {\"id\": \"2\"}}", null, null);
        HashMap<String, CoreValue> sv = new HashMap<String, CoreValue>();
        CoreValueFactory vf = new CoreValueFactory();
        sv.put("id", vf.createValue("1"));
        Iterator<? extends ResultRow> result;
        result = qe.executeQuery("select * from [nt:base] where id = $id", QueryEngine.SQL2, sv).getRows();
        assertTrue(result.hasNext());
        assertEquals("/test/hello", result.next().getPath());

        sv.put("id", vf.createValue("2"));
        result = qe.executeQuery("select * from [nt:base] where id = $id", QueryEngine.SQL2, sv).getRows();
        assertTrue(result.hasNext());
        assertEquals("/test/world", result.next().getPath());


        result = qe.executeQuery("explain select * from [nt:base] where id = 1 order by id", QueryEngine.SQL2, null).getRows();
        assertTrue(result.hasNext());
        assertEquals("nt:base AS nt:base /* traverse \"//*\" */", result.next().getValue("plan").getString());

    }

    private void test(String file) throws Exception {
        InputStream in = getClass().getResourceAsStream(file);
        LineNumberReader r = new LineNumberReader(new InputStreamReader(in));
        PrintWriter w = new PrintWriter(new OutputStreamWriter(new FileOutputStream("target/" + file)));
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
                } else if (line.startsWith("xpath")) {
                    line = line.substring("xpath".length()).trim();
                    w.println("xpath " + line);
                    XPathToSQL2Converter c = new XPathToSQL2Converter();
                    String got;
                    try {
                        got = c.convert(line);
                    } catch (ParseException e) {
                        got = "invalid: " + e.getMessage().replace('\n', ' ');
                    }
                    line = r.readLine().trim();
                    w.println(got);
                    if (!line.equals(got)) {
                        errors = true;
                    }
                } else if (line.startsWith("select") || line.startsWith("explain")) {
                    w.println(line);
                    Iterator<? extends ResultRow> result = qe.executeQuery(line, QueryEngine.SQL2, null).getRows();
                    boolean readEnd = true;
                    while (result.hasNext()) {
                        ResultRow row = result.next();
                        String resultLine = readRow(line, row);
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
                } else {
                    w.println(line);
                    mk.commit("/", line, mk.getHeadRevision(), "");
                }
            }
        } finally {
            w.close();
            r.close();
        }
        if (errors) {
            throw new Exception("Results in target/queryTest.txt don't match expected " +
                    "results in src/test/resources/quersTest.txt; compare the files for details");
        }
    }

    private String readRow(String query, ResultRow row) {
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

}
