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
import java.util.Map;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * AbstractQueryTest...
 */
public abstract class AbstractQueryTest extends AbstractOakTest implements
        IndexConstants {

    protected static final String TEST_INDEX_NAME = "test-index";
    protected static final String TEST_INDEX_HOME = DEFAULT_INDEX_HOME;
    protected static final String INDEX_DEFINITION_NODE_TYPE = "nam:oak:queryIndexDefinition";

    protected SessionQueryEngine qe;
    protected ContentSession session;
    protected Root root;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        session = createAdminSession();
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
        createTestIndexNode();
    }

    /**
     * Override this method to add your default index definition
     * 
     * {@link #createTestIndexNode(Tree, String)} for a helper method
     */
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        createTestIndexNode(index, "unknown");
        root.commit();
    }

    protected static Tree createTestIndexNode(Tree index, String type)
            throws Exception {
        Tree indexDef = index;
        for (String p : PathUtils.elements(TEST_INDEX_HOME)) {
            if (indexDef.hasChild(p)) {
                indexDef = indexDef.getChild(p);
            } else {
                indexDef = indexDef.addChild(p);
            }
        }
        indexDef = indexDef.addChild(INDEX_DEFINITIONS_NAME).addChild(
                TEST_INDEX_NAME);
        indexDef.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITION_NODE_TYPE);
        indexDef.setProperty(TYPE_PROPERTY_NAME, type);
        indexDef.setProperty(REINDEX_PROPERTY_NAME, true);
        return indexDef;
    }

    protected Result executeQuery(String statement, String language,
            Map<String, PropertyValue> sv) throws ParseException {
        return qe.executeQuery(statement, language, Long.MAX_VALUE, 0, sv,
                session.getLatestRoot(), null);
    }

    protected SecurityProvider getSecurityProvider() {
        return new OpenSecurityProvider();
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
    public void xpath() throws Exception {
        test("xpath.txt");
    }

    @Test
    @Ignore("OAK-336")
    public void sql2_measure() throws Exception {
        test("sql2_measure.txt");
    }

    @Test
    public void bindVariableTest() throws Exception {
        JsopUtil.apply(
                root,
                "/ + \"test\": { \"hello\": {\"id\": \"1\"}, \"world\": {\"id\": \"2\"}}");
        root.commit();

        Map<String, PropertyValue> sv = new HashMap<String, PropertyValue>();
        sv.put("id", PropertyValues.newString("1"));
        Iterator<? extends ResultRow> result;
        result = executeQuery("select * from [nt:base] where id = $id",
                QueryEngineImpl.SQL2, sv).getRows().iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/hello", result.next().getPath());

        sv.put("id", PropertyValues.newString("2"));
        result = executeQuery("select * from [nt:base] where id = $id",
                QueryEngineImpl.SQL2, sv).getRows().iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/world", result.next().getPath());
    }

    protected void test(String file) throws Exception {
        InputStream in = AbstractQueryTest.class.getResourceAsStream(file);
        LineNumberReader r = new LineNumberReader(new InputStreamReader(in));
        PrintWriter w = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream("target/" + getClass().getName() + "_"
                        + file)));
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
                } else if (line.startsWith("select")
                        || line.startsWith("explain")
                        || line.startsWith("measure")
                        || line.startsWith("sql1") || line.startsWith("xpath")) {
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
                    JsopUtil.apply(root, line);
                    root.commit();
                }
                w.flush();
            }
        } finally {
            w.close();
            r.close();
        }
        if (errors) {
            throw new Exception("Results in target/" + file
                    + " don't match expected "
                    + "results in src/test/resources/" + file
                    + "; compare the files for details");
        }
    }

    protected List<String> executeQuery(String query, String language) {
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

    protected static String readRow(ResultRow row) {
        StringBuilder buff = new StringBuilder();
        PropertyValue[] values = row.getValues();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            PropertyValue v = values[i];
            buff.append(v == null ? "null" : v.getValue(Type.STRING));
        }
        return buff.toString();
    }

    /**
     * Check whether the test is running in debug mode.
     * 
     * @return true if debug most is (most likely) enabled
     */
    protected static boolean isDebugModeEnabled() {
        return java.lang.management.ManagementFactory.getRuntimeMXBean()
                .getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;
    }

}