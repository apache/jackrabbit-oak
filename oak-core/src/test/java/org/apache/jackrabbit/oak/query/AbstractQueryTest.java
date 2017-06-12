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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.json.TypeCodes;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.query.QueryEngineImpl.QuerySelectionMode;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.junit.Before;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * AbstractQueryTest...
 */
public abstract class AbstractQueryTest {

    protected static final String TEST_INDEX_NAME = "test-index";
    protected static final String SQL2 = QueryEngineImpl.SQL2;
    protected static final String XPATH = QueryEngineImpl.XPATH;

    protected QueryEngine qe;
    protected ContentSession session;
    protected Root root;

    @Before
    public void before() throws Exception {
        session = createRepository().login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
        createTestIndexNode();
    }

    protected abstract ContentRepository createRepository();

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
        Tree indexDef = index.addChild(INDEX_DEFINITIONS_NAME).addChild(
                TEST_INDEX_NAME);
        indexDef.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        indexDef.setProperty(TYPE_PROPERTY_NAME, type);
        indexDef.setProperty(REINDEX_PROPERTY_NAME, true);
        return indexDef;
    }

    protected Result executeQuery(String statement, String language,
            Map<String, PropertyValue> sv) throws ParseException {
        return qe.executeQuery(statement, language, sv, NO_MAPPINGS);
    }

    protected void test(String file) throws Exception {

        String className = getClass().getName();
        String shortClassName = className.replaceAll("org.apache.jackrabbit.oak.plugins.index.", "oajopi.");

        // OAK-3252 getting the input/output paths for better error reporting. Still using the
        // stream for input as other projects uses dependencies on sql2.txt of oak-core and it fails
        // resolving the whole path on disk
        File input = new File(AbstractQueryTest.class.getResource(file).getPath());
        File output = new File("target/" + shortClassName + "_" + file);

        InputStream in = AbstractQueryTest.class.getResourceAsStream(file);
        ContinueLineReader r = new ContinueLineReader(new LineNumberReader(new InputStreamReader(in)));
        PrintWriter w = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream(output)));
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
                    } catch (ParseException e) {
                        got = "invalid: " + e.getMessage().replace('\n', ' ');
                    } catch (Exception e) {
                        // e.printStackTrace();
                        got = "error: " + e.toString().replace('\n', ' ');
                    }
                    String formatted = formatSQL(got);
                    if (!knownQueries.add(line)) {
                        got = "duplicate xpath2sql query";
                    }
                    line = r.readLine().trim();
                    w.println(formatted);
                    if (!line.equals(got) && !line.equals(formatted)) {
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
                    apply(root, line);
                    root.commit();
                }
                w.flush();
            }
        } finally {
            w.close();
            r.close();
        }
        if (errors) {
            RandomAccessFile f = new RandomAccessFile(output, "r");
            byte[] data = new byte[(int) f.length()];
            f.readFully(data);
            f.close();
            throw new Exception("Results in " + output.getPath()
                    + " don't match expected "
                    + "results in " + input.getPath()
                    + "; compare the files for details; got=\n" +
                    new String(data, "UTF-8"));
        }
    }

    protected List<String> executeQuery(String query, String language) {
        boolean pathsOnly = false;
        if (language.equals(QueryEngineImpl.XPATH)) {
            pathsOnly = true;
        }
        return executeQuery(query, language, pathsOnly);
    }

    protected List<String> executeQuery(String query, String language, boolean pathsOnly) {
        return executeQuery(query, language, pathsOnly, false);
    }

    protected List<String> executeQuery(String query, String language, boolean pathsOnly, boolean skipSort) {
        long time = System.currentTimeMillis();
        List<String> lines = new ArrayList<String>();
        try {
            Result result = executeQuery(query, language, NO_BINDINGS);
            for (ResultRow row : result.getRows()) {
                String r = readRow(row, pathsOnly);
                if (query.startsWith("explain ")) {
                    r = formatPlan(r);
                }
                lines.add(r);
            }
            if (!query.contains("order by") && !skipSort) {
                Collections.sort(lines);
            }
        } catch (ParseException e) {
            lines.add(e.toString());
        } catch (IllegalArgumentException e) {
            lines.add(e.toString());
        }
        time = System.currentTimeMillis() - time;
        if (time > 5 * 60 * 1000 && !isDebugModeEnabled()) {
            // more than 5 minutes
            fail("Query took too long: " + query + " took " + time + " ms");
        }
        return lines;
    }

    protected List<String> assertQuery(String sql, List<String> expected) {
        return assertQuery(sql, SQL2, expected);
    }

    protected void assertResultSize(String query, String language, long expected) {
        long time = System.currentTimeMillis();
        try {
            Result result = executeQuery(query, language, NO_BINDINGS);
            // currently needed to iterate to really execute the query
            result.getRows().iterator().hasNext();
            long got = result.getSize(SizePrecision.APPROXIMATION, 0);
            assertEquals(expected, got);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        time = System.currentTimeMillis() - time;
        if (time > 10000 && !isDebugModeEnabled()) {
            fail("Query took too long: " + query + " took " + time + " ms");
        }
    }

    protected List<String> assertQuery(String sql, String language,
            List<String> expected) {
        return assertQuery(sql, language, expected, false);
    }

    protected List<String> assertQuery(String sql, String language,
                                       List<String> expected, boolean skipSort) {
        List<String> paths = executeQuery(sql, language, true, skipSort);
        assertResult(expected, paths);
        return paths;

    }

    protected static void assertResult(@Nonnull List<String> expected, @Nonnull List<String> actual) {
        for (String p : checkNotNull(expected)) {
            assertTrue("Expected path " + p + " not found, got " + actual, checkNotNull(actual)
                .contains(p));
        }
        assertEquals("Result set size is different: " + actual, expected.size(),
                actual.size());
    }

    protected void setTraversalEnabled(boolean traversalEnabled) {
        ((QueryEngineImpl) qe).setTraversalEnabled(traversalEnabled);
    }

    protected void setQuerySelectionMode(@Nonnull QuerySelectionMode querySelectionMode) {
        ((QueryEngineImpl) qe).setQuerySelectionMode(checkNotNull(querySelectionMode));
    }

    protected static String readRow(ResultRow row, boolean pathOnly) {
        if (pathOnly) {
            return row.getValue(QueryConstants.JCR_PATH).getValue(Type.STRING);
        }
        StringBuilder buff = new StringBuilder();
        PropertyValue[] values = row.getValues();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            PropertyValue v = values[i];
            if (v == null) {
                buff.append("null");
            } else if (v.isArray()) {
                buff.append('[');
                for (int j = 0; j < v.count(); j++) {
                    buff.append(v.getValue(Type.STRING, j));
                    if (j > 0) {
                        buff.append(", ");
                    }
                }
                buff.append(']');
            } else {
                buff.append(v.getValue(Type.STRING));
            }
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

    /**
     * Applies the commit string to a given Root instance
     *
     * The commit string represents a sequence of operations, jsonp style:
     *
     * <p>
     * / + "test": { "a": { "id": "ref:123" }, "b": { "id" : "str:123" }}
     * <p>
     * or
     * <p>
     * "/ - "test"
     * </p>
     *
     * @param root
     * @param commit the commit string
     * @throws UnsupportedOperationException if the operation is not supported
     */
    private static void apply(Root root, String commit)
            throws UnsupportedOperationException {
        int index = commit.indexOf(' ');
        String path = commit.substring(0, index).trim();
        Tree c = root.getTree(path);
        if (!c.exists()) {
            // TODO create intermediary?
            throw new UnsupportedOperationException("Non existing path " + path);
        }
        commit = commit.substring(index);
        JsopTokenizer tokenizer = new JsopTokenizer(commit);
        if (tokenizer.matches('-')) {
            removeTree(c, tokenizer);
        } else if (tokenizer.matches('+')) {
            addTree(c, tokenizer);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported " + (char) tokenizer.read()
                    + ". This should be either '+' or '-'.");
        }
    }

    private static void removeTree(Tree t, JsopTokenizer tokenizer) {
        String path = tokenizer.readString();
        for (String p : PathUtils.elements(path)) {
            if (!t.hasChild(p)) {
                return;
            }
            t = t.getChild(p);
        }
        t.remove();
    }

    private static void addTree(Tree t, JsopTokenizer tokenizer) {
        do {
            String key = tokenizer.readString();
            tokenizer.read(':');
            if (tokenizer.matches('{')) {
                Tree c = t.addChild(key);
                if (!tokenizer.matches('}')) {
                    addTree(c, tokenizer);
                    tokenizer.read('}');
                }
            } else if (tokenizer.matches('[')) {
                t.setProperty(readArrayProperty(key, tokenizer));
            } else {
                t.setProperty(readProperty(key, tokenizer));
            }
        } while (tokenizer.matches(','));
    }

    /**
     * Read a {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    private static PropertyState readProperty(String name, JsopReader reader) {
        if (reader.matches(JsopReader.NUMBER)) {
            String number = reader.getToken();
            return createProperty(name, number, PropertyType.LONG);
        } else if (reader.matches(JsopReader.TRUE)) {
            return BooleanPropertyState.booleanProperty(name, true);
        } else if (reader.matches(JsopReader.FALSE)) {
            return BooleanPropertyState.booleanProperty(name, false);
        } else if (reader.matches(JsopReader.STRING)) {
            String jsonString = reader.getToken();
            int split = TypeCodes.split(jsonString);
            if (split != -1) {
                int type = TypeCodes.decodeType(split, jsonString);
                String value = TypeCodes.decodeName(split, jsonString);
                if (type == PropertyType.BINARY) {
                    throw new UnsupportedOperationException();
                } else {
                    return createProperty(name, value, type);
                }
            } else {
                return StringPropertyState.stringProperty(name, jsonString);
            }
        } else {
            throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
        }
    }

    /**
     * Read a multi valued {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    private static PropertyState readArrayProperty(String name, JsopReader reader) {
        int type = PropertyType.STRING;
        List<Object> values = Lists.newArrayList();
        while (!reader.matches(']')) {
            if (reader.matches(JsopReader.NUMBER)) {
                String number = reader.getToken();
                type = PropertyType.LONG;
                values.add(Conversions.convert(number).toLong());
            } else if (reader.matches(JsopReader.TRUE)) {
                type = PropertyType.BOOLEAN;
                values.add(true);
            } else if (reader.matches(JsopReader.FALSE)) {
                type = PropertyType.BOOLEAN;
                values.add(false);
            } else if (reader.matches(JsopReader.STRING)) {
                String jsonString = reader.getToken();
                int split = TypeCodes.split(jsonString);
                if (split != -1) {
                    type = TypeCodes.decodeType(split, jsonString);
                    String value = TypeCodes.decodeName(split, jsonString);
                    if (type == PropertyType.BINARY) {
                        throw new UnsupportedOperationException();
                    } else if (type == PropertyType.DOUBLE) {
                        values.add(Conversions.convert(value).toDouble());
                    } else if (type == PropertyType.DECIMAL) {
                        values.add(Conversions.convert(value).toDecimal());
                    } else {
                        values.add(value);
                    }
                } else {
                    type = PropertyType.STRING;
                    values.add(jsonString);
                }
            } else {
                throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
            }
            reader.matches(',');
        }
        return createProperty(name, values, Type.fromTag(type, true));
    }
    
    static String formatSQL(String sql) {
        // the "(?s)" is enabling the "dot all" flag
        // keep /* xpath ... */ to ensure the xpath comment
        // is really there (and at the right position)
        sql = sql.replaceAll("(?s) /\\* .* \\*/", "\n  /* xpath ... */").trim();
        sql = sql.replaceAll(" union select ", "\n  union select ");
        sql = sql.replaceAll(" from ", "\n  from ");
        sql = sql.replaceAll(" where ", "\n  where ");
        sql = sql.replaceAll(" inner join ", "\n  inner join ");
        sql = sql.replaceAll(" on ", "\n  on ");
        sql = sql.replaceAll(" and ", "\n  and ");
        sql = sql.replaceAll(" or ", "\n  or ");
        sql = sql.replaceAll(" order by ", "\n  order by ");
        return sql;
    }
    
    static String formatPlan(String plan) {
        plan = plan.replaceAll(" where ", "\n  where ");
        plan = plan.replaceAll(" inner join ", "\n  inner join ");
        plan = plan.replaceAll(" on ", "\n  on ");
        plan = plan.replaceAll(" and ", "\n  and ");
        return plan;
    }
    
    /**
     * A line reader that supports multi-line statements, where lines that start
     * with a space belong to the previous line.
     */
    class ContinueLineReader {
        
        private final LineNumberReader reader;
        
        ContinueLineReader(LineNumberReader reader) {
            this.reader = reader;
        }
        
        public void close() throws IOException {
            reader.close();
        }
        
        public String readLine() throws IOException {
            String line = reader.readLine();
            if (line == null || line.trim().length() == 0) {
                return line;
            }
            while (true) {
                reader.mark(4096);
                String next = reader.readLine();
                if (next == null || !next.startsWith(" ")) {
                    reader.reset();
                    return line;
                }
                line = (line.trim() + "\n" + next).trim();
            }
        }
    }

}
