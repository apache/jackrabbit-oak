/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class ExcerptTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtils.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Before
    public void setup() throws Exception { //named so that it gets called after super.before :-/
        Tree rootTree = root.getTree("/");

        Tree def = rootTree.addChild(INDEX_DEFINITIONS_NAME).addChild("testExcerpt");
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        def.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());

        Tree properties = def.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE);

        Tree notIndexedProp = properties.addChild("baz");
        notIndexedProp.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        Tree relativeProp = properties.addChild("relative-baz");
        relativeProp.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        relativeProp.setProperty(FulltextIndexConstants.PROP_USE_IN_EXCERPT, true);
        relativeProp.setProperty(FulltextIndexConstants.PROP_NAME, "relative/baz");

        Tree allProps = properties.addChild("allProps");
        allProps.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        allProps.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        allProps.setProperty(FulltextIndexConstants.PROP_USE_IN_EXCERPT, true);
        allProps.setProperty(FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
        allProps.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);

        root.commit();
    }

    @Test
    public void getAllSelectedColumns() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "is fox ifoxing");
        contentRoot.setProperty("bar", "ifoxing fox");
        contentRoot.setProperty("baz", "fox ifoxing");
        root.commit();

        List<String> columns = new ArrayList<>(Arrays.asList("rep:excerpt", "rep:excerpt(.)", "rep:excerpt(foo)", "rep:excerpt(bar)"));
        String selectColumns = columns.stream().map(col -> "[" + col + "]").collect(Collectors.joining(","));
        String query = "SELECT " + selectColumns + " FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        assertEventually(() -> {
            try {
                Result result = executeQuery(query, SQL2, NO_BINDINGS);
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                assertTrue(resultIter.hasNext());
                ResultRow firstRow = resultIter.next();

                for (String col : columns) {
                    PropertyValue excerptValue = firstRow.getValue(col);
                    assertNotNull(col + " not evaluated", excerptValue);

                    String excerpt = excerptValue.getValue(STRING);
                    assertFalse(col + " didn't evaluate correctly - got '" + excerpt + "'",
                            excerpt.contains("i<strong>fox</foxing>ing"));
                }
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }

    @Test
    public void nodeExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "is fox ifoxing");
        contentRoot.setProperty("bar", "ifoxing fox");
        root.commit();

        String query = "SELECT [rep:excerpt],[rep:excerpt(.)] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        assertEventually(() -> {
            try {
                Result result = executeQuery(query, SQL2, NO_BINDINGS);
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                assertTrue(resultIter.hasNext());
                ResultRow firstRow = resultIter.next();

                PropertyValue nodeExcerpt = firstRow.getValue("rep:excerpt");
                assertNotNull("rep:excerpt not evaluated", nodeExcerpt);
                String excerpt1 = nodeExcerpt.getValue(STRING);
                assertTrue("rep:excerpt didn't evaluate correctly - got '" + excerpt1 + "'",
                        "is <strong>fox</strong> ifoxing".equals(excerpt1) || "ifoxing <strong>fox</strong>".equals(excerpt1));

                nodeExcerpt = firstRow.getValue("rep:excerpt(.)");
                assertNotNull("rep:excerpt(.) not evaluated", nodeExcerpt);
                String excerpt2 = nodeExcerpt.getValue(STRING);
                assertEquals("excerpt extracted via rep:excerpt not same as rep:excerpt(.)", excerpt1, excerpt2);
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }

    @Test
    public void nonIndexedRequestedPropExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "fox");
        contentRoot.setProperty("baz", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt(baz)] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        assertEventually(() -> {
            try {
                Result result = executeQuery(query, SQL2, NO_BINDINGS);
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                assertTrue(resultIter.hasNext());
                ResultRow firstRow = resultIter.next();

                PropertyValue nodeExcerpt = firstRow.getValue("rep:excerpt(baz)");
                assertNull("rep:excerpt(baz) if requested explicitly must be indexed to be evaluated", nodeExcerpt);
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }

    @Test
    public void propExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt(foo)] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        assertEventually(() -> {
            try {
                Result result = executeQuery(query, SQL2, NO_BINDINGS);
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                assertTrue(resultIter.hasNext());
                ResultRow firstRow = resultIter.next();

                PropertyValue nodeExcerpt = firstRow.getValue("rep:excerpt(foo)");
                assertNotNull("rep:excerpt(foo) not evaluated", nodeExcerpt);
                String excerpt = nodeExcerpt.getValue(STRING);
                assertEquals("rep:excerpt(foo) didn't evaluate correctly - got '" + excerpt + "'", 
                        "is <strong>fox</strong> ifoxing", excerpt);
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }

    @Test
    public void indexedNonRequestedPropExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        assertEventually(() -> {
            try {
                Result result = executeQuery(query, SQL2, NO_BINDINGS);
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                assertTrue(resultIter.hasNext());
                ResultRow firstRow = resultIter.next();

                PropertyValue nodeExcerpt = firstRow.getValue("rep:excerpt(foo)");
                assertNotNull("rep:excerpt(foo) not evaluated", nodeExcerpt);
                String excerpt = nodeExcerpt.getValue(STRING);
                assertTrue("rep:excerpt(foo) didn't evaluate correctly - got '" + excerpt + "'",
                        excerpt.contains("<strong>fox</strong>"));

                assertFalse("rep:excerpt(foo) highlighting inside words - got '" + excerpt + "'",
                        excerpt.contains("i<strong>fox</strong>ing"));
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }

    @Test
    public void nonIndexedNonRequestedPropExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.setProperty("foo", "fox");
        contentRoot.setProperty("baz", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        assertEventually(() -> {
            try {
                Result result = executeQuery(query, SQL2, NO_BINDINGS);
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                assertTrue(resultIter.hasNext());
                ResultRow firstRow = resultIter.next();

                PropertyValue nodeExcerpt = firstRow.getValue("rep:excerpt(baz)");
                assertNotNull("rep:excerpt(baz) not evaluated", nodeExcerpt);
                String excerpt = nodeExcerpt.getValue(STRING);
                assertTrue("rep:excerpt(foo) didn't evaluate correctly - got '" + excerpt + "'",
                        excerpt.contains("<strong>fox</strong>"));

                assertFalse("rep:excerpt(baz) highlighting inside words - got '" + excerpt + "'",
                        excerpt.contains("i<strong>fox</strong>ing"));
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }

    //We 'intentionally' are indexing node names only on root state as we don't support indexing relative or
    //regex for node name indexing. Comment taken from FulltextDocumentMaker #148. Test skipped
    @Test
    @Ignore
    public void relativePropExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");
        contentRoot.addChild("relative").setProperty("baz", "is fox ifoxing");
        root.commit();

        String query = "SELECT [rep:excerpt(relative/baz)] FROM [nt:base] WHERE CONTAINS([relative/baz], 'fox')";
        assertEventually(() -> {
            try {
                Result result = executeQuery(query, SQL2, NO_BINDINGS);
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                assertTrue(resultIter.hasNext());
                ResultRow firstRow = resultIter.next();

                PropertyValue nodeExcerpt = firstRow.getValue("rep:excerpt(relative/baz)");
                assertNotNull("rep:excerpt(relative/baz) not evaluated", nodeExcerpt);
                String excerpt = nodeExcerpt.getValue(STRING);
                assertEquals("rep:excerpt(relative/baz) didn't evaluate correctly - got '" + excerpt + "'",
                        "is <strong>fox</strong> ifoxing", excerpt);
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }

    @Test
    public void binaryExcerpt() throws Exception {
        Tree contentRoot = root.getTree("/").addChild("testRoot");

        String binaryText = "is fox foxing as a fox cub";
        Blob blob = new ArrayBasedBlob(binaryText.getBytes());
        TestUtil.createFileNode(contentRoot, "binaryNode", blob, "text/plain");
        root.commit();

        String query = "SELECT [rep:excerpt] FROM [nt:base] WHERE CONTAINS(*, 'fox')";
        assertEventually(() -> {
            try {
                Result result = executeQuery(query, SQL2, NO_BINDINGS);
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                assertTrue(resultIter.hasNext());
                ResultRow firstRow = resultIter.next();

                PropertyValue nodeExcerpt = firstRow.getValue("rep:excerpt");
                assertNotNull("rep:excerpt not evaluated", nodeExcerpt);
                String excerpt = nodeExcerpt.getValue(STRING);
                String expected = binaryText.replaceAll(" fox ", " <strong>fox</strong> ");
                assertTrue("rep:excerpt didn't evaluate correctly - got '" + excerpt + "'",
                        excerpt.contains(expected));
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }
}
