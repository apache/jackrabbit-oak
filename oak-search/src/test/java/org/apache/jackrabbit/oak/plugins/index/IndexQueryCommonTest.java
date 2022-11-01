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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.SQL2Parser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.event.Level;

import java.text.ParseException;
import java.util.*;

import javax.jcr.query.Query;

import static java.util.Collections.singletonList;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_VALUE_REGEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the query engine using the default index implementation: the
 * IndexProvider
 */
public abstract class IndexQueryCommonTest extends AbstractQueryTest {

    private Tree indexDefn;
    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;
    private LogCustomizer logCustomizer;
    private final String nativeWarnLog = "Native queries are deprecated. Query:";

    @Before
    public void setupLogger(){
        logCustomizer = LogCustomizer.forLogger(SQL2Parser.class.getName()).enable(Level.WARN)
                        .contains(nativeWarnLog).create();
        logCustomizer.starting();
    }

    @After
    public void closeLogger(){
        logCustomizer.finished();
    }


    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        indexDefn.setProperty("tags", "x");

        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:base");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enablePropertyIndex(props, "c1/p", false);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);
        TestUtil.enablePropertyIndex(props, "a/name", false);
        TestUtil.enablePropertyIndex(props, "b/name", false);
        TestUtil.enableFunctionIndex(props, "length([name])");
        TestUtil.enableFunctionIndex(props, "lower([name])");
        TestUtil.enableFunctionIndex(props, "upper([name])");
        TestUtil.enableForFullText(props, "propa", false);
        TestUtil.enableForFullText(props, "propb", false);

        Tree dateProp = TestUtil.enableForOrdered(props, "propDate");
        dateProp.setProperty(FulltextIndexConstants.PROP_TYPE, "Date");

        // Note  - certain tests in this class like #sql2 test regex based like queries.
        // And since all the tests here use this common full text index - please be careful while adding any new properties.
        // For example - #sql2() tests with a query on length of name property.
        // Since this is a fulltext index with a regex property that indexes everything, those property names are also indexed.
        // So if we add any property with propName that has length equal to what that test expects - that will effectively break the #sql2() test (giving more results).
        // Ideally one would see the test failing while adding new properties - but there have been cases where this test was ignored due to a different reason
        // and adding a new property added more failure reasons.

        // So just be careful while changing the test collateral/setup here.

        root.commit();
    }

    // TODO : The below 3 tests - #sql1, #sq2 and #sql2FullText need refactoring.
    //  These are huge tests with multiple queries running and verification happening in the end by comparing against results in an expected test file.
    //  These could possibly be broken down into several smaller tests instead which would make debugging much easier.
    @Test
    public void sql1() throws Exception {
        test("sql1.txt");
    }

    @Test
    public void sql2() throws Exception {
        test("sql2.txt");
    }

    @Test
    public void sql2FullText() throws Exception {
        test("sql2-fulltext.txt");
    }

    @Test
    public void testValueRegex() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        Tree b = test.addChild("b");
        a.setProperty("name", "hello");
        b.setProperty("name", "hello pattern");
        root.commit();

        final String query = "select [jcr:path] from [nt:base] where isdescendantnode('/test') and contains(*, 'hello')";

        assertEventually(() -> {
            Iterator<String> result = executeQuery(query, Query.JCR_SQL2).iterator();
            List<String> paths = new ArrayList<>();
            result.forEachRemaining(paths::add);
            assertEquals(2, paths.size());
            assertEquals(paths.get(0), a.getPath());
            assertEquals(paths.get(1), b.getPath());
        });

        indexDefn.setProperty(PROP_VALUE_REGEX, "pat*");
        indexDefn.setProperty(REINDEX_PROPERTY_NAME, true);
        root.commit();

        assertEventually(() -> {
            Iterator<String> result = executeQuery(query, Query.JCR_SQL2).iterator();
            List<String> paths = new ArrayList<>();
            result.forEachRemaining(paths::add);
            assertEquals(1, paths.size());
            assertEquals(paths.get(0), b.getPath());
        });
    }

    @Test
    public void descendantTest() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        assertEventually(() -> {
            Iterator<String> result = executeQuery(
                    "select [jcr:path] from [nt:base] where isdescendantnode('/test')",
                    Query.JCR_SQL2).iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertEquals("/test/b", result.next());
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void descendantTestWithIndexTag() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        assertEventually(() -> {
            Iterator<String> result = executeQuery(
                    "select [jcr:path] from [nt:base] where isdescendantnode('/test') option (index tag x)",
                    Query.JCR_SQL2).iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertEquals("/test/b", result.next());
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void descendantTestWithIndexTagExplain() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        String query = "explain select [jcr:path] from [nt:base] where isdescendantnode('/test') option (index tag x)";
        assertEventually(getAssertionForExplain(query, Query.JCR_SQL2, getExplainValueForDescendantTestWithIndexTagExplain(), true));
    }

    // Check if this is a valid behaviour or not ?
    // This was discovered when we removed setTraversalEnabled(false); from the test setup.
    @Ignore("Index not picked even when using option tag if traversal cost is lower")
    @Test
    public void descendantTestWithIndexTagExplainWithNoData() {
        String query = "explain select [jcr:path] from [nt:base] where isdescendantnode('/test') option (index tag x)";
        assertEventually(getAssertionForExplain(query, Query.JCR_SQL2, getExplainValueForDescendantTestWithIndexTagExplain(), true));
    }

    @Test
    public void descendantTest2() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", asList("Hello", "World"), STRINGS);
        test.addChild("b").setProperty("name", "Hello");
        root.commit();

        assertEventually(() -> {
            Iterator<String> result = executeQuery(
                    "select [jcr:path] from [nt:base] where isdescendantnode('/test') and name='World'",
                    Query.JCR_SQL2).iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void isChildNodeTest() throws Exception {
        Tree tree = root.getTree("/");
        Tree parents = tree.addChild("parents");
        parents.addChild("p0").setProperty("id", "0");
        parents.addChild("p1").setProperty("id", "1");
        parents.addChild("p2").setProperty("id", "2");
        Tree children = tree.addChild("children");
        children.addChild("c1").setProperty("p", "1");
        children.addChild("c2").setProperty("p", "2");
        children.addChild("c3").setProperty("p", "3");
        children.addChild("c4").setProperty("p", "4");
        root.commit();

        assertEventually(() -> {
            Iterator<String> result = executeQuery(
                    "select p.[jcr:path], p2.[jcr:path] from [nt:base] as p inner join [nt:base] as p2 on ischildnode(p2, p) where p.[jcr:path] = '/'",
                    Query.JCR_SQL2).iterator();
            assertTrue(result.hasNext());
            assertEquals("/, /children", result.next());
            assertEquals("/, /jcr:system", result.next());
            assertEquals("/, /oak:index", result.next());
            assertEquals("/, /parents", result.next());
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void contains() throws Exception {
        String h = "Hello" + System.currentTimeMillis();
        String w = "World" + System.currentTimeMillis();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", asList(h, w), STRINGS);
        test.addChild("b").setProperty("name", h);
        root.commit();

        // query 'hello'
        assertEventually(() ->
                assertQuery("/jcr:root//*[jcr:contains(., '" + h + "')]", "xpath", asList("/test/a", "/test/b"))
        );

        // query 'world'
        assertEventually(() ->
                assertQuery("/jcr:root//*[jcr:contains(., '" + w + "')]", "xpath", singletonList("/test/a"))
        );
    }

    @Ignore("OAK-2424")
    @Test
    public void containsDash() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", "hello-wor");
        test.addChild("b").setProperty("name", "hello-world");
        test.addChild("c").setProperty("name", "hello");
        root.commit();

        assertQuery("/jcr:root//*[jcr:contains(., 'hello-wor*')]", "xpath", asList("/test/a", "/test/b"));
        assertQuery("/jcr:root//*[jcr:contains(., '*hello-wor*')]", "xpath", asList("/test/a", "/test/b"));
    }

    @Ignore("OAK-2424")
    @Test
    public void multiPhraseQuery() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("dc:format", "type:application/pdf");
        test.addChild("b").setProperty("dc:format", "progress");
        root.commit();

        assertQuery("/jcr:root//*[jcr:contains(@dc:format, 'pro*')]", "xpath", singletonList("/test/b"));
        assertQuery("/jcr:root//*[jcr:contains(@dc:format, 'type:appli*')]", "xpath", singletonList("/test/a"));
    }

    @Test
    public void containsPath() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", "/parent/child/node");
        root.commit();

        String stmt = "//*[jcr:contains(., '/parent/child')]";
        assertEventually(() -> assertQuery(stmt, "xpath", singletonList("/test/a")));
    }

    @Test
    public void containsPathNum() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("name", "/segment1/segment2/segment3");
        root.commit();

        String stmt = "//*[jcr:contains(., '/segment1/segment2')]";
        assertEventually(() -> assertQuery(stmt, "xpath", singletonList("/test/a")));
    }

    @Test
    public void containsPathStrict() throws Exception {
        root.getTree("/").addChild("matchOnPath");
        root.getTree("/").addChild("match_on_path");
        root.commit();

        String stmt = "//*[jcr:contains(., 'match')]";
        assertEventually(() -> assertQuery(stmt, "xpath", singletonList("/match_on_path")));
    }

    @Test
    public void containsPathStrictNum() throws Exception {
        root.getTree("/").addChild("matchOnPath1234");
        root.getTree("/").addChild("match_on_path1234");
        root.commit();

        String stmt = "//*[jcr:contains(., 'match')]";
        assertEventually(() -> assertQuery(stmt, "xpath", singletonList("/match_on_path1234")));
    }

    /**
     * OAK-1208 property existence constraints break queries
     */
    @Test
    public void testOAK1208() throws Exception {
        Tree t = root.getTree("/").addChild("containsWithMultipleOr");
        Tree one = t.addChild("one");
        one.setProperty("p", "dam/smartcollection");
        one.setProperty("t", "media");

        Tree two = t.addChild("two");
        two.setProperty("p", "dam/collection");
        two.setProperty("t", "media");

        Tree three = t.addChild("three");
        three.setProperty("p", "dam/hits");
        three.setProperty("t", "media");

        root.commit();

        String stmt = "//*[jcr:contains(., 'media') and (@p = 'dam/smartcollection' or @p = 'dam/collection') ]";
        assertEventually(() -> assertQuery(stmt, "xpath", asList(one.getPath(), two.getPath())));
    }

    @Test
    public void testNativeLuceneQuery() throws Exception {
        String nativeQueryString = "select [jcr:path] from [nt:base] where native('lucene', 'title:foo -title:bar')";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("title", "foo");
        test.addChild("b").setProperty("title", "bar");
        root.commit();

        assertEventually(() -> {
            Iterator<String> result = executeQuery(nativeQueryString, Query.JCR_SQL2).iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertFalse(result.hasNext());
        });
        Assert.assertNotEquals(0, logCustomizer.getLogs().size());
        Assert.assertTrue("native query WARN message is not present, message in Logger is "
                +  logCustomizer.getLogs(), logCustomizer.getLogs().get(0).contains(nativeQueryString));
    }

    @Test
    public void testRepSimilarAsNativeQuery() throws Exception {
        String nativeQueryString = "select [jcr:path] from [nt:base] where " +
                "native('lucene', 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0')";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World");
        test.addChild("b").setProperty("text", "He said Hello and then the world said Hello as well.");
        test.addChild("c").setProperty("text", "He said Hi.");
        root.commit();
        assertEventually(() -> {
            Iterator<String> result = executeQuery(nativeQueryString, Query.JCR_SQL2).iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertTrue(result.hasNext());
            assertEquals("/test/b", result.next());
            assertFalse(result.hasNext());
        });
        Assert.assertNotEquals(0, logCustomizer.getLogs().size());
        Assert.assertTrue("native query WARN message is not present, message in Logger is "
                +  logCustomizer.getLogs(), logCustomizer.getLogs().get(0).contains(nativeWarnLog));
    }

    @Test
    public void testRepSimilarQuery() throws Exception {
        String query = "select [jcr:path] from [nt:base] where similar(., '/test/a')";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World Hello World");
        test.addChild("b").setProperty("text", "Hello World");
        test.addChild("c").setProperty("text", "World");
        test.addChild("d").setProperty("text", "Hello");
        test.addChild("e").setProperty("text", "World");
        test.addChild("f").setProperty("text", "Hello");
        test.addChild("g").setProperty("text", "World");
        test.addChild("h").setProperty("text", "Hello");
        root.commit();
        assertEventually(() -> {
            Iterator<String> result = executeQuery(query, Query.JCR_SQL2).iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertTrue(result.hasNext());
            assertEquals("/test/b", result.next());
            assertTrue(result.hasNext());
        });
    }

    @Test
    public void testRepSimilarXPathQuery() throws Exception {
        String query = "//element(*, nt:base)[rep:similar(., '/test/a')]";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World Hello World");
        test.addChild("b").setProperty("text", "Hello World");
        test.addChild("c").setProperty("text", "World");
        test.addChild("d").setProperty("text", "Hello");
        test.addChild("e").setProperty("text", "World");
        test.addChild("f").setProperty("text", "Hello");
        test.addChild("g").setProperty("text", "World");
        test.addChild("h").setProperty("text", "Hello");
        root.commit();
        assertEventually(() -> {
            Iterator<String> result = executeQuery(query, "xpath").iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertTrue(result.hasNext());
            assertEquals("/test/b", result.next());
        });
    }

    @Test
    public void testTokenizeCN() throws Exception {
        Tree t = root.getTree("/").addChild("containsCN");
        Tree one = t.addChild("one");
        one.setProperty("t", "美女衬衫");
        root.commit();
        assertEventually(() -> assertQuery("//*[jcr:contains(., '美女')]", "xpath", singletonList(one.getPath())));
    }

    @Test
    public void testMultiValuedPropUpdate() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        String child = "child";
        String mulValuedProp = "prop";
        test.addChild(child).setProperty(mulValuedProp, asList("foo", "bar"), STRINGS);
        root.commit();
        assertEventually(() -> assertQuery("/jcr:root//*[jcr:contains(@" + mulValuedProp + ", 'foo')]", "xpath", singletonList("/test/" + child)));

        test.getChild(child).setProperty(mulValuedProp, new ArrayList<>(), STRINGS);
        root.commit();
        assertEventually(() -> assertQuery("/jcr:root//*[jcr:contains(@" + mulValuedProp + ", 'foo')]", "xpath", new ArrayList<>()));

        test.getChild(child).setProperty(mulValuedProp, singletonList("bar"), STRINGS);
        root.commit();
        assertEventually(() -> assertQuery("/jcr:root//*[jcr:contains(@" + mulValuedProp + ", 'foo')]", "xpath", new ArrayList<>()));

        test.getChild(child).removeProperty(mulValuedProp);
        root.commit();
        assertEventually(() -> assertQuery("/jcr:root//*[jcr:contains(@" + mulValuedProp + ", 'foo')]", "xpath", new ArrayList<>()));
    }

    @SuppressWarnings("unused")
    private static void walktree(final Tree t) {
        System.out.println("+ " + t.getPath());
        for (PropertyState p : t.getProperties()) {
            System.out.println("  -" + p.getName() + "=" + p.getValue(STRING));
        }
        for (Tree t1 : t.getChildren()) {
            walktree(t1);
        }
    }

    @Test
    public void oak3371() throws Exception {
        setTraversalEnabled(false);
        Tree t, t1;

        t = root.getTree("/");
        t = child(t, "test", NT_UNSTRUCTURED);
        t1 = child(t, "a", NT_UNSTRUCTURED);
        t1.setProperty("foo", "bar");
        t1 = child(t, "b", NT_UNSTRUCTURED);
        t1.setProperty("foo", "cat");
        t1 = child(t, "c", NT_UNSTRUCTURED);
        t1 = child(t, "d", NT_UNSTRUCTURED);
        t1.setProperty("foo", "bar cat");

        root.commit();

        assertEventually(() -> {
            assertQuery(
                    "SELECT * FROM [nt:unstructured] WHERE ISDESCENDANTNODE('/test') AND CONTAINS(foo, 'bar')",
                    asList("/test/a", "/test/d"));

            assertQuery(
                    "SELECT * FROM [nt:unstructured] WHERE ISDESCENDANTNODE('/test') AND NOT CONTAINS(foo, 'bar')",
                    asList("/test/b", "/test/c"));

            assertQuery(
                    "SELECT * FROM [nt:unstructured] WHERE ISDESCENDANTNODE('/test') AND CONTAINS(foo, 'bar cat')",
                    singletonList("/test/d"));

            assertQuery(
                    "SELECT * FROM [nt:unstructured] WHERE ISDESCENDANTNODE('/test') AND NOT CONTAINS(foo, 'bar cat')",
                    singletonList("/test/c"));
        });
        setTraversalEnabled(true);
    }


    @Test
    public void fullTextQueryTestAllowLeadingWildcards() throws Exception {

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "ship_to_canada");
        test.addChild("b").setProperty("propa", "steamship_to_canada");
        test.addChild("c").setProperty("propa", "ship_to_can");
        test.addChild("d").setProperty("propa", "starship");
        test.addChild("e").setProperty("propa", "Hello starship");
        root.commit();

        String query = "//*[jcr:contains(@propa, 'Hello *ship')] ";
        assertEventually(() -> assertQuery(query, XPATH, Arrays.asList("/test/e")));
    }


    @Test
    public void fullTextQueryTestAllowLeadingWildcards2() throws Exception {

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "ship_to_canada");
        test.addChild("b").setProperty("propa", "steamship_to_canada");
        test.addChild("c").setProperty("propa", "ship_to_can");
        test.addChild("d").setProperty("propa", "starship");
        test.addChild("e").setProperty("propa", "Hello starship");
        root.commit();

        String query = "//*[jcr:contains(@propa, '*ship to can*')] ";
        assertEventually(() -> assertQuery(query, XPATH, Arrays.asList("/test/a", "/test/b", "/test/c")));
    }

    @Test
    public void fullTextQueryGeneric() throws Exception {
        Tree test = root.getTree("/").addChild("test");

        Tree testNodeA = test.addChild("nodea");
        testNodeA.setProperty("a", "hello");
        testNodeA.setProperty("b", "ocean");

        Tree testNodeB = test.addChild("nodeb");
        testNodeB.setProperty("a", "hello world");
        testNodeB.setProperty("b", "soccer-shoe");

        Tree testNodeC = test.addChild("nodec");
        testNodeC.setProperty("a", "hello");
        testNodeC.setProperty("b", "world");
        root.commit();

        assertEventually(() -> {
            // case insensitive
            assertQuery("//*[jcr:contains(., 'WORLD')] ", XPATH, Arrays.asList("/test/nodeb", "/test/nodec"));

            // wild card
            assertQuery("//*[jcr:contains(., 'Hell*')] ", XPATH, Arrays.asList("/test/nodea", "/test/nodeb", "/test/nodec"));
            assertQuery("//*[jcr:contains(., 'He*o')] ", XPATH, Arrays.asList("/test/nodea", "/test/nodeb", "/test/nodec"));
            assertQuery("//*[jcr:contains(., '*llo')] ", XPATH, Arrays.asList("/test/nodea", "/test/nodeb", "/test/nodec"));
            assertQuery("//*[jcr:contains(., '?orld')] ", XPATH, Arrays.asList("/test/nodeb", "/test/nodec"));
            assertQuery("//*[jcr:contains(., 'wo?ld')] ", XPATH, Arrays.asList("/test/nodeb", "/test/nodec"));
            assertQuery("//*[jcr:contains(., 'worl?')] ", XPATH, Arrays.asList("/test/nodeb", "/test/nodec"));

            // space explained as AND
            assertQuery("//*[jcr:contains(., 'hello world')] ", XPATH, Arrays.asList("/test/nodeb", "/test/nodec"));

            // exclude
            assertQuery("//*[jcr:contains(., 'hello -world')] ", XPATH, Arrays.asList("/test/nodea"));

            // explicit OR
            assertQuery("//*[jcr:contains(., 'ocean OR world')] ", XPATH, Arrays.asList("/test/nodea", "/test/nodeb", "/test/nodec"));
        });
    }

    @Test
    public void testInequalityQuery_native() throws Exception {

        Tree test = root.getTree("/").addChild("test");
        test.addChild("test1").setProperty("propa", "hello");
        test.addChild("test2").setProperty("propa", "foo");
        test.addChild("test3").setProperty("propa", "foo");
        test.addChild("test4").setProperty("propa", "bar");
        test.addChild("test5").setProperty("propa", "bar");
        test.addChild("test6");
        root.commit();

        String query = "explain /jcr:root/test//*[propa!='bar']";

        assertEventually(getAssertionForExplain(query, XPATH, getContainsValueForInequalityQuery_native(), false));

        String query2 = "/jcr:root/test//*[propa!='bar']";

        assertEventually(() -> assertQuery(query2, XPATH, Arrays.asList("/test/test1", "/test/test2", "/test/test3")));
    }

    @Test
    public void testNotNullQuery_native() throws Exception {

        Tree test = root.getTree("/").addChild("test");
        test.addChild("test1").setProperty("propa", "hello");
        test.addChild("test2").setProperty("propa", "foo");
        test.addChild("test3").setProperty("propa", "foo");
        test.addChild("test4");
        root.commit();

        String query = "explain select * from [nt:base] as s where propa is not null and ISDESCENDANTNODE(s, '/test')";

        assertEventually(getAssertionForExplain(query, SQL2, getContainsValueForNotNullQuery_native(), false));

        String query2 = "select * from [nt:base] as s where propa is not null and ISDESCENDANTNODE(s, '/test')";

        assertEventually(() -> assertQuery(query2, SQL2, Arrays.asList("/test/test1", "/test/test2", "/test/test3")));
    }

    @Test
    public void testInequalityQueryWithoutAncestorFilter_native() throws Exception {
        Tree test = root.getTree("/");
        test.addChild("test1").setProperty("propa", "hello");
        test.addChild("test2").setProperty("propa", "foo");
        test.addChild("test3").setProperty("propa", "foo");
        test.addChild("test4").setProperty("propa", "bar");
        test.addChild("test5").setProperty("propa", "bar");
        test.addChild("test6");
        root.commit();

        String query = "explain //*[propa!='bar']";

        assertEventually(getAssertionForExplain(query, XPATH, getContainsValueForInequalityQueryWithoutAncestorFilter_native(), false));

        String query2 = "//*[propa!='bar']";

        assertEventually(() -> assertQuery(query2, XPATH, Arrays.asList("/test1", "/test2", "/test3")));
    }

    @Test
    public void testEqualityInequalityCombined_native() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("test1").setProperty("propa", "hello");
        test.getChild("test1").setProperty("propb", "world");
        test.addChild("test2").setProperty("propa", "foo");
        test.addChild("test3").setProperty("propa", "foo");
        test.addChild("test4").setProperty("propa", "bar");
        test.addChild("test5").setProperty("propa", "bar");
        test.addChild("test6").setProperty("propb", "world");
        root.commit();

        String query = "explain /jcr:root/test//*[propa!='bar' and propb='world']";
        assertEventually(getAssertionForExplain(query, XPATH, getContainsValueForEqualityInequalityCombined_native(), false));

        String query2 = "/jcr:root/test//*[propa!='bar' and propb='world']";
        // Expected - nodes with both properties defined and propb with value 'world' and propa with value not equal to bar should be returned
        // /test/test6 should NOT be returned because for it propa = null
        assertEventually(() -> assertQuery(query2, XPATH, Arrays.asList("/test/test1")));
    }


    @Test
    public void testEqualityQuery_native() throws Exception {

        Tree test = root.getTree("/").addChild("test");
        test.addChild("test1").setProperty("propa", "foo");
        test.addChild("test2").setProperty("propa", "foo");
        test.addChild("test3").setProperty("propa", "foo");
        test.addChild("test4").setProperty("propa", "bar");
        root.commit();

        String query = "explain /jcr:root/test//*[propa='bar']";

        assertEventually(getAssertionForExplain(query, XPATH, getContainsValueForEqualityQuery_native(), false));

        String query2 = "/jcr:root/test//*[propa='bar']";

        assertEventually(() -> assertQuery(query2, XPATH, Arrays.asList("/test/test4")));
    }

    @Test
    public void testDateQueryWithIncorrectData() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("test1").setProperty("propDate", "foo");
        test.getChild("test1").setProperty("propa", "bar");
        test.addChild("test2").setProperty("propDate", "2021-01-22T01:02:03.000Z", Type.DATE);
        test.addChild("test2").setProperty("propa", "bar");
        test.addChild("test3").setProperty("propDate", "2022-01-22T01:02:03.000Z", Type.DATE);
        root.commit();

        // Query on propa should work fine even if the data on propDate is of incorrect type (i.e String instead of Date)
        // It should return both /test/test1 -> where content for propDate is of incorrect data type
        // and /test/test2 -> where content for propDate is of correct data type.
        String query = "/jcr:root/test//*[propa='bar']";
        assertEventually(() -> assertQuery(query, XPATH, Arrays.asList("/test/test2", "/test/test1")));

        // Check inequality query on propDate - this should not return /test/test1 -> since that node should not have been indexed for propDate
        // due to incorrect data type in the content for this property.
        String query2 = "/jcr:root/test//*[propDate!='2021-01-22T01:02:03.000Z']";
        assertEventually(() -> assertQuery(query2, XPATH, Arrays.asList("/test/test3")));
    }

    @Test
    public void testQueryWithDifferentDataTypesForSameProperty() throws Exception {
        // propa doesn't have any type defined in index - so by default it's a String type property
        Tree test = root.getTree("/").addChild("test");
        test.addChild("test1").setProperty("propa", "bar");
        test.addChild("test2").setProperty("propa", 10);
        test.addChild("test3").setProperty("propa", 10L);
        test.addChild("test4").setProperty("propa", true);
        root.commit();

        // Below queries will ensure propa is searchable with different data types as content and behaviour is similar for lucene and elastic.
        String query = "/jcr:root/test//*[propa='bar']";
        assertEventually(() -> assertQuery(query, XPATH, Arrays.asList("/test/test1")));

        String query2 = "/jcr:root/test//*[propa=true]";
        assertEventually(() -> assertQuery(query2, XPATH, Arrays.asList("/test/test4")));

        String query3 = "/jcr:root/test//*[propa=10]";
        assertEventually(() -> assertQuery(query3, XPATH, Arrays.asList("/test/test2", "/test/test3")));
    }

    @Test
    public void testDateQueryWithCorrectData() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("test1").setProperty("propa", "foo");
        test.getChild("test1").setProperty("propDate", "2021-01-22T01:02:03.000Z", Type.DATE);
        test.addChild("test2").setProperty("propa", "foo");
        root.commit();

        // Test query returns correct node on querying on dateProp
        String query = "/jcr:root/test//*[propDate='2021-01-22T01:02:03.000Z']";
        assertEventually(() -> assertQuery(query, XPATH, Arrays.asList("/test/test1")));

        // Test query returns correct node on querying on String type property
        String query2 = "/jcr:root/test//*[propa='foo']";
        assertEventually(() -> assertQuery(query2, XPATH, Arrays.asList("/test/test1", "/test/test2")));
    }

    @Test
    public void testDateQueryWithCorrectData_Ordered() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("test1").setProperty("propa", "foo");
        test.getChild("test1").setProperty("propDate", "2021-01-22T01:02:03.000Z", Type.DATE);
        test.addChild("test2").setProperty("propa", "foo");
        test.addChild("test2").setProperty("propDate", "2019-01-22T01:02:03.000Z", Type.DATE);
        test.addChild("test3").setProperty("propa", "foo");
        test.addChild("test3").setProperty("propDate", "2020-01-22T01:02:03.000Z", Type.DATE);
        root.commit();

        // Test query returns correct node on querying on dateProp
        String query = "/jcr:root/test//*[propa='foo'] order by @propDate descending";
        assertEventually(() -> assertQuery(query, XPATH, Arrays.asList("/test/test1", "/test/test3", "/test/test2"), true, true));
    }

    private static Tree child(Tree t, String n, String type) {
        Tree t1 = t.addChild(n);
        t1.setProperty(JCR_PRIMARYTYPE, type, Type.NAME);
        return t1;
    }

    public abstract String getContainsValueForEqualityQuery_native();

    public abstract String getContainsValueForInequalityQuery_native();

    public abstract String getContainsValueForInequalityQueryWithoutAncestorFilter_native();

    public abstract String getContainsValueForEqualityInequalityCombined_native();

    public abstract String getContainsValueForNotNullQuery_native();

    public abstract String getExplainValueForDescendantTestWithIndexTagExplain();

    private Runnable getAssertionForExplain(String query, String language, String expected, boolean matchComplete) {
        return () -> {
            Result result = null;
            try {
                result = executeQuery(query, language, NO_BINDINGS);
            } catch (ParseException e) {
                fail(e.getMessage());
            }
            ResultRow row = result.getRows().iterator().next();
            if (matchComplete) {
                assertEquals(row.getValue("plan").toString(), expected);
            } else {
                assertTrue(row.getValue("plan").toString().contains(expected));
            }
        };
    }

    private static void assertEventually(Runnable r) {
        TestUtil.assertEventually(r, 3000 * 3);
    }
}
