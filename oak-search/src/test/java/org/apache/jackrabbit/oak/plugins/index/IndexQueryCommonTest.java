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
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_VALUE_REGEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the query engine using the default index implementation: the
 * IndexProvider
 */
public abstract class IndexQueryCommonTest extends AbstractQueryTest {

    private Tree indexDefn;
    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
        Tree index = root.getTree("/");
        indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:base");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enablePropertyIndex(props, "c1/p", false);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);
        TestUtil.enablePropertyIndex(props, "a/name", false);
        TestUtil.enablePropertyIndex(props, "b/name", false);
        TestUtil.enableFunctionIndex(props, "length([name])");
        TestUtil.enableFunctionIndex(props, "lower([name])");
        TestUtil.enableFunctionIndex(props, "upper([name])");

        root.commit();
    }

    @Ignore
    //TODO ES failing
    @Test
    public void sql1() throws Exception {
        test("sql1.txt");
    }

    @Ignore
    //TODO ES Failing
    @Test
    public void sql2() throws Exception {
        test("sql2.txt");
    }

    //TODO ES test failing
    @Ignore
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
            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
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
            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
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
                    "JCR-SQL2").iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertEquals("/test/b", result.next());
            assertFalse(result.hasNext());
        });
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
                    "JCR-SQL2").iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertFalse(result.hasNext());
        });
    }

    //TODO ES Failing
    @Ignore
    @Test
    public void ischildnodeTest() throws Exception {
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
                    "JCR-SQL2").iterator();
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
            Iterator<String> result = executeQuery(nativeQueryString, "JCR-SQL2").iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertFalse(result.hasNext());
        });
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
            Iterator<String> result = executeQuery(nativeQueryString, "JCR-SQL2").iterator();
            assertTrue(result.hasNext());
            assertEquals("/test/a", result.next());
            assertTrue(result.hasNext());
            assertEquals("/test/b", result.next());
            assertFalse(result.hasNext());
        });
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
            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
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

    private static Tree child(Tree t, String n, String type) {
        Tree t1 = t.addChild(n);
        t1.setProperty(JCR_PRIMARYTYPE, type, Type.NAME);
        return t1;
    }

    private static void assertEventually(Runnable r) {
        TestUtils.assertEventually(r, 3000 * 3);
    }
}
