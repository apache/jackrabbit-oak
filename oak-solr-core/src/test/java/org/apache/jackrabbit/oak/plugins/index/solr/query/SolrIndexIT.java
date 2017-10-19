/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import javax.jcr.query.Query;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.DefaultSolrServerProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * General query extensive testcase for {@link SolrQueryIndex}
 */
public class SolrIndexIT extends AbstractQueryTest {

    @Rule
    public TestName name = new TestName();

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        Tree solrIndexNode = createTestIndexNode(index, SolrQueryIndex.TYPE);
        solrIndexNode.setProperty("pathRestrictions", true);
        solrIndexNode.setProperty("propertyRestrictions", true);
        solrIndexNode.setProperty("primaryTypes", true);
        solrIndexNode.setProperty("commitPolicy", "hard");
        Tree server = solrIndexNode.addChild("server");
        server.setProperty("solrServerType", "embedded");
        server.setProperty("solrHomePath", "target/" + name.getMethodName());

        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        try {
            DefaultSolrServerProvider solrServerProvider = new DefaultSolrServerProvider();
            DefaultSolrConfigurationProvider oakSolrConfigurationProvider = new DefaultSolrConfigurationProvider();
            return new Oak().with(new InitialContent())
                    .with(new OpenSecurityProvider())
                    .with(new SolrQueryIndexProvider(solrServerProvider, oakSolrConfigurationProvider))
                    .with(new SolrIndexEditorProvider(solrServerProvider, oakSolrConfigurationProvider))
                    .createContentRepository();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    public void sql2FullText() throws Exception {
        test("sql2-fulltext.txt");
    }

    @Test
    @Ignore("OAK-420")
    public void sql2Measure() throws Exception {
        test("sql2_measure.txt");
    }

    @Test
    public void descendantTest() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").addChild("c");
        test.addChild("b").addChild("d");
        root.commit();

        Iterator<String> result = executeQuery(
                "select [jcr:path] from [nt:base] where isdescendantnode('/test')",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/a/c", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/b", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/b/d", result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void descendantTest2() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").addChild("c").setProperty("name", asList("Hello", "World"), STRINGS);
        test.addChild("b").setProperty("name", "Hello");
        root.commit();

        Iterator<String> result = executeQuery(
                "select [jcr:path] from [nt:base] where isdescendantnode('/test') and name='World'",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a/c", result.next());
        assertFalse(result.hasNext());
    }

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

        Iterator<String> result = executeQuery(
                "select p.[jcr:path], p2.[jcr:path] from [nt:base] as p inner join [nt:base] as p2 on ischildnode(p2, p) where p.[jcr:path] = '/'",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/, /children", result.next());
        assertTrue(result.hasNext());
        assertEquals("/, /jcr:system", result.next());
        assertTrue(result.hasNext());
        assertEquals("/, /oak:index", result.next());
        assertTrue(result.hasNext());
        assertEquals("/, /parents", result.next());
        assertFalse(result.hasNext());

    }

    @Test
    public void ischildnodeTest2() throws Exception {
        Tree tree = root.getTree("/");
        Tree test = tree.addChild("test");
        test.addChild("jcr:resource").addChild("x");
        test.addChild("resource");
        root.commit();

        Iterator<String> strings = executeQuery("select [jcr:path] from [nt:base] as b where ischildnode(b, '/test')", "JCR-SQL2").iterator();
        assertTrue(strings.hasNext());
        assertEquals("/test/jcr:resource", strings.next());
        assertTrue(strings.hasNext());
        assertEquals("/test/resource", strings.next());
        assertFalse(strings.hasNext());
    }

    @Test
    public void testNativeSolrQuery() throws Exception {
        String nativeQueryString = "select [jcr:path] from [nt:base] where native('solr', 'name:(Hello OR World)')";

        Tree tree = root.getTree("/");
        Tree test = tree.addChild("test");
        test.addChild("a").setProperty("name", "Hello");
        test.addChild("b").setProperty("name", "World");
        test.addChild("c");
        root.commit();

        Iterator<String> strings = executeQuery(nativeQueryString, "JCR-SQL2").iterator();
        assertTrue(strings.hasNext());
        assertEquals("/test/a", strings.next());
        assertTrue(strings.hasNext());
        assertEquals("/test/b", strings.next());
        assertFalse(strings.hasNext());
    }

    @Test
    public void testNativeSolrFunctionQuery() throws Exception {
        String nativeQueryString = "select [jcr:path] from [nt:base] where native('solr', 'path_child:\\/test  _val_:\"recip(rord(name),1,2,3)\"')";

        Tree tree = root.getTree("/");
        Tree test = tree.addChild("test");
        test.addChild("a").setProperty("name", "Hello");
        test.addChild("b").setProperty("name", "World");
        tree.addChild("c");
        root.commit();

        Iterator<String> strings = executeQuery(nativeQueryString, "JCR-SQL2").iterator();
        assertTrue(strings.hasNext());
        assertEquals("/test/a", strings.next());
        assertTrue(strings.hasNext());
        assertEquals("/test/b", strings.next());
        assertFalse(strings.hasNext());
    }

    @Test
    public void testNativeSolrLocalParams() throws Exception {
        String nativeQueryString = "select [jcr:path] from [nt:base] where native('solr', '_query_:\"{!dismax qf=catch_all q.op=OR}hello world\"')";

        Tree tree = root.getTree("/");
        Tree test = tree.addChild("test");
        test.addChild("a").setProperty("name", "Hello");
        test.addChild("b").setProperty("name", "World");
        test.addChild("c");
        root.commit();

        Iterator<String> strings = executeQuery(nativeQueryString, "JCR-SQL2").iterator();
        assertTrue(strings.hasNext());
        assertEquals("/test/a", strings.next());
        assertTrue(strings.hasNext());
        assertEquals("/test/b", strings.next());
        assertFalse(strings.hasNext());
    }

    @Test
    public void testNativeMLTQuery() throws Exception {
        // TODO: OAK-1819
        assumeTrue(!System.getProperty("java.version").startsWith("1.8"));
        assumeTrue(!System.getProperty("java.version").startsWith("9"));

        String nativeQueryString = "select [jcr:path] from [nt:base] where native('solr', 'mlt?q=text:World&mlt.fl=text&mlt.mindf=0&mlt.mintf=0')";

        Tree tree = root.getTree("/");
        Tree test = tree.addChild("test");
        test.addChild("a").setProperty("text", "Hello World, today weather is nice");
        test.addChild("b").setProperty("text", "Cheers World, today weather is quite nice");
        test.addChild("c").setProperty("text", "Halo Welt, today sky is cloudy");
        root.commit();

        Iterator<String> strings = executeQuery(nativeQueryString, "JCR-SQL2").iterator();
        assertTrue(strings.hasNext());
        assertEquals("/test/a", strings.next());
        assertTrue(strings.hasNext());
        assertEquals("/test/c", strings.next());
        assertFalse(strings.hasNext());
    }

    @Test
    public void testNativeMLTQueryWithStream() throws Exception {
        // TODO: OAK-1819
        assumeTrue(!System.getProperty("java.version").startsWith("1.8"));
        assumeTrue(!System.getProperty("java.version").startsWith("9"));

        String nativeQueryString = "select [jcr:path] from [nt:base] where native('solr', 'mlt?stream.body=world is nice today&mlt.fl=text&mlt.mindf=0&mlt.mintf=0')";

        Tree tree = root.getTree("/");
        Tree test = tree.addChild("test");
        test.addChild("a").setProperty("text", "Hello World, today weather is nice");
        test.addChild("b").setProperty("text", "Cheers World, today weather is quite nice");
        test.addChild("c").setProperty("text", "Halo Welt, today sky is cloudy");
        root.commit();

        Iterator<String> strings = executeQuery(nativeQueryString, "JCR-SQL2").iterator();
        assertTrue(strings.hasNext());
        assertEquals("/test/a", strings.next());
        assertTrue(strings.hasNext());
        assertEquals("/test/c", strings.next());
        assertFalse(strings.hasNext());
    }

    @Test
    public void testRepSimilarXPathQuery() throws Exception {
        String query = "//element(*, nt:base)[rep:similar(., '/test/a')]";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "the quick brown fox jumped over the lazy white dog");
        test.addChild("b").setProperty("text", "I am a dog");
        test.addChild("c").setProperty("text", "dogs don't hurt");
        test.addChild("d").setProperty("text", "white men can't jump");
        test.addChild("e").setProperty("text", "the fox is brown");
        test.addChild("f").setProperty("text", "a quickest dog jumped over the quick white dog");
        test.addChild("g").setProperty("text", "hello world");
        test.addChild("h").setProperty("text", "over the lazy top");
        root.commit();
        Iterator<String> result = executeQuery(query, "xpath").iterator();
        String list = "";
        while (result.hasNext()) {
            String p = result.next();
            if (p.startsWith("/oak:index")) {
                // OAK-3728
                // /oak:index nodes can match, because they have an info
                // we ignore those
                continue;
            }
            list += p + " ";
        }
        assertEquals(
                "/test/b " +
                        "/test/d " +
                        "/test/e " +
                        "/test/f " +
                        "/test/h ", list);
    }

    @Test
    public void nativeSolr() throws Exception {
        test("native_solr.txt");
    }

    @Test
    public void testTokenizeCN() throws Exception {
        Tree t = root.getTree("/").addChild("containsCN");
        Tree one = t.addChild("one");
        one.setProperty("t", "美女衬衫");
        root.commit();
        assertQuery("//*[jcr:contains(., '美女')]", "xpath", ImmutableList.of(one.getPath()));
    }

    @Test
    public void testCompositeRepExcerpt() throws Exception {
        String sqlQuery = "select [jcr:path], [jcr:score], [rep:excerpt] from [nt:base] as a " +
                "where (contains([jcr:content/*], 'square') or contains([jcr:content/jcr:title], 'square')" +
                " or contains([jcr:content/jcr:description], 'square')) and isdescendantnode(a, '/test') " +
                "order by [jcr:score] desc";
        Tree tree = root.getTree("/");
        Tree test = tree.addChild("test");
        Tree child = test.addChild("child");
        Tree a = child.addChild("a");
        a.setProperty("jcr:title", "Hello World, today square is nice");
        Tree b = child.addChild("b");
        b.setProperty("jcr:description", "Cheers World, today weather is squary nice");
        Tree c = child.addChild("c");
        c.setProperty("jcr:title", "Halo Welt, today sky is square");
        root.commit();

        Iterator<String> strings = executeQuery(sqlQuery, "JCR-SQL2").iterator();
        assertTrue(strings.hasNext());
        assertTrue(strings.next().startsWith("/test/child,"));
        assertFalse(strings.hasNext());
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
        StringBuffer stmt = new StringBuffer();
        stmt.append("/jcr:root//*[jcr:contains(., '").append(h);
        stmt.append("')]");
        assertQuery(stmt.toString(), "xpath",
                ImmutableList.of("/test/a", "/test/b"));

        // query 'world'
        stmt = new StringBuffer();
        stmt.append("/jcr:root//*[jcr:contains(., '").append(w);
        stmt.append("')]");
        assertQuery(stmt.toString(), "xpath", ImmutableList.of("/test/a"));

    }

    @Test
    @Ignore("depends on chosen text_general tokenizer")
    public void containsDash() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", "hello-wor");
        test.addChild("b").setProperty("name", "hello-world");
        test.addChild("c").setProperty("name", "hello");
        root.commit();

        assertQuery("/jcr:root//*[jcr:contains(., 'hello-wor*')]", "xpath",
                ImmutableList.of("/test/a", "/test/b"));
        assertQuery("/jcr:root//*[jcr:contains(., '*hello-wor*')]", "xpath",
                ImmutableList.of("/test/a", "/test/b"));

    }

    @Test
    public void multiPhraseQuery() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("dc:format", "type:application/pdf");
        root.commit();

        assertQuery(
                "/jcr:root//*[jcr:contains(@dc:format, 'type:appli*')]",
                "xpath", ImmutableList.of("/test/a"));

    }

    @Test
    public void containsPath() throws Exception {

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", "/parent/child/node");
        root.commit();

        assertQuery("//*[jcr:contains(., '/parent/child')]", "xpath", ImmutableList.of("/test/a"));

    }

    @Test
    public void containsPathNum() throws Exception {

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("name", "/segment1/segment2/segment3");
        root.commit();

        assertQuery("//*[jcr:contains(., '/segment1/segment2')]", "xpath", ImmutableList.of("/test/a"));

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

        assertQuery("//*[jcr:contains(., 'media') and (@p = 'dam/smartcollection' or @p = 'dam/collection') ]", "xpath",
                ImmutableList.of(one.getPath(), two.getPath()));
    }

    @Test
    public void testSortingOnPath() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").addChild("c");
        test.addChild("b").addChild("d");
        root.commit();

        Iterator<String> result = executeQuery(
                "select [jcr:path] from [nt:base] where isdescendantnode('/test') order by [jcr:path] asc",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/a/c", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/b", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/b/d", result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void testSortingOnProperty() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("foo", "bar");
        a.addChild("c").setProperty("foo", "car");
        Tree b = test.addChild("b");
        b.setProperty("foo", "tar");
        b.addChild("d").setProperty("foo", "jar");
        root.commit();

        Iterator<String> result = executeQuery(
                "select [jcr:path] from [nt:base] where isdescendantnode('/test') order by [foo] asc",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/a/c", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/b/d", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/b", result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void testOrderByJcrScore() throws Exception {
        Tree index = root.getTree("/oak:index/" + TEST_INDEX_NAME);
        assertTrue(index.exists());

        index.setProperty("rows", 10000);
        index.setProperty("reindex", true);
        root.commit();

        Tree content = root.getTree("/").addChild("content");
        Tree a = content.addChild("a");
        a.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        a.setProperty("type", "doc doc doc");
        root.commit();

        String statement = "select [jcr:path], [jcr:score], [rep:excerpt] " +
                "from [nt:unstructured] as a " +
                "where contains(*, 'doc') " +
                "and isdescendantnode(a, '/content') " +
                "order by [jcr:score] desc";

        Iterator<String> results = executeQuery(statement, Query.JCR_SQL2, true).iterator();
        assertTrue(results.hasNext());
        assertEquals("/content/a", results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void testOrderByMVProperty() throws Exception {
        Tree index = root.getTree("/oak:index/" + TEST_INDEX_NAME);
        assertTrue(index.exists());

        index.setProperty("rows", 10000);
        index.setProperty("reindex", true);
        root.commit();

        Tree content = root.getTree("/").addChild("content");
        Tree a = content.addChild("a");
        a.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        a.setProperty("type", Arrays.asList("a", "z"), Type.STRINGS);
        a.setProperty("text", "a doc");
        Tree aContent = a.addChild("jcr:content");
        aContent.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        aContent.setProperty("type", Collections.singletonList("3"), Type.STRINGS);
        Tree b = content.addChild("b");
        b.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        b.setProperty("type", Arrays.asList("b", "y"), Type.STRINGS);
        b.setProperty("text", "b doc");
        Tree bContent = b.addChild("jcr:content");
        bContent.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        bContent.setProperty("type", Collections.singletonList("1"), Type.STRINGS);
        root.commit();
        Tree c = content.addChild("c");
        c.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        c.setProperty("type", Arrays.asList("c", "x"), Type.STRINGS);
        c.setProperty("text", "c doc");
        Tree cContent = c.addChild("jcr:content");
        cContent.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        cContent.setProperty("type", Collections.singletonList("2"), Type.STRINGS);
        root.commit();

        String statement = "select [jcr:path], [jcr:score] " +
                "from [nt:unstructured] as a " +
                "where contains(*, 'doc') " +
                "and isdescendantnode(a, '/content') " +
                "order by [type] desc";

        Iterator<String> results = executeQuery(statement, Query.JCR_SQL2, true).iterator();
        assertTrue(results.hasNext());
        assertEquals("/content/c", results.next());
        assertTrue(results.hasNext());
        assertEquals("/content/b", results.next());
        assertTrue(results.hasNext());
        assertEquals("/content/a", results.next());
        assertFalse(results.hasNext());

        statement = "select [jcr:path], [jcr:score] " +
                "from [nt:unstructured] as a " +
                "where contains(*, 'doc') " +
                "and isdescendantnode(a, '/content') " +
                "order by [type] asc";

        results = executeQuery(statement, Query.JCR_SQL2, true).iterator();
        assertTrue(results.hasNext());
        assertEquals("/content/a", results.next());
        assertTrue(results.hasNext());
        assertEquals("/content/b", results.next());
        assertTrue(results.hasNext());
        assertEquals("/content/c", results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void testCollapsedJcrContentNodeDescandants() throws Exception {

        Tree index = root.getTree("/oak:index/" + TEST_INDEX_NAME);
        assertTrue(index.exists());

        index.setProperty("collapseJcrContentNodes", true);
        index.setProperty("reindex", true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        Tree content = test.addChild("content");
        Tree content1 = content.addChild("sample1").addChild("jcr:content");
        content1.setProperty("foo", "bar");
        content1.addChild("text").setProperty("text", "bar");
        Tree content2 = content.addChild("sample2").addChild("jcr:content");
        content2.setProperty("foo", "bar");
        content2.addChild("text").setProperty("text", "bar");
        root.commit();

        String xpath = "/jcr:root/test/content//element(*, nt:base)[jcr:contains(., 'bar')]";

        Iterator<String> result = executeQuery(xpath, XPATH).iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/content/sample1/jcr:content", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/content/sample2/jcr:content", result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void testNotNullAndNative() throws Exception {
        Tree index = root.getTree("/oak:index/" + TEST_INDEX_NAME);
        assertTrue(index.exists());

        index.setProperty("rows", 10000);
        index.setProperty("reindex", true);
        root.commit();

        Tree content = root.getTree("/").addChild("content");
        Tree a = content.addChild("a");
        a.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        a.setProperty("foo", "doc doc doc");
        a.setProperty("loc", "2");
        Tree b = content.addChild("b");
        b.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        b.setProperty("foo", "bye bye bye");
        b.setProperty("loc", "1");
        root.commit();

        String query = "select [jcr:path] from [nt:base] where native('solr','select?q=loc:*') AND foo IS NOT NULL";

        Iterator<String> results = executeQuery(query, Query.JCR_SQL2, true).iterator();
        assertTrue(results.hasNext());
        assertEquals("/content/a", results.next());
        assertTrue(results.hasNext());
        assertEquals("/content/b", results.next());
        assertFalse(results.hasNext());
    }
}
