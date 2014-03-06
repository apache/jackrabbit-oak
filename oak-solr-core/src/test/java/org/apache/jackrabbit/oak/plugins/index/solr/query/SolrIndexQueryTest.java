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

import java.util.Iterator;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.solr.TestUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * General query extensive testcase for {@link SolrQueryIndex}
 */
public class SolrIndexQueryTest extends AbstractQueryTest {

    private SolrServer solrServer;

    @After
    public void tearDown() throws Exception {
        solrServer.deleteByQuery("*:*");
        solrServer.commit();
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        createTestIndexNode(index, SolrQueryIndex.TYPE);
        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        TestUtils provider = new TestUtils();
        solrServer = provider.getSolrServer();
        try {
            return new Oak().with(new InitialContent())
                    .with(new OpenSecurityProvider())
                    .with(new SolrQueryIndexProvider(provider, provider))
                    .with(new SolrIndexEditorProvider(provider, provider))
                    .createContentRepository();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sql2() throws Exception {
        test("sql2.txt");
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
    public void testNativeSolrNestedQuery() throws Exception {
        String nativeQueryString = "select [jcr:path] from [nt:base] where native('solr', '_query_:\"{!dismax qf=catch_all q.op=OR}hello world\"')";

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
    public void testNativeMLTQuery() throws Exception {
        String nativeQueryString = "select [jcr:path] from [nt:base] where native('solr', 'mlt?q=name:World&mlt.fl=name&mlt.mindf=0&mlt.mintf=0')";

        Tree tree = root.getTree("/");
        Tree test = tree.addChild("test");
        test.addChild("a").setProperty("name", "Hello World, today weather is nice");
        test.addChild("b").setProperty("name", "Cheers World, today weather is quite nice");
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
    public void nativeSolr() throws Exception {
        test("native_solr.txt");
    }
}
