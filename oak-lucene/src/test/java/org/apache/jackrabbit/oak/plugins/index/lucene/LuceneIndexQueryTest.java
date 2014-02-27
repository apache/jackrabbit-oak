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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the query engine using the default index implementation: the
 * {@link LuceneIndexProvider}
 */
public class LuceneIndexQueryTest extends AbstractQueryTest {

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        createTestIndexNode(index, LuceneIndexConstants.TYPE_LUCENE);
        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new LowCostLuceneIndexProvider())
                .with(new LuceneIndexEditorProvider())
                .createContentRepository();
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
    public void descendantTest() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        Iterator<String> result = executeQuery(
                "select [jcr:path] from [nt:base] where isdescendantnode('/test')",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertEquals("/test/b", result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void descendantTest2() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", asList("Hello", "World"), STRINGS);
        test.addChild("b").setProperty("name", "Hello");
        root.commit();

        Iterator<String> result = executeQuery(
                "select [jcr:path] from [nt:base] where isdescendantnode('/test') and name='World'",
                "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
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
        assertEquals("/, /jcr:system", result.next());
        assertEquals("/, /oak:index", result.next());
        assertEquals("/, /parents", result.next());
        assertFalse(result.hasNext());
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
    public void containsPath() throws Exception {

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", "/parent/child/node");
        root.commit();

        StringBuffer stmt = new StringBuffer();
        stmt.append("//*[jcr:contains(., '/parent/child')]");
        assertQuery(stmt.toString(), "xpath", ImmutableList.of("/test/a"));

    }

    @Test
    public void containsPathNum() throws Exception {

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("name", "/segment1/segment2/segment3");
        root.commit();

        StringBuffer stmt = new StringBuffer();
        stmt.append("//*[jcr:contains(., '/segment1/segment2')]");
        assertQuery(stmt.toString(), "xpath", ImmutableList.of("/test/a"));

    }

    @Test
    public void containsPathStrict() throws Exception {
        root.getTree("/").addChild("matchOnPath");
        root.getTree("/").addChild("match_on_path");
        root.commit();

        StringBuffer stmt = new StringBuffer();
        stmt.append("//*[jcr:contains(., 'match')]");
        assertQuery(stmt.toString(), "xpath",
                ImmutableList.of("/match_on_path"));

    }

    @Test
    public void containsPathStrictNum() throws Exception {
        root.getTree("/").addChild("matchOnPath1234");
        root.getTree("/").addChild("match_on_path1234");
        root.commit();

        StringBuffer stmt = new StringBuffer();
        stmt.append("//*[jcr:contains(., 'match')]");
        assertQuery(stmt.toString(), "xpath",
                ImmutableList.of("/match_on_path1234"));

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

        StringBuffer stmt = new StringBuffer();
        stmt.append("//*[jcr:contains(., 'media') and (@p = 'dam/smartcollection' or @p = 'dam/collection') ]");
        assertQuery(stmt.toString(), "xpath",
                ImmutableList.of(one.getPath(), two.getPath()));
    }

    @Test
    public void testNativeLuceneQuery() throws Exception {
        String nativeQueryString = "select [jcr:path] from [nt:base] where native('lucene', 'title:foo -title:bar')";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("title", "foo");
        test.addChild("b").setProperty("title", "bar");
        root.commit();

        Iterator<String> result = executeQuery(nativeQueryString, "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertFalse(result.hasNext());
    }

}
