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

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.event.Level;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public abstract class FunctionIndexCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected String getIndexProvider() {
        return "lucene:";
    }

    protected void postCommitHook(){
        // does nothing by default
    }

    @Test
    public void noIndexTest() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        for (int idx = 0; idx < 3; idx++) {
            Tree low = test.addChild("" + (char) ('a' + idx));
            low.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            Tree up = test.addChild("" + (char) ('A' + idx));
            up.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        }
        root.commit();
        postCommitHook();

        String query = "select [jcr:path] from [nt:base] where lower(localname()) = 'b'";
        assertThat(explain(query), containsString("traverse"));
        assertQuery(query, Lists.newArrayList("/test/b", "/test/B"));

        String queryXPath = "/jcr:root/test//*[fn:lower-case(fn:local-name()) = 'b']";
        assertThat(explainXpath(queryXPath), containsString("traverse"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/b", "/test/B"));

        queryXPath = "/jcr:root/test//*[fn:lower-case(fn:local-name()) > 'b']";
        assertThat(explainXpath(queryXPath), containsString("traverse"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/c", "/test/C"));

        query = "select [jcr:path] from [nt:base] where lower(localname()) = 'B'";
        assertThat(explain(query), containsString("traverse"));
        assertQuery(query, Lists.<String>newArrayList());
    }

    @Test
    public void lowerCaseLocalName() throws Exception {
        Tree luceneIndex = createIndex("lowerLocalName", Collections.<String>emptySet());
        luceneIndex.setProperty("excludedPaths",
                Lists.newArrayList("/jcr:system", "/oak:index"), Type.STRINGS);
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("lowerLocalName");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "lower(localname())");

        Tree test = root.getTree("/").addChild("test");
        for (int idx = 0; idx < 3; idx++) {
            Tree low = test.addChild("" + (char) ('a' + idx));
            low.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            Tree up = test.addChild("" + (char) ('A' + idx));
            up.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        }
        root.commit();
        postCommitHook();

        String query = "select [jcr:path] from [nt:base] where lower(localname()) = 'b'";
        assertThat(explain(query), containsString(getIndexProvider() + "lowerLocalName"));
        assertQuery(query, Lists.newArrayList("/test/b", "/test/B"));

        String queryXPath = "/jcr:root//*[fn:lower-case(fn:local-name()) = 'b']";
        assertThat(explainXpath(queryXPath), containsString(getIndexProvider() + "lowerLocalName"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/b", "/test/B"));

        queryXPath = "/jcr:root//*[fn:lower-case(fn:local-name()) > 'b']";
        assertThat(explainXpath(queryXPath), containsString(getIndexProvider() + "lowerLocalName"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/c", "/test/C", "/test"));

        query = "select [jcr:path] from [nt:base] where lower(localname()) = 'B'";
        assertThat(explain(query), containsString(getIndexProvider() + "lowerLocalName"));
        assertQuery(query, Lists.<String>newArrayList());
    }

    @Test
    public void lengthName() throws Exception {
        Tree luceneIndex = createIndex("lengthName", Collections.<String>emptySet());
        luceneIndex.setProperty("excludedPaths",
                Lists.newArrayList("/jcr:system", "/oak:index"), Type.STRINGS);
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("lengthName");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        func.setProperty(FulltextIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:string-length(fn:name())");

        Tree test = root.getTree("/").addChild("test");
        for (int idx = 1; idx < 1000; idx *= 10) {
            Tree testNode = test.addChild("test" + idx);
            testNode.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        }
        root.commit();
        postCommitHook();

        String query = "select [jcr:path] from [nt:base] where length(name()) = 6";
        assertThat(explain(query), containsString(getIndexProvider() + "lengthName"));
        assertQuery(query, Lists.newArrayList("/test/test10"));

        String queryXPath = "/jcr:root//*[fn:string-length(fn:name()) = 7]";
        assertThat(explainXpath(queryXPath), containsString(getIndexProvider() + "lengthName"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/test100"));

        queryXPath = "/jcr:root//* order by fn:string-length(fn:name())";
        assertThat(explainXpath(queryXPath), containsString(getIndexProvider() + "lengthName"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList(
                "/test", "/test/test1", "/test/test10", "/test/test100"));
    }

    @Test
    public void length() throws Exception {
        Tree luceneIndex = createIndex("length", Collections.<String>emptySet());
        luceneIndex.setProperty("excludedPaths",
                Lists.newArrayList("/jcr:system", "/oak:index"), Type.STRINGS);
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("lengthName");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:string-length(@value)");

        Tree test = root.getTree("/").addChild("test");
        for (int idx = 1; idx <= 1000; idx *= 10) {
            Tree testNode = test.addChild("test" + idx);
            testNode.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            testNode.setProperty("value", new byte[idx]);
        }
        root.commit();
        postCommitHook();

        String query = "select [jcr:path] from [nt:base] where length([value]) = 100";
        assertThat(explain(query), containsString(getIndexProvider() + "length"));
        assertQuery(query, Lists.newArrayList("/test/test100"));

        String queryXPath = "/jcr:root//*[fn:string-length(@value) = 10]";
        assertThat(explainXpath(queryXPath), containsString(getIndexProvider() + "length"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/test10"));
    }

    @Test
    public void upperCase() throws Exception {
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("upperName");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(@name)");

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        List<String> paths = Lists.newArrayList();
        for (int idx = 0; idx < 15; idx++) {
            Tree a = test.addChild("n" + idx);
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            a.setProperty("name", "10% foo");
            paths.add("/test/n" + idx);
        }
        root.commit();
        postCommitHook();

        String query = "select [jcr:path] from [nt:unstructured] where upper([name]) = '10% FOO'";
        assertThat(explain(query), containsString(getIndexProvider() + "upper"));
        assertQuery(query, paths);

        query = "select [jcr:path] from [nt:unstructured] where upper([name]) like '10\\% FOO'";
        assertThat(explain(query), containsString(getIndexProvider() + "upper"));
        assertQuery(query, paths);
    }

    @Test
    public void path() throws Exception {
        Tree index = createIndex("pathIndex", Collections.<String>emptySet());
        Tree func = index.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("pathFunction");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "path()");

        Tree test = root.getTree("/").addChild("test");
        test.addChild("hello");
        test.addChild("world");
        test.addChild("hello world");
        root.commit();
        postCommitHook();

        String query = "select [jcr:path] from [nt:base] where path() = '/test/world'";
        assertThat(explain(query), containsString(getIndexProvider() + "pathIndex(/oak:index/pathIndex)"));
        assertQuery(query, asList("/test/world"));

        query = "select [jcr:path] from [nt:base] where path() like '%hell%'";
        assertThat(explain(query), containsString(getIndexProvider() + "pathIndex(/oak:index/pathIndex)"));
        assertQuery(query, asList("/test/hello", "/test/hello world"));

        query = "select [jcr:path] from [nt:base] where path() like '%ll_'";
        assertThat(explain(query), containsString(getIndexProvider() + "pathIndex(/oak:index/pathIndex)"));
        assertQuery(query, asList("/test/hello"));

    }

    @Test
    public void testOrdering2() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:unstructured");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);
        Tree upper = TestUtil.enableFunctionIndex(props, "upper([foo])");
        upper.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree a = test.addChild("n1");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "hello");

        a = test.addChild("n2");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "World!");
        a = test.addChild("n3");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "Hallo");
        a = test.addChild("n4");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "10%");
        a = test.addChild("n5");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "10 percent");

        a = test.addChild("n0");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a = test.addChild("n9");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        String query = "select a.[foo]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo)";

        root.commit();
        postCommitHook();

        assertThat(explain(query), containsString(getIndexProvider() + "test-index(/oak:index/test-index)"));

        List<String> result = executeQuery(query, SQL2);
        assertEquals("Ordering doesn't match", asList("10 percent", "10%", "Hallo", "hello", "World!"), result);

    }

    /*
     * Test order by func(a),func(b)
     * order by func(b),func(a)
     * func(a) DESC,func(b)
     * func(a),func(b)DESC
     * where both func(a) and func(b) have ordered set = true
     * Correct ordering is effectively served by the index
     */
    @Test
    public void testOrdering3() throws Exception {

        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:unstructured");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        Tree upper = TestUtil.enableFunctionIndex(props, "upper([foo])");
        upper.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        Tree upper2 = TestUtil.enableFunctionIndex(props, "upper([foo2])");
        upper2.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree a = test.addChild("n1");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b2");

        a = test.addChild("n2");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a2");
        a.setProperty("foo2", "b3");

        a = test.addChild("n3");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a3");
        a.setProperty("foo2", "b1");

        a = test.addChild("n4");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b3");

        a = test.addChild("n5");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b1");

        String query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo),upper(a.foo2)";

        root.commit();
        postCommitHook();

        List<String> result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a1, b1", "a1, b2", "a1, b3", "a2, b3", "a3, b1"), result);

        query = "select a.[foo2],a.[foo]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo2),upper(a.foo)";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("b1, a1", "b1, a3", "b2, a1", "b3, a1", "b3, a2"), result);

        query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo) DESC, upper(a.foo2)";
        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a3, b1", "a2, b3", "a1, b1", "a1, b2", "a1, b3"), result);

        query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo), upper(a.foo2) DESC";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a1, b3", "a1, b2", "a1, b1", "a2, b3", "a3, b1"), result);

    }

    /*
     * Test order by func(a),func(b)
     * order by func(b),func(a)
     * func(a) DESC,func(b)
     * func(a),func(b)DESC
     * where only func(a) is ordered by index
     * The effective ordering in this case will be done by QueryEngine
     */
    @Test
    public void testOrdering4() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:unstructured");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        Tree upper = TestUtil.enableFunctionIndex(props, "upper([foo])");
        upper.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        TestUtil.enableFunctionIndex(props, "upper([foo2])");

        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree a = test.addChild("n1");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b2");

        a = test.addChild("n2");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a2");
        a.setProperty("foo2", "b3");

        a = test.addChild("n3");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a3");
        a.setProperty("foo2", "b1");

        a = test.addChild("n4");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b3");

        a = test.addChild("n5");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b1");

        String query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo),upper(a.foo2)";

        root.commit();
        postCommitHook();

        List<String> result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a1, b1", "a1, b2", "a1, b3", "a2, b3", "a3, b1"), result);

        query = "select a.[foo2],a.[foo]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo2),upper(a.foo)";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("b1, a1", "b1, a3", "b2, a1", "b3, a1", "b3, a2"), result);

        query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo) DESC, upper(a.foo2)";
        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a3, b1", "a2, b3", "a1, b1", "a1, b2", "a1, b3"), result);

        query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo), upper(a.foo2) DESC";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a1, b3", "a1, b2", "a1, b1", "a2, b3", "a3, b1"), result);

    }

    /*
     * Test order by func(a),b
     * order by b,func(a)
     * order by func(a) DESC,b
     * order by func(a),b DESC
     * where both b and func(a) have ordered=true
     */
    @Test
    public void testOrdering5() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:unstructured");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        Tree upper = TestUtil.enableFunctionIndex(props, "upper([foo])");
        upper.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        Tree upper2 = TestUtil.enablePropertyIndex(props, "foo2", false);
        upper2.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree a = test.addChild("n1");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b2");

        a = test.addChild("n2");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a2");
        a.setProperty("foo2", "b3");

        a = test.addChild("n3");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a3");
        a.setProperty("foo2", "b1");

        a = test.addChild("n4");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b3");

        a = test.addChild("n5");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b1");

        String query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo),a.foo2";

        root.commit();
        postCommitHook();

        List<String> result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a1, b1", "a1, b2", "a1, b3", "a2, b3", "a3, b1"), result);

        query = "select a.[foo2],a.[foo]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by a.foo2,upper(a.foo)";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("b1, a1", "b1, a3", "b2, a1", "b3, a1", "b3, a2"), result);

        query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo) DESC, a.foo2";
        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a3, b1", "a2, b3", "a1, b1", "a1, b2", "a1, b3"), result);

        query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo), a.foo2 DESC";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a1, b3", "a1, b2", "a1, b1", "a2, b3", "a3, b1"), result);

    }

    /*
     * Test order by func(a),b
     * orrder by b,func(a)
     * order by func(a) DESC,b
     * order by func(a),b DESC
     * where func(a) does not have ordered = true
     */
    @Test
    public void testOrdering6() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:unstructured");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        TestUtil.enableFunctionIndex(props, "upper([foo])");

        Tree upper2 = TestUtil.enablePropertyIndex(props, "foo2", false);
        upper2.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree a = test.addChild("n1");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b2");

        a = test.addChild("n2");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a2");
        a.setProperty("foo2", "b3");

        a = test.addChild("n3");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a3");
        a.setProperty("foo2", "b1");

        a = test.addChild("n4");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b3");

        a = test.addChild("n5");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "a1");
        a.setProperty("foo2", "b1");

        String query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo),a.foo2";

        root.commit();
        postCommitHook();

        List<String> result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a1, b1", "a1, b2", "a1, b3", "a2, b3", "a3, b1"), result);

        query = "select a.[foo2],a.[foo]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by a.foo2,upper(a.foo)";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("b1, a1", "b1, a3", "b2, a1", "b3, a1", "b3, a2"), result);

        query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo) DESC, a.foo2";
        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a3, b1", "a2, b3", "a1, b1", "a1, b2", "a1, b3"), result);

        query = "select a.[foo],a.[foo2]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where a.foo is not null and isdescendantnode(a , '/test') order by upper(a.foo), a.foo2 DESC";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("a1, b3", "a1, b2", "a1, b1", "a2, b3", "a3, b1"), result);
    }

    /*
     * Testing order by for
     * different function implementations
     */
    @Test
    public void testOrdering7() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:unstructured");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        Tree fn = TestUtil.enableFunctionIndex(props, "upper([foo])");
        fn.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        fn = TestUtil.enableFunctionIndex(props, "lower([foo])");
        fn.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        fn = TestUtil.enableFunctionIndex(props, "length([foo])");
        fn.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        // Any function property trying to sory by length needs to explicitly set the
        // type to Long
        fn.setProperty(FulltextIndexConstants.PROP_TYPE, "Long");

        fn = TestUtil.enableFunctionIndex(props, "coalesce([foo2],[foo])");
        fn.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        fn = TestUtil.enableFunctionIndex(props, "name()");
        fn.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        fn = TestUtil.enableFunctionIndex(props, "localname()");
        fn.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        fn = TestUtil.enableFunctionIndex(props, "lower(coalesce([foo2], coalesce([foo], localname())))");
        fn.setProperty(FulltextIndexConstants.PROP_ORDERED, true);

        fn = TestUtil.enableFunctionIndex(props, "length(coalesce([foo], coalesce([foo2], localname())))");
        fn.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        fn.setProperty(FulltextIndexConstants.PROP_TYPE, "Long");

        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree a = test.addChild("d1");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "c");

        a = test.addChild("d2");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "bbbb");
        a.setProperty("foo2", "22");

        a = test.addChild("d3");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "aa");

        a = test.addChild("jcr:content");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("foo", "test");
        a.setProperty("foo2", "11");

        root.commit();
        postCommitHook();

        String query = "select [jcr:path]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where  isdescendantnode(a , '/test') order by coalesce([foo2],[foo]) ";

        List<String> result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("/test/jcr:content", "/test/d2", "/test/d3", "/test/d1"), result);

        query = "select a.[foo]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where  isdescendantnode(a , '/test') order by lower([a].[foo])";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("aa", "bbbb", "c", "test"), result);

        query = "select [jcr:path]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where  isdescendantnode(a , '/test') order by localname() ";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("/test/jcr:content", "/test/d1", "/test/d2", "/test/d3"), result);

        query = "select [jcr:path]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where  isdescendantnode(a , '/test') order by name() ";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("/test/d1", "/test/d2", "/test/d3", "/test/jcr:content"), result);

        query = "select [jcr:path]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where  isdescendantnode(a , '/test') order by lower(coalesce([a].[foo2], coalesce([a].[foo], localname())))";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("/test/jcr:content", "/test/d2", "/test/d3", "/test/d1"), result);

        query = "select [jcr:path]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where  isdescendantnode(a , '/test') order by lower(coalesce([a].[foo2], coalesce([a].[foo], localname()))) DESC";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("/test/d1", "/test/d3", "/test/d2", "/test/jcr:content"), result);

        query = "select [jcr:path]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where  a.[foo] is not null AND isdescendantnode(a , '/test') order by length([a].[foo]) DESC, localname()";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("/test/jcr:content", "/test/d2", "/test/d3", "/test/d1"), result);

        query = "select [jcr:path]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where  a.[foo] is not null AND isdescendantnode(a , '/test') order by length([a].[foo]), localname()";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("/test/d1", "/test/d3", "/test/jcr:content", "/test/d2"), result);

        query = "select [jcr:path]\n" +
                "\t  from [nt:unstructured] as a\n" +
                "\t  where  a.[foo] is not null AND isdescendantnode(a , '/test') order by length(coalesce([foo], coalesce([foo2], localname()))), localname() DESC";

        result = executeQuery(query, SQL2);

        assertEquals("Ordering doesn't match", asList("/test/d1", "/test/d3", "/test/d2", "/test/jcr:content"), result);

    }

    @Test
    public void testOrdering() throws Exception {
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree nonFunc = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("foo");
        nonFunc.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        nonFunc.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        nonFunc.setProperty("name", "foo");

        Tree func = luceneIndex.getChild(FulltextIndexConstants.INDEX_RULES)
                .getChild("nt:base")
                .getChild(FulltextIndexConstants.PROP_NODE)
                .addChild("fooUpper");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(@foo)");
        func.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);

        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        List<String> paths = Lists.newArrayList();
        for (int idx = 0; idx < 10; idx++) {
            paths.add("/test/n" + idx);
            if (idx % 2 == 0)
                continue;
            Tree a = test.addChild("n" + idx);
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            a.setProperty("foo", "bar" + idx);

        }
        for (int idx = 0; idx < 10; idx++) {
            if (idx % 2 != 0)
                continue;
            Tree a = test.addChild("n" + idx);
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            a.setProperty("foo", "bar" + idx);
        }
        root.commit();
        postCommitHook();

        String query = "/jcr:root//element(*, nt:unstructured) [jcr:like(fn:upper-case(@foo),'BAR%')] order by foo";
        assertThat(explainXpath(query), containsString(getIndexProvider() + "upper"));
        List<String> result = assertQuery(query, "xpath", paths);
        assertEquals("Ordering doesn't match", paths, result);

        query = "/jcr:root//element(*, nt:unstructured) [jcr:like(fn:upper-case(@foo),'BAR%')] order by fn:upper-case(@foo)";
        assertThat(explainXpath(query), containsString(getIndexProvider() + "upper"));
        List<String> result2 = assertQuery(query, "xpath", paths);
        assertEquals("Ordering doesn't match", paths, result2);
    }

    @Test
    public void upperCaseRelative() throws Exception {
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("upperName");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "upper([data/name])");

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        List<String> paths = Lists.newArrayList();
        for (int idx = 0; idx < 15; idx++) {
            Tree a = test.addChild("n" + idx);
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            Tree b = a.addChild("data");
            b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            b.setProperty("name", "foo");
            paths.add("/test/n" + idx);
        }
        root.commit();

        String query = "select [jcr:path] from [nt:unstructured] where upper([data/name]) = 'FOO'";
        assertThat(explain(query), containsString(getIndexProvider() + "upper"));
        assertQuery(query, paths);

        String queryXPath = "/jcr:root//element(*, nt:unstructured)[fn:upper-case(data/@name) = 'FOO']";
        assertThat(explainXpath(queryXPath), containsString(getIndexProvider() + "upper"));
        assertQuery(queryXPath, "xpath", paths);

        for (int idx = 0; idx < 15; idx++) {
            Tree a = test.getChild("n" + idx);
            Tree b = a.getChild("data");
            b.setProperty("name", "bar");
        }
        root.commit();
        postCommitHook();

        query = "select [jcr:path] from [nt:unstructured] where upper([data/name]) = 'BAR'";
        assertThat(explain(query), containsString(getIndexProvider() + "upper"));
        assertQuery(query, paths);

        queryXPath = "/jcr:root//element(*, nt:unstructured)[fn:upper-case(data/@name) = 'BAR']";
        assertThat(explainXpath(queryXPath), containsString(getIndexProvider() + "upper"));
        assertQuery(queryXPath, "xpath", paths);
    }

    @Test
    public void coalesceOrdering() throws Exception {

        IndexDefinitionBuilder idxb = indexOptions.createIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo", null).function(
                "coalesce([jcr:content/foo2], [jcr:content/foo])").ordered();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").addChild("jcr:content").setProperty("foo2", "a");
        rootTree.addChild("b").addChild("jcr:content").setProperty("foo", "b");
        Tree child = rootTree.addChild("c").addChild("jcr:content");
        child.setProperty("foo", "c");
        child.setProperty("foo2", "a1");

        root.commit();
        postCommitHook();

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by coalesce([jcr:content/foo2], [jcr:content/foo])",
                getIndexProvider() + "test1(/oak:index/test1)", asList("/a", "/c", "/b"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by coalesce([jcr:content/foo2], [jcr:content/foo]) DESC",
                getIndexProvider() + "test1(/oak:index/test1)", asList("/b", "/c", "/a"));
    }

    @Test
    public void coalesce() throws Exception {
        IndexDefinitionBuilder idxb = indexOptions.createIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo", null).function(
                "lower(coalesce([jcr:content/foo2], coalesce([jcr:content/foo], localname())))");

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").addChild("jcr:content").setProperty("foo2", "BAR");
        rootTree.addChild("b").addChild("jcr:content").setProperty("foo", "bAr");
        Tree child = rootTree.addChild("c").addChild("jcr:content");
        child.setProperty("foo", "bar");
        child.setProperty("foo2", "bar1");
        rootTree.addChild("bar");

        root.commit();
        postCommitHook();

        assertPlanAndQuery(
                "select * from [nt:base] where lower(coalesce([jcr:content/foo2], coalesce([jcr:content/foo], localname()))) = 'bar'",
                getIndexProvider() + "test1(/oak:index/test1)", asList("/a", "/b", "/bar"));
    }

    /*
     * Given an index def with 2 orderable property definitions(Relative) for same
     * property - one with function and one without
     * Order by should give correct results
     */
    @Test
    public void sameOrderableRelPropWithAndWithoutFunc_checkOrdering() throws Exception {

        // Index def with same property - ordered - one with function and one without
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree nonFunc = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("foo");
        nonFunc.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        nonFunc.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        nonFunc.setProperty("name", "jcr:content/n/foo");

        Tree func = luceneIndex.getChild(FulltextIndexConstants.INDEX_RULES)
                .getChild("nt:base")
                .getChild(FulltextIndexConstants.PROP_NODE)
                .addChild("testOak");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(jcr:content/n/@foo)");

        root.commit();

        int i = 1;
        // Create nodes that will be served by the index definition that follows
        for (String node : asList("a", "c", "b", "e", "d")) {

            Tree test = root.getTree("/").addChild(node);
            test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            Tree a = test.addChild("jcr:content");
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            Tree b = a.addChild("n");

            b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            b.setProperty("foo", "bar" + i);
            i++;
        }

        root.commit();
        // pure paranoia, this test seems to be more suceptable to delays in indexng
        // than others
        for (int j = 0; j < 5; j++) {
            postCommitHook();
        }

        // Check ordering works for func and non func properties
        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo])",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo]",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo]) DESC",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo] DESC",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

        // Now we change the value of foo on already indexed nodes and see if changes
        // get indexed properly.

        i = 5;
        for (String node : asList("a", "c", "b", "e", "d")) {

            Tree test = root.getTree("/").getChild(node).getChild("jcr:content").getChild("n");

            test.setProperty("foo", "bar" + i);
            i--;
        }
        root.commit();
        // pure paranoia, this test seems to be more suceptable to delays in indexng
        // than others
        for (int j = 0; j < 5; j++) {
            postCommitHook();
        }

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo])",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo]",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo]) DESC",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo] DESC",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

    }

    /*
     * Given an index def with 2 orderable property definitions(non-relative) for
     * same property - one with function and one without
     * Indexer should index any changes properly and ordering should work as
     * expected.
     */
    @Test
    public void sameOrderablePropertyWithandWithoutFunction() throws Exception {
        LogCustomizer customLogs = LogCustomizer.forLogger(getLoggerName()).enable(org.slf4j.event.Level.WARN).create();
        // Create nodes that will be served by the index definition that follows
        int i = 1;
        // Create nodes that will be served by the index definition that follows
        for (String node : asList("a", "c", "b", "e", "d")) {

            Tree test = root.getTree("/").addChild(node);
            test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            test.setProperty("foo", "bar" + i);
            i++;
        }

        root.commit();

        // Index def with same property - ordered - one with function and one without
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree nonFunc = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("foo");
        nonFunc.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        nonFunc.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        nonFunc.setProperty("name", "foo");

        Tree func = luceneIndex.getChild(FulltextIndexConstants.INDEX_RULES)
                .getChild("nt:base")
                .getChild(FulltextIndexConstants.PROP_NODE)
                .addChild("testOak");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(@foo)");

        // Now do some change in the node that are covered by above index definition
        try {
            customLogs.starting();
            i = 5;
            for (String node : asList("a", "c", "b", "e", "d")) {

                Tree test = root.getTree("/").addChild(node);
                test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

                test.setProperty("foo", "bar" + i);
                i--;
            }

            root.commit();
            postCommitHook();
            Assert.assertFalse(customLogs.getLogs().contains("Failed to index the node [/test]"));
            Assert.assertTrue(customLogs.getLogs().size() == 0);

            assertOrderedPlanAndQuery(
                    "select * from [nt:base] order by upper([foo])",
                    getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

            assertOrderedPlanAndQuery(
                    "select * from [nt:base] order by [foo]",
                    getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

            assertOrderedPlanAndQuery(
                    "select * from [nt:base] order by upper([foo]) DESC",
                    getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

            assertOrderedPlanAndQuery(
                    "select * from [nt:base] order by [foo] DESC",
                    getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

        } finally {
            customLogs.finished();
        }

    }

    /*
     * <OAK-8166>
     * Given an index def with 2 orderable property definitions(Relative) for same
     * property - one with function and one without
     * Indexer should not fail to index the nodes covered by this index
     */
    @Test
    public void sameOrderableRelativePropertyWithAndWithoutFunction() throws Exception {

        LogCustomizer customLogs = LogCustomizer.forLogger(getLoggerName()).enable(Level.WARN).create();
        // Create nodes that will be served by the index definition that follows
        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree a = test.addChild("jcr:content");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree b = a.addChild("n");

        b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        b.setProperty("foo", "bar");

        root.commit();

        // Index def with same property - ordered - one with function and one without
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree nonFunc = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:unstructured")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("foo");
        nonFunc.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        nonFunc.setProperty("name", "jcr:content/n/foo");

        Tree func = luceneIndex.getChild(FulltextIndexConstants.INDEX_RULES)
                .getChild("nt:unstructured")
                .getChild(FulltextIndexConstants.PROP_NODE)
                .addChild("testOak");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(jcr:content/n/@foo)");

        // Now do some change in the node that are covered by above index definition
        try {
            customLogs.starting();
            root.getTree("/").getChild("test").getChild("jcr:content").getChild("n").setProperty("foo", "bar2");
            root.commit();
            postCommitHook();
            Assert.assertFalse(customLogs.getLogs().contains("Failed to index the node [/test]"));
            Assert.assertTrue(customLogs.getLogs().size() == 0);
        } finally {
            customLogs.finished();
        }

    }

    @Test
    public void duplicateFunctionInIndex() throws Exception {
        // Index def with same property - ordered - one with function and one without
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree prop = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE);
        Tree upper1 = prop.addChild("upper1");
        upper1.setProperty(FulltextIndexConstants.PROP_ORDERED,true);
        upper1.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(jcr:content/n/@foo)");
        Tree upper2 = prop.addChild("upper2");
        upper2.setProperty(FulltextIndexConstants.PROP_ORDERED,true);
        upper2.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(jcr:content/n/@foo)");
        Tree upper3 = prop.addChild("upper3");
        upper3.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(jcr:content/n/@foo)");
        Tree upper4 = prop.addChild("upper4");
        upper4.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(jcr:content/n/@foo)");

        root.commit();

        int i = 1;
        // Create nodes that will be served by the index definition that follows
        for (String node : asList("a", "c", "b", "e", "d")) {

            Tree test = root.getTree("/").addChild(node);
            test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            Tree a = test.addChild("jcr:content");
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            Tree b = a.addChild("n");

            b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            b.setProperty("foo", "bar"+i);
            i++;
        }

        root.commit();
        postCommitHook();

        // Check ordering works for func and non func properties
        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo])",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/a","/c","/b","/e","/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo]) DESC",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/d","/e","/b","/c","/a"));

    }

    protected String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    protected String explainXpath(String query) throws ParseException {
        String explain = "explain " + query;
        Result result = executeQuery(explain, "xpath", NO_BINDINGS);
        ResultRow row = Iterables.getOnlyElement(result.getRows());
        String plan = row.getValue("plan").getValue(Type.STRING);
        return plan;
    }

    private void assertOrderedPlanAndQuery(String query, String planExpectation, List<String> paths) {
        List<String> result = assertPlanAndQuery(query, planExpectation, paths);
        assertEquals("Ordering doesn't match", paths, result);
    }

    private List<String> assertPlanAndQuery(String query, String planExpectation, List<String> paths) {
        assertThat(explain(query), containsString(planExpectation));
        return assertQuery(query, paths);
    }

    protected Tree createIndex(String name, Set<String> propNames) {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    abstract protected Tree createIndex(Tree index, String name, Set<String> propNames);

    abstract protected String getLoggerName();
}
