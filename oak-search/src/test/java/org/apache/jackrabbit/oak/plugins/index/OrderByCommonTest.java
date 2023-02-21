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
package org.apache.jackrabbit.oak.plugins.index;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import javax.jcr.PropertyType;
import java.text.ParseException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public abstract class OrderByCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Test
    public void withoutOrderByClause() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo", null)
                .analyzed();
        indexOptions.setIndex(root, UUID.randomUUID().toString(), indexOptions.createIndex(builder, false, "foo"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "hello");
        test.addChild("b").setProperty("foo", "hello hello");
        root.commit();

        // results are sorted by score desc, node `b` returns first because it has a higher score from a tf/idf perspective
        assertOrderedQuery("select [jcr:path] from [nt:base] where contains(foo, 'hello')", asList("/test/b", "/test/a"));
    }

    @Test
    public void orderByScore() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo", null)
                .analyzed();
        indexOptions.setIndex(root, UUID.randomUUID().toString(), indexOptions.createIndex(builder, false, "foo"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "hello");
        test.addChild("b").setProperty("foo", "hello hello");
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where contains(foo, 'hello') order by [jcr:score]",
                asList("/test/a", "/test/b"));

        assertOrderedQuery("select [jcr:path] from [nt:base] where contains(foo, 'hello') order by [jcr:score] DESC",
                asList("/test/b", "/test/a"));
    }

    @Test
    public void orderByPath() throws Exception {
        indexOptions.setIndex(root, UUID.randomUUID().toString(), indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "foo"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "bar");
        test.addChild("b").setProperty("foo", "bar");
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'bar' order by [jcr:path]", asList("/test/a", "/test/b"));
        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'bar' order by [jcr:path] DESC", asList("/test/b", "/test/a"));
    }

    @Test
    public void orderByWithRegexProperty() throws Exception {
        Tree idx = indexOptions.setIndex(root, UUID.randomUUID().toString(), indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("foo", "bar");
        a.setProperty("baz", "zzzzzz");
        Tree b = test.addChild("b");
        b.setProperty("foo", "bar");
        b.setProperty("baz", "aaaaaa");
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] as a where foo = 'bar' order by @baz", asList("/test/b", "/test/a"));
        assertOrderedQuery("select [jcr:path] from [nt:base] as a where foo = 'bar' order by @baz DESC", asList("/test/a", "/test/b"));
    }

    @Test
    public void orderByFunctionWithRegexProperty() throws Exception {
        Tree idx = indexOptions.setIndex(root, UUID.randomUUID().toString(), indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);

        Tree test = root.getTree("/").addChild("test");
        test.addChild("A").setProperty("foo", "bar");
        test.addChild("b").setProperty("foo", "bar");
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] as a where foo = 'bar' order by lower(name(a))", asList("/test/A", "/test/b"));
        assertOrderedQuery("select [jcr:path] from [nt:base] as a where foo = 'bar' order by lower(name(a)) DESC", asList("/test/b", "/test/A"));
    }

    @Test
    public void orderByProperty() throws Exception {
        indexOptions.setIndex(root, UUID.randomUUID().toString(), indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "foo", "baz"));

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("foo", "bar");
        a.setProperty("baz", "zzzzzz");
        Tree b = test.addChild("b");
        b.setProperty("foo", "bar");
        b.setProperty("baz", "aaaaaa");
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'bar' order by @baz", asList("/test/b", "/test/a"));
        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'bar' order by @baz DESC", asList("/test/a", "/test/b"));
    }

    @Test
    public void orderByAnalyzedProperty() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("baz", null)
                .analyzed();
        indexOptions.setIndex(root, UUID.randomUUID().toString(), indexOptions.createIndex(builder, false, "foo", "baz"));

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("foo", "bar");
        a.setProperty("baz", "bbbbb");
        Tree b = test.addChild("b");
        b.setProperty("foo", "bar");
        b.setProperty("baz", "hello aaaa");
        root.commit();

        // this test verifies we use the keyword multi field when an analyzed properties is specified in order by
        // http://www.technocratsid.com/string-sorting-in-elasticsearch/

        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'bar' order by @baz", asList("/test/a", "/test/b"));
        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'bar' order by @baz DESC", asList("/test/b", "/test/a"));
    }

    @Test
    public void orderByNumericProperty() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("baz", null).propertyIndex().type(PropertyType.TYPENAME_LONG).ordered();
        indexOptions.setIndex(root, UUID.randomUUID().toString(), indexOptions.createIndex(builder, false, "foo", "baz"));

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("foo", "bar");
        a.setProperty("baz", "10");
        Tree b = test.addChild("b");
        b.setProperty("foo", "bar");
        b.setProperty("baz", "5");
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'bar' order by @baz", asList("/test/b", "/test/a"));
        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'bar' order by @baz DESC", asList("/test/a", "/test/b"));
    }

    @Test
    public void orderByMultiProperties() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("bar", null).propertyIndex().type(PropertyType.TYPENAME_LONG).ordered();
        indexOptions.setIndex(root, UUID.randomUUID().toString(), indexOptions.createIndex(builder, false, "foo", "bar", "baz"));

        Tree test = root.getTree("/").addChild("test");
        Tree a1 = test.addChild("a1");
        a1.setProperty("foo", "foobar");
        a1.setProperty("bar", "1");
        a1.setProperty("baz", "a");
        Tree a2 = test.addChild("a2");
        a2.setProperty("foo", "foobar");
        a2.setProperty("bar", "2");
        a2.setProperty("baz", "a");
        Tree b = test.addChild("b");
        b.setProperty("foo", "foobar");
        b.setProperty("bar", "100");
        b.setProperty("baz", "b");
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'foobar' order by @baz, @bar",
                asList("/test/a1", "/test/a2", "/test/b"));

        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'foobar' order by @baz, @bar DESC",
                asList("/test/a2", "/test/a1", "/test/b"));

        assertOrderedQuery("select [jcr:path] from [nt:base] where foo = 'foobar' order by @bar DESC, @baz DESC",
                asList("/test/b", "/test/a2", "/test/a1"));
    }

    @Test
    public void sortOnNodeName() throws Exception {
        // setup function index on "fn:name()"
        IndexDefinitionBuilder idbFnName = indexOptions.createIndexDefinitionBuilder();
        idbFnName.indexRule("nt:base")
                .property("nodeName", null)
                .propertyIndex()
                .ordered()
                .function("fn:name()");
        idbFnName.getBuilderTree().setProperty("tags", of("fnName"), STRINGS);
        indexOptions.setIndex(root, "fnName", indexOptions.createIndex(idbFnName, false));

        // same index as above except for function - "name()"
        IndexDefinitionBuilder idbName = indexOptions.createIndexDefinitionBuilder();
        idbName.indexRule("nt:base")
                .property("nodeName", null)
                .propertyIndex()
                .ordered()
                .function("name()");
        idbName.getBuilderTree().setProperty("tags", of("name"), STRINGS);
        indexOptions.setIndex(root, "name", indexOptions.createIndex(idbName, false));

        Tree testTree = root.getTree("/").addChild("test");
        List<String> expected = IntStream.range(0, 3).mapToObj(i -> {
            String nodeName = "a" + i;
            testTree.addChild(nodeName);
            return "/test/" + nodeName;
        }).sorted().collect(Collectors.toList());
        root.commit();

        String query = "/jcr:root/test/* order by fn:name() option(index tag fnName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnName(/oak:index/fnName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:name() ascending option(index tag fnName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnName(/oak:index/fnName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:name() descending option(index tag fnName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnName(/oak:index/fnName)");
        assertEquals(Lists.reverse(expected), executeQuery(query, XPATH));

        // order by fn:name() although function index is on "name()"
        query = "/jcr:root/test/* order by fn:name() option(index tag name)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":name(/oak:index/name)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:name() ascending option(index tag name)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":name(/oak:index/name)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:name() descending option(index tag name)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":name(/oak:index/name)");
        assertEquals(Lists.reverse(expected), executeQuery(query, XPATH));
    }

    @Test
    public void sortOnLocalName() throws Exception {
        // setup function index on "fn:local-name()"
        IndexDefinitionBuilder idbFnLocalName = indexOptions.createIndexDefinitionBuilder();
        idbFnLocalName.indexRule("nt:base")
                .property("nodeName", null)
                .propertyIndex()
                .ordered()
                .function("fn:local-name()");
        idbFnLocalName.getBuilderTree().setProperty("tags", of("fnLocalName"), STRINGS);
        indexOptions.setIndex(root, "fnLocalName", indexOptions.createIndex(idbFnLocalName, false));

        // same index as above except for function - "localName()"
        IndexDefinitionBuilder idbLocalName = indexOptions.createIndexDefinitionBuilder();
        idbLocalName.indexRule("nt:base")
                .property("nodeName", null)
                .propertyIndex()
                .ordered()
                .function("localname()");
        idbLocalName.getBuilderTree().setProperty("tags", of("localName"), STRINGS);
        indexOptions.setIndex(root, "localName", indexOptions.createIndex(idbLocalName, false));

        Tree testTree = root.getTree("/").addChild("test");
        List<String> expected = IntStream.range(0, 3).mapToObj(i -> {
            String nodeName = "ja" + i; //'j*' should come after (asc) 'index' in sort order
            testTree.addChild(nodeName);
            return "/test/" + nodeName;
        }).sorted((s1, s2) -> { //sort expectation based on local name
            final StringBuffer sb1 = new StringBuffer();
            PathUtils.elements(s1).forEach(elem -> {
                String[] split = elem.split(":", 2);
                sb1.append(split[split.length - 1]);
            });
            s1 = sb1.toString();

            final StringBuffer sb2 = new StringBuffer();
            PathUtils.elements(s2).forEach(elem -> {
                String[] split = elem.split(":", 2);
                sb2.append(split[split.length - 1]);
            });
            s2 = sb2.toString();

            return s1.compareTo(s2);
        }).collect(Collectors.toList());
        root.commit();

        String query = "/jcr:root/test/* order by fn:local-name() option(index tag fnLocalName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnLocalName(/oak:index/fnLocalName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:local-name() ascending option(index tag fnLocalName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnLocalName(/oak:index/fnLocalName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:local-name() descending option(index tag fnLocalName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnLocalName(/oak:index/fnLocalName)");
        assertEquals(Lists.reverse(expected), executeQuery(query, XPATH));

        // order by fn:name() although function index is on "name()"
        query = "/jcr:root/test/* order by fn:local-name() option(index tag localName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":localName(/oak:index/localName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:local-name() ascending option(index tag localName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":localName(/oak:index/localName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:local-name() descending option(index tag localName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":localName(/oak:index/localName)");
        assertEquals(Lists.reverse(expected), executeQuery(query, XPATH));
    }

    @Test
    public void orderByScoreAcrossUnion() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder()
                .evaluatePathRestrictions().noAsync();
        builder.indexRule("nt:base")
                .property("foo").analyzed().propertyIndex().nodeScopeIndex()
                .property("p1").propertyIndex()
                .property("p2").propertyIndex();
        indexOptions.setIndex(root, "test-index", indexOptions.createIndex(builder, false));

        Tree testRoot = root.getTree("/").addChild("test");
        Tree path1 = testRoot.addChild("path1");
        Tree path2 = testRoot.addChild("path2");

        Tree c1 = path1.addChild("c1");
        c1.setProperty("foo", "bar. some extra stuff");
        c1.setProperty("p1", "d");

        Tree c2 = path2.addChild("c2");
        c2.setProperty("foo", "bar");
        c2.setProperty("p2", "d");

        Tree c3 = path2.addChild("c3");
        c3.setProperty("foo", "bar. some extra stuff... and then some to make it worse than c1");
        c3.setProperty("p2", "d");

        // create more stuff to get num_docs in index large and hence force union optimization
        for (int i = 0; i < 10; i++) {
            testRoot.addChild("extra" + i).setProperty("foo", "stuff");
        }

        root.commit();

        List<String> expected = asList(c2.getPath(), c1.getPath(), c3.getPath());

        // manual union
        String query =
                "select [jcr:path] from [nt:base] where contains(*, 'bar') and isdescendantnode('" + path1.getPath() + "')" +
                        " union " +
                        "select [jcr:path] from [nt:base] where contains(*, 'bar') and isdescendantnode('" + path2.getPath() + "')" +
                        " order by [jcr:score] desc";

        assertEquals(expected, executeQuery(query, SQL2));

        // no union (estimated fulltext without union would be same as sub-queries and it won't be optimized
        query = "select [jcr:path] from [nt:base] where contains(*, 'bar')" +
                " and (isdescendantnode('" + path1.getPath() + "') or" +
                " isdescendantnode('" + path2.getPath() + "'))" +
                " order by [jcr:score] desc";

        assertEquals(expected, executeQuery(query, SQL2));

        // optimization UNION as we're adding constraints to sub-queries that would improve cost of optimized union
        query = "select [jcr:path] from [nt:base] where contains(*, 'bar')" +
                " and ( (p1 = 'd' and isdescendantnode('" + path1.getPath() + "')) or" +
                " (p2 = 'd' and isdescendantnode('" + path2.getPath() + "')))" +
                " order by [jcr:score] desc";

        assertEquals(expected, executeQuery(query, SQL2));
    }

    private void assertXpathPlan(String query, String planExpectation) throws ParseException {
        assertThat(explainXpath(query), containsString(planExpectation));
    }

    private String explainXpath(String query) throws ParseException {
        String explain = "explain " + query;
        Result result = executeQuery(explain, "xpath", NO_BINDINGS);
        ResultRow row = Iterables.getOnlyElement(result.getRows());
        return row.getValue("plan").getValue(Type.STRING);
    }

    protected void assertOrderedQuery(String sql, List<String> paths) {
        List<String> result = executeQuery(sql, AbstractQueryTest.SQL2, true, true);
        assertEquals(paths, result);
    }

}
