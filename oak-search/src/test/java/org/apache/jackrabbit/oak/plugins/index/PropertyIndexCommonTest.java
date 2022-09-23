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

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static javax.jcr.PropertyType.TYPENAME_DATE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NOT_NULL_CHECK_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NULL_CHECK_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_PROPERTY_INDEX;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class PropertyIndexCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Test
    public void testBulkProcessorFlushLimit() throws Exception {
        indexOptions.setIndex(root, "test1", indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propa"));

        Tree test = root.getTree("/").addChild("test");
        for (int i = 1; i < 249; i++) {
            test.addChild("a" + i).setProperty("propa", "foo" + i);
        }
        root.commit();

        // 250 is the default flush limit for bulk processor, and we added just less than 250 nodes
        // So once the index writer is closed , bulk Processor would be closed and all the 248 entries should be flushed.
        // Make sure that the last entry is indexed correctly.
        String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo248'";
        assertEventually(() -> {
            assertThat(explain(propaQuery), containsString(indexOptions.getIndexType() + ":test1"));

            assertQuery(propaQuery, singletonList("/test/a248"));
        });

        // Now we test for 250 < nodes < 500

        for (int i = 250; i < 300; i++) {
            test.addChild("a" + i).setProperty("propa", "foo" + i);
        }
        root.commit();
        String propaQuery2 = "select [jcr:path] from [nt:base] where [propa] = 'foo299'";
        assertEventually(() -> {
            assertThat(explain(propaQuery2), containsString(indexOptions.getIndexType() + ":test1"));

            assertQuery(propaQuery2, singletonList("/test/a299"));
        });
    }

    @Test
    public void indexSelection() throws Exception {
        indexOptions.setIndex(root, "test1", indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propa", "propb"));
        indexOptions.setIndex(root, "test2", indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propc"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "foo");
        test.addChild("c").setProperty("propa", "foo2");
        test.addChild("d").setProperty("propc", "foo");
        test.addChild("e").setProperty("propd", "foo");
        root.commit();

        String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo'";

        assertEventually(() -> {
            IndexDefinitionBuilder builder = indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false);
            builder.includedPaths("/test")
                    .indexRule("nt:base")
                    .property("nodeName", PROPDEF_PROP_NODE_NAME);
            indexOptions.setIndex(root, "test1", builder);
            assertThat(explain(propaQuery), containsString(indexOptions.getIndexType() + ":test1"));
            assertThat(explain("select [jcr:path] from [nt:base] where [propc] = 'foo'"),
                    containsString(indexOptions.getIndexType() + ":test2"));

            assertQuery(propaQuery, Arrays.asList("/test/a", "/test/b"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 'foo2'", singletonList("/test/c"));
            assertQuery("select [jcr:path] from [nt:base] where [propc] = 'foo'", singletonList("/test/d"));
        });
    }

    //OAK-3825
    @Test
    public void nodeNameViaPropDefinition() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.noAsync();
        builder.includedPaths("/test")
                .indexRule("nt:base")
                .property("nodeName", PROPDEF_PROP_NODE_NAME);
        indexOptions.setIndex(root, "test1", builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");
        test.addChild("foo");
        test.addChild("camelCase");
        test.addChild("sc").addChild("bar");
        root.commit();

        String queryPrefix = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/test') AND ";
        //test
        String propabQuery = queryPrefix + "LOCALNAME() = 'foo'";

        assertEventually(() -> {
            String explanation = explain(propabQuery);
            assertThat(explanation, containsString(indexOptions.getIndexType() + ":test1(/oak:index/test1) "));
            //assertThat(explanation, containsString("{\"term\":{\":nodeName\":{\"value\":\"foo\","));
            assertQuery(propabQuery, singletonList("/test/foo"));

            assertQuery(queryPrefix + "LOCALNAME() = 'bar'", singletonList("/test/sc/bar"));
            assertQuery(queryPrefix + "LOCALNAME() LIKE 'foo'", singletonList("/test/foo"));
            assertQuery(queryPrefix + "LOCALNAME() LIKE 'camel%'", singletonList("/test/camelCase"));

            assertQuery(queryPrefix + "NAME() = 'bar'", singletonList("/test/sc/bar"));
            assertQuery(queryPrefix + "NAME() LIKE 'foo'", singletonList("/test/foo"));
            assertQuery(queryPrefix + "NAME() LIKE 'camel%'", singletonList("/test/camelCase"));
        });
    }

    @Test
    public void emptyIndex() throws Exception {
        indexOptions.setIndex(root, "test1", indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propa", "propb"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();
        assertEventually(() -> assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 'foo'"),
                containsString(indexOptions.getIndexType() + ":test1")));
    }

    @Test
    public void propertyExistenceQuery() throws Exception {
        indexOptions.setIndex(root, "test1", indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(),
                false, "propa", "propb"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        root.commit();
        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where propa is not null",
                Arrays.asList("/test/a", "/test/b")));
    }

    @Test
    public void propertyExistenceQueryWithNullCheck() throws Exception {
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");

        Tree idx = indexOptions.setIndex(root, "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), TestUtil.NT_TEST, false, "propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(PROP_NAME, "propa");
        prop.setProperty(PROP_PROPERTY_INDEX, true);
        prop.setProperty(PROP_NOT_NULL_CHECK_ENABLED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithType(test, "a", "oak:TestNode").setProperty("propa", "a");
        createNodeWithType(test, "b", "oak:TestNode").setProperty("propa", "c");
        createNodeWithType(test, "c", "oak:TestNode").setProperty("propb", "e");
        root.commit();

        String query = "select [jcr:path] from [oak:TestNode] where [propa] is not null";
        String explanation = explain(query);
        assertThat(explanation, containsString(propertyExistenceQueryWithNullCheckExpectedExplain()));
        assertEventually(() -> assertQuery(query, Arrays.asList("/test/a", "/test/b")));
    }

    protected String propertyExistenceQueryWithNullCheckExpectedExplain() {
        return indexOptions.getIndexType() + ":test1(/oak:index/test1) ";
    }

    @Test
    public void propertyNonExistenceQuery() throws Exception {
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");

        Tree idx = indexOptions.setIndex(root, "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), TestUtil.NT_TEST, false, "propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(PROP_NAME, "propa");
        prop.setProperty(PROP_PROPERTY_INDEX, true);
        prop.setProperty(PROP_NULL_CHECK_ENABLED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithType(test, "a", "oak:TestNode").setProperty("propa", "a");
        createNodeWithType(test, "b", "oak:TestNode").setProperty("propa", "c");
        createNodeWithType(test, "c", "oak:TestNode").setProperty("propb", "e");
        root.commit();

        String query = "select [jcr:path] from [oak:TestNode] where [propa] is null";
        String explanation = explain(query);
        assertThat(explanation, containsString(propertyNonExistenceQueryExpectedExplain()));
        assertEventually(() -> assertQuery(query, singletonList("/test/c")));
    }

    protected String propertyNonExistenceQueryExpectedExplain() {
        return indexOptions.getIndexType() + ":test1(/oak:index/test1) ";
    }

    @Test
    public void dateQuery() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, indexOptions.getIndexType());
        TestUtil.useV2(indexDefn);

        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:base");
        Tree prop = TestUtil.enablePropertyIndex(props, "date", false);
        prop.setProperty(FulltextIndexConstants.PROP_TYPE, TYPENAME_DATE);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        Tree b = test.addChild("b");
        Tree c = test.addChild("c");
        Tree d = test.addChild("d");
        a.setProperty("date", "2020-12-07T11:45:48.119Z", Type.DATE);
        b.setProperty("date", "2020-12-07T17:23:33.933Z", Type.DATE);
        c.setProperty("date", "2020-12-07T22:23:33.933Z", Type.DATE);
        d.setProperty("date", "2020-12-07T10:23:33.933-09:00", Type.DATE);
        root.commit();

        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where date > CAST('2020-12-06T12:32:35.886Z' AS DATE)",
                Arrays.asList("/test/a", "/test/b", "/test/c", "/test/d")));
        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where date > CAST('2020-12-07T12:32:35.886Z' AS DATE) " +
                        "and date < CAST('2020-12-07T20:32:35.886Z' AS DATE)",
                Arrays.asList("/test/b", "/test/d")));
        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where date < CAST('2020-12-07T11:23:33.933-09:00' AS DATE)",
                Arrays.asList("/test/a", "/test/b", "/test/d")));
    }

    @Test
    public void likeQueriesWithString() throws Exception {
        indexOptions.setIndex(
                root,
                "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propa")
        );

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "humpty");
        test.addChild("b").setProperty("propa", "dumpty");
        test.addChild("c").setProperty("propa", "humpy");
        test.addChild("d").setProperty("propa", "alice");
        root.commit();

        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where propa like 'hum%'",
                ImmutableList.of("/test/a", "/test/c")));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%ty'",
                ImmutableList.of("/test/a", "/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%ump%'",
                ImmutableList.of("/test/a", "/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '_ump%'",
                ImmutableList.of("/test/a", "/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'a_ice%'",
                ImmutableList.of("/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'a_i_e%'",
                ImmutableList.of("/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '_____'",
                ImmutableList.of("/test/c", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'h%y'",
                ImmutableList.of("/test/a", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'humpy'",
                ImmutableList.of("/test/c"));
    }

    @Test
    public void likeQueriesWithEscapedChars() throws Exception {
        indexOptions.setIndex(
                root,
                "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propa")
        );
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo%");
        test.addChild("b").setProperty("propa", "%bar");
        test.addChild("c").setProperty("propa", "foo%bar");
        test.addChild("d").setProperty("propa", "foo_");
        test.addChild("e").setProperty("propa", "_foo");
        test.addChild("f").setProperty("propa", "foo_bar");
        test.addChild("g").setProperty("propa", "foo%_bar");
        test.addChild("h").setProperty("propa", "foo\\bar");
        test.addChild("i").setProperty("propa", "foo\\\\%bar");
        root.commit();

        assertEventually(() ->
                assertQuery("select [jcr:path] from [nt:base] where propa like 'foo%'",
                        ImmutableList.of("/test/a", "/test/c", "/test/d", "/test/f", "/test/g", "/test/h", "/test/i"))
        );

        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo%'",
                ImmutableList.of("/test/a", "/test/c", "/test/d", "/test/e", "/test/f", "/test/g", "/test/h", "/test/i"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'foo\\%'",
                ImmutableList.of("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\%'",
                ImmutableList.of("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\%%'",
                ImmutableList.of("/test/a", "/test/c", "/test/g"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '\\%b%'",
                ImmutableList.of("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'foo_'",
                ImmutableList.of("/test/a", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '_oo_'",
                ImmutableList.of("/test/a", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'foo\\_'",
                ImmutableList.of("/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\_'",
                ImmutableList.of("/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\_%'",
                ImmutableList.of("/test/d", "/test/f"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\%\\_%'",
                ImmutableList.of("/test/g"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'foo\\\\bar'",
                ImmutableList.of("/test/h"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%\\\\%'",
                ImmutableList.of("/test/h", "/test/i"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%\\\\\\%%'",
                ImmutableList.of("/test/i"));
    }

    protected String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    protected static Tree createNodeWithType(Tree t, String nodeName, String typeName) {
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }
}
