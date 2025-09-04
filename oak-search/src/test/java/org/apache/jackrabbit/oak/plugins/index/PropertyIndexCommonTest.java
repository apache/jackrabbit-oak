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

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;

import javax.jcr.PropertyType;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import static javax.jcr.PropertyType.TYPENAME_DATE;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NOT_NULL_CHECK_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NULL_CHECK_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_PROPERTY_INDEX;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
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
            assertThat(explain(propaQuery), containsString("/oak:index/test1"));

            assertQuery(propaQuery, List.of("/test/a248"));
        });

        // Now we test for 250 < nodes < 500

        for (int i = 250; i < 300; i++) {
            test.addChild("a" + i).setProperty("propa", "foo" + i);
        }
        root.commit();
        String propaQuery2 = "select [jcr:path] from [nt:base] where [propa] = 'foo299'";
        assertEventually(() -> {
            assertThat(explain(propaQuery2), containsString("/oak:index/test1"));

            assertQuery(propaQuery2, List.of("/test/a299"));
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
            assertThat(explain(propaQuery), containsString("/oak:index/test1"));
            assertThat(explain("select [jcr:path] from [nt:base] where [propc] = 'foo'"),
                    containsString("/oak:index/test2"));

            assertQuery(propaQuery, List.of("/test/a", "/test/b"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 'foo2'", List.of("/test/c"));
            assertQuery("select [jcr:path] from [nt:base] where [propc] = 'foo'", List.of("/test/d"));
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
            assertThat(explanation, containsString("/oak:index/test1"));
            //assertThat(explanation, containsString("{\"term\":{\":nodeName\":{\"value\":\"foo\","));
            assertQuery(propabQuery, List.of("/test/foo"));

            assertQuery(queryPrefix + "LOCALNAME() = 'bar'", List.of("/test/sc/bar"));
            assertQuery(queryPrefix + "LOCALNAME() LIKE 'foo'", List.of("/test/foo"));
            assertQuery(queryPrefix + "LOCALNAME() LIKE 'camel%'", List.of("/test/camelCase"));

            assertQuery(queryPrefix + "NAME() = 'bar'", List.of("/test/sc/bar"));
            assertQuery(queryPrefix + "NAME() LIKE 'foo'", List.of("/test/foo"));
            assertQuery(queryPrefix + "NAME() LIKE 'camel%'", List.of("/test/camelCase"));
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
                containsString("/oak:index/test1")));
    }

    @Test
    public void sizeQuery() throws Exception {
        indexOptions.setIndex(root, "test1", indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propa"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "bar");
        root.commit();

        assertEventually(() -> {
            try {
                Result result = executeQuery("select [jcr:path] from [nt:base] where [propa] = 'foo'", SQL2, NO_BINDINGS);
                assertThat(result.getSize(Result.SizePrecision.APPROXIMATION, 0), is(1L));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });

        // this verifies OAK-10782 is fixed
        test.addChild("c").setProperty("propa", "foo");
        test.addChild("d").setProperty("propb", "bar");
        root.commit();

        assertEventually(() -> {
            try {
                Result result = executeQuery("select [jcr:path] from [nt:base] where [propa] = 'foo'", SQL2, NO_BINDINGS);
                assertThat(result.getSize(Result.SizePrecision.APPROXIMATION, 0), is(2L));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });
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
                List.of("/test/a", "/test/b")));
    }

    @Test
    public void propertyExistenceQueryWithNullCheck() throws Exception {
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE, StandardCharsets.UTF_8), "test nodeType");

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
        assertEventually(() -> assertQuery(query, List.of("/test/a", "/test/b")));
    }

    protected String propertyExistenceQueryWithNullCheckExpectedExplain() {
        return "/oak:index/test1";
    }

    @Test
    public void propertyNonExistenceQuery() throws Exception {
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE, StandardCharsets.UTF_8), "test nodeType");

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
        assertEventually(() -> assertQuery(query, List.of("/test/c")));
    }

    protected String propertyNonExistenceQueryExpectedExplain() {
        return "/oak:index/test1";
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
                List.of("/test/a", "/test/b", "/test/c", "/test/d")));
        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where date > CAST('2020-12-07T12:32:35.886Z' AS DATE) " +
                        "and date < CAST('2020-12-07T20:32:35.886Z' AS DATE)",
                List.of("/test/b", "/test/d")));
        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where date < CAST('2020-12-07T11:23:33.933-09:00' AS DATE)",
                List.of("/test/a", "/test/b", "/test/d")));
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
                List.of("/test/a", "/test/c")));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%ty'",
                List.of("/test/a", "/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%ump%'",
                List.of("/test/a", "/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '_ump%'",
                List.of("/test/a", "/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'a_ice%'",
                List.of("/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'a_i_e%'",
                List.of("/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '_____'",
                List.of("/test/c", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'h%y'",
                List.of("/test/a", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'humpy'",
                List.of("/test/c"));
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
                        List.of("/test/a", "/test/c", "/test/d", "/test/f", "/test/g", "/test/h", "/test/i"))
        );

        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo%'",
                List.of("/test/a", "/test/c", "/test/d", "/test/e", "/test/f", "/test/g", "/test/h", "/test/i"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'foo\\%'",
                List.of("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\%'",
                List.of("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\%%'",
                List.of("/test/a", "/test/c", "/test/g"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '\\%b%'",
                List.of("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'foo_'",
                List.of("/test/a", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '_oo_'",
                List.of("/test/a", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'foo\\_'",
                List.of("/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\_'",
                List.of("/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\_%'",
                List.of("/test/d", "/test/f"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%oo\\%\\_%'",
                List.of("/test/g"));
        assertQuery("select [jcr:path] from [nt:base] where propa like 'foo\\\\bar'",
                List.of("/test/h"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%\\\\%'",
                List.of("/test/h", "/test/i"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%\\\\\\%%'",
                List.of("/test/i"));
    }

    @Test
    public void rangeQueriesWithBeforeEpoch() throws Exception {
        Tree idx = indexOptions.setIndex(
                root,
                "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propa")
        );
        Tree propIdx = idx.getChild("indexRules").getChild("nt:base").getChild(PROP_NODE).getChild("propa");
        propIdx.setProperty(FulltextIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", createCal("14/02/1768"));
        test.addChild("b").setProperty("propa", createCal("14/03/1769"));
        test.addChild("c").setProperty("propa", createCal("14/04/1770"));
        root.commit();

        assertEventually(() -> {
            assertQuery("select [jcr:path] from [nt:base] where [propa] >= " + dt("15/02/1768"), List.of("/test/b", "/test/c"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] <=" + dt("15/03/1769"), List.of("/test/b", "/test/a"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] < " + dt("14/03/1769"), List.of("/test/a"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] <> " + dt("14/03/1769"), List.of("/test/a", "/test/c"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] > " + dt("15/02/1768") + " and [propa] < " + dt("13/04/1770"), List.of("/test/b"));
            assertQuery("select [jcr:path] from [nt:base] where propa is not null", List.of("/test/a", "/test/b", "/test/c"));
        });
    }

    @Test
    public void dateQueryWithEmptyValue() throws Exception {
        Tree idx = indexOptions.setIndex(
                root,
                "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "textField", "imageLaunchDate")
        );
        Tree aggregates = idx.addChild("aggregates").addChild("nt:base");
        Tree include0 = aggregates.addChild("include0");
        include0.setProperty("path", "jcr:content/metadata/product", Type.STRING);

        Tree dateField = idx.getChild("indexRules").getChild("nt:base").getChild(PROP_NODE).getChild("imageLaunchDate");
        dateField.setProperty("name", "jcr:content/metadata/product/imageLaunchDate");
        dateField.setProperty(FulltextIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        dateField.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("textField", "foo");
        Tree content = a.addChild("jcr:content").addChild("metadata").addChild("product");
        content.setProperty("imageLaunchDate", "", Type.STRING);
        root.commit();

        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where [textField] = 'foo'", List.of("/test/a")));
    }

    @Test
    public void inQueryWithUnparseableValue() throws Exception {
        Tree idx = indexOptions.setIndex(
                root,
                "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "booleanField")
        );
        Tree booleanField = idx.getChild("indexRules").getChild("nt:base").getChild(PROP_NODE).getChild("booleanField");
        booleanField.setProperty(FulltextIndexConstants.PROP_TYPE, PropertyType.TYPENAME_BOOLEAN);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("booleanField", true);
        root.commit();

        assertEventually(() -> {
            assertQuery("select [jcr:path] from [nt:base] where [booleanField] in('true', 'True')", List.of("/test/a"));
            assertQuery("select [jcr:path] from [nt:base] where [booleanField] in('true', 'InvalidBool')", List.of("/test/a"));
            assertQuery("select [jcr:path] from [nt:base] where [booleanField] in('foo', 'InvalidBool')", List.of());
        });
    }

    @Test
    public void indexingBasedOnMixin() throws Exception {
        indexOptions.setIndex(
                root,
                "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), "mix:title", false, "jcr:title")
        );
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithMixinType(test, "a", "mix:title").setProperty("jcr:title", "a");
        createNodeWithMixinType(test, "b", "mix:title").setProperty("jcr:title", "c");
        test.addChild("c").setProperty("jcr:title", "a");
        root.commit();

        String propabQuery = "select [jcr:path] from [mix:title] where [jcr:title] = 'a'";
        assertEventually(() -> {
            assertThat(explain(propabQuery), containsString("/oak:index/test1"));
            assertQuery(propabQuery, List.of("/test/a"));
        });
    }

    @Test
    public void indexingBasedOnMixinWithInheritance() throws Exception {
        indexOptions.setIndex(
                root,
                "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), "mix:mimeType", false, "jcr:mimeType")
        );
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithType(test, "a", "nt:resource").setProperty("jcr:mimeType", "a");
        createNodeWithType(test, "b", "nt:resource").setProperty("jcr:mimeType", "c");
        test.addChild("c").setProperty("jcr:mimeType", "a");
        root.commit();

        String propabQuery = "select [jcr:path] from [mix:mimeType] where [jcr:mimeType] = 'a'";
        assertEventually(() -> {
            assertThat(explain(propabQuery), containsString("/oak:index/test1"));
            assertQuery(propabQuery, List.of("/test/a"));
        });
    }

    @Test
    public void indexingBasedOnMixinAndRelativeProps() throws Exception {
        indexOptions.setIndex(
                root,
                "test1",
                indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), "mix:title", false, "jcr:title", "jcr:content/type")
        );
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        Tree a = createNodeWithMixinType(test, "a", "mix:title");
        a.setProperty("jcr:title", "a");
        a.addChild("jcr:content").setProperty("type", "foo-a");

        Tree c = createNodeWithMixinType(test, "c", "mix:title");
        c.setProperty("jcr:title", "c");
        c.addChild("jcr:content").setProperty("type", "foo-c");

        test.addChild("c").setProperty("jcr:title", "a");
        root.commit();

        String propabQuery = "select [jcr:path] from [mix:title] where [jcr:content/type] = 'foo-a'";
        assertEventually(() -> {
            assertThat(explain(propabQuery), containsString("/oak:index/test1"));
            assertQuery(propabQuery, List.of("/test/a"));
        });
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

    private static Calendar createCal(String dt) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(dt));
        return cal;
    }

    private static String dt(String date) {
        try {
            return String.format("CAST ('%s' AS DATE)", ISO8601.format(createCal(date)));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static Tree createNodeWithMixinType(Tree t, String nodeName, String typeName){
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_MIXINTYPES, List.of(typeName), Type.NAMES);
        return t;
    }
}
