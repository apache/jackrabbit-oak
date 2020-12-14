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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static javax.jcr.PropertyType.TYPENAME_DATE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class PropertyIndexCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtils.assertEventually(r,
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
        indexOptions.setIndex(root, "test1", indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propa", "propb"));
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

    private String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }
}
