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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import ch.qos.logback.classic.Level;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticPropertyIndexTest extends ElasticAbstractQueryTest {

    @Test
    public void testBulkProcessorEventsFlushLimit() throws Exception {
        setIndex("test1", createIndex("propa"));

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
            assertThat(explain(propaQuery), containsString("elasticsearch:test1"));
            assertQuery(propaQuery, List.of("/test/a248"));
        });

        // Now we test for 250 < nodes < 500

        for (int i = 250; i < 300; i++) {
            test.addChild("a" + i).setProperty("propa", "foo" + i);
        }
        root.commit();
        String propaQuery2 = "select [jcr:path] from [nt:base] where [propa] = 'foo299'";
        assertEventually(() -> {
            assertThat(explain(propaQuery2), containsString("elasticsearch:test1"));
            assertQuery(propaQuery2, List.of("/test/a299"));
        });
    }

    @Test
    public void testBulkProcessorSizeFlushLimit() throws Exception {
        LogCustomizer customLogger = LogCustomizer
                .forLogger(
                        "org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticBulkProcessorHandler")
                .enable(Level.DEBUG).create();
        try {
            customLogger.starting();
        /*
        Below are the conditions to flush data from bulk processor.
        1. Based on events by default 250 events.
        2. Based on size of estimated bulk request size.
        3. When index writer is closed.
        To trigger flush on bulk request size, we will load large documents so that
         instead of event, flush is triggered because of bulk request size.
         */
            setIndex("test1", createIndex("propa", "propb"));
            long bulkSize = ElasticIndexDefinition.BULK_SIZE_BYTES_DEFAULT;
            int docSize = 1024 * 16;
            // +1 at end leads to bulk size breach, leading to two bulkIds.
            long docCountBreachingBulkSize = (bulkSize / docSize) + 1;
            // 250 is the default flush limit for bulk processor
            Assert.assertTrue(docCountBreachingBulkSize < 250);
            String random = RandomStringUtils.random(docSize, true, true);

            Tree test = root.getTree("/").addChild("test");
            for (int i = 1; i <= docCountBreachingBulkSize; i++) {
                test.addChild("a" + i).setProperty("propa", "foo" + i);
                test.addChild("a" + i).setProperty("propb", random + i);
            }
            root.commit();

            String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo" + docCountBreachingBulkSize + "'";
            assertEventually(() -> {
                assertThat(explain(propaQuery), containsString("elasticsearch:test1"));
                assertQuery(propaQuery, List.of("/test/a" + docCountBreachingBulkSize));
            });

            Assert.assertEquals(1, customLogger.getLogs().stream().filter(n -> n.contains("Bulk with id 2 processed with status OK in")).count());
            Assert.assertEquals(0, customLogger.getLogs().stream().filter(n -> n.contains("Bulk with id 3 processed with status OK in")).count());
        } finally {
            customLogger.finished();
        }
    }

    @Test
    public void indexSelection() throws Exception {
        setIndex("test1", createIndex("propa", "propb"));
        setIndex("test2", createIndex("propc"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "foo");
        test.addChild("c").setProperty("propa", "foo2");
        test.addChild("d").setProperty("propc", "foo");
        test.addChild("e").setProperty("propd", "foo");
        root.commit();

        String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo'";

        assertEventually(() -> {
            assertThat(explain(propaQuery), containsString("elasticsearch:test1"));
            assertThat(explain("select [jcr:path] from [nt:base] where [propc] = 'foo'"), containsString("elasticsearch:test2"));

            assertQuery(propaQuery, List.of("/test/a", "/test/b"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 'foo2'", List.of("/test/c"));
            assertQuery("select [jcr:path] from [nt:base] where [propc] = 'foo'", List.of("/test/d"));
        });
    }

    //OAK-3825
    @Test
    public void nodeNameViaPropDefinition() throws Exception {
        //make index
        IndexDefinitionBuilder builder = createIndex();
        builder.includedPaths("/test")
                .indexRule("nt:base")
                .property("nodeName", PROPDEF_PROP_NODE_NAME);
        setIndex("test1", builder);
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
            assertThat(explanation, containsString("elasticsearch:test1(/oak:index/test1) "));
            assertThat(explanation, containsString("{\"term\":{\":nodeName\":{\"value\":\"foo\""));
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
        setIndex("test1", createIndex("propa", "propb"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        assertEventually(() -> assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 'foo'"),
                containsString("elasticsearch:test1")));
    }

    @Test
    public void inOperandStringValues() throws Exception {
        String query = "select [jcr:path] from [nt:base] where [propa] in(\"a\", \"e\", \"i\")";
        createIndexOfType("String");

        Tree test = root.getTree("/").addChild("test");
        for (char ch = 'a'; ch <= 'z'; ch++) {
            test.addChild("node-" + ch).setProperty("propa", Character.toString(ch));
        }
        root.commit();
        assertEventually(() -> {
            assertThat(explain(query), containsString("{\"terms\":{\"propa\":[\"a\",\"e\",\"i\"]}}"));
            assertQuery(query, SQL2, List.of("/test/node-a", "/test/node-e", "/test/node-i"));
        });
    }

    @Test
    public void inOperandLongValues() throws Exception {
        String query = "select [jcr:path] from [nt:base] where [propa] in(2, 3, 5, 7)";
        createIndexOfType("Long");

        Tree test = root.getTree("/").addChild("test");
        for (int i = 0; i < 10; i++) {
            test.addChild("node-" + i).setProperty("propa", Integer.toString(i));
        }
        root.commit();

        assertEventually(() -> {
            assertThat(explain(query), containsString("{\"terms\":{\"propa\":[2,3,5,7]}}"));
            assertQuery(query, SQL2, List.of("/test/node-2", "/test/node-3", "/test/node-5", "/test/node-7"));
        });
    }

    @Test
    public void inOperandDoubleValues() throws Exception {
        String query = "select [jcr:path] from [nt:base] where [propa] in(2.0, 3.0, 5.0, 7.0)";

        createIndexOfType("Double");

        Tree test = root.getTree("/").addChild("test");
        for (int i = 0; i < 10; i++) {
            test.addChild("node-" + i).setProperty("propa", (double) i);
        }
        root.commit();

        assertEventually(() -> {
            assertThat(explain(query), containsString("{\"terms\":{\"propa\":[2.0,3.0,5.0,7.0]}}"));
            assertQuery(query, SQL2, List.of("/test/node-2", "/test/node-3", "/test/node-5", "/test/node-7"));
        });
    }

    @Test
    public void indexFailuresWithFailOnErrorOn() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a");
        builder.includedPaths("/test")
                .indexRule("nt:base")
                .property("nodeName", PROPDEF_PROP_NODE_NAME);

        // configuring the index with a regex property and strict mapping to simulate failures
        builder.indexRule("nt:base").property("b", true).propertyIndex();
        builder.getBuilderTree().setProperty(ElasticIndexDefinition.DYNAMIC_MAPPING, "strict");

        setIndex("test1", builder);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        for (int i = 1; i < 3; i++) {
            test.addChild("a" + i).setProperty("a", "foo");
        }
        root.commit();

        // now we add 5 correct docs and 5 docs cannot be mapped
        test.addChild("a100").setProperty("a", "foo");
        test.addChild("a200").setProperty("b", "foo");
        test.addChild("a101").setProperty("a", "foo");
        test.addChild("a201").setProperty("b", "foo");
        test.addChild("a102").setProperty("a", "foo");
        test.addChild("a202").setProperty("b", "foo");
        test.addChild("a103").setProperty("a", "foo");
        test.addChild("a203").setProperty("b", "foo");
        test.addChild("a104").setProperty("a", "foo");
        test.addChild("a204").setProperty("b", "foo");

        CommitFailedException cfe = null;
        try {
            root.commit();
        } catch (CommitFailedException e) {
            cfe = e;
        }

        assertThat("no exception thrown", cfe != null);
        assertThat("the exception cause has to be an IOException", cfe.getCause() instanceof IOException);
        assertThat("there should be 5 suppressed exception", cfe.getCause().getSuppressed().length == 5);

        String query = "select [jcr:path] from [nt:base] where [a] = 'foo'";
        assertEventually(() -> assertQuery(query, SQL2,
                List.of("/test/a1", "/test/a2", "/test/a100", "/test/a101", "/test/a102", "/test/a103", "/test/a104")
        ));
    }

    @Test
    public void indexFailuresWithFailOnErrorOff() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a");
        builder.includedPaths("/test")
                .indexRule("nt:base")
                .property("nodeName", PROPDEF_PROP_NODE_NAME);

        // configuring the index with a regex property and strict mapping to simulate failures
        builder.indexRule("nt:base").property("b", true).propertyIndex();
        builder.getBuilderTree().setProperty(ElasticIndexDefinition.DYNAMIC_MAPPING, "strict");
        builder.getBuilderTree().setProperty(ElasticIndexDefinition.FAIL_ON_ERROR, false);

        setIndex("test1", builder);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        for (int i = 1; i < 3; i++) {
            test.addChild("a" + i).setProperty("a", "foo");
        }
        root.commit();

        // now we add 5 correct docs and 5 docs cannot be mapped
        test.addChild("a100").setProperty("a", "foo");
        test.addChild("a200").setProperty("b", "foo");
        test.addChild("a101").setProperty("a", "foo");
        test.addChild("a201").setProperty("b", "foo");
        test.addChild("a102").setProperty("a", "foo");
        test.addChild("a202").setProperty("b", "foo");
        test.addChild("a103").setProperty("a", "foo");
        test.addChild("a203").setProperty("b", "foo");
        test.addChild("a104").setProperty("a", "foo");
        test.addChild("a204").setProperty("b", "foo");
        root.commit();

        String query = "select [jcr:path] from [nt:base] where [a] = 'foo'";
        assertEventually(() -> assertQuery(query, SQL2,
                List.of("/test/a1", "/test/a2", "/test/a100", "/test/a101", "/test/a102", "/test/a103", "/test/a104")
        ));
    }

    private void createIndexOfType(String type) throws CommitFailedException {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("propa")
                .propertyIndex()
                .type(type);

        setIndex("test", builder);
        root.commit();
    }
}
