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

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticPropertyIndexTest extends ElasticAbstractQueryTest {

    @Test
    public void testBulkProcessorFlushLimit() throws Exception {
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

            assertQuery(propaQuery, singletonList("/test/a248"));
        });

        // Now we test for 250 < nodes < 500

        for (int i = 250; i < 300; i++) {
            test.addChild("a" + i).setProperty("propa", "foo" + i);
        }
        root.commit();
        String propaQuery2 = "select [jcr:path] from [nt:base] where [propa] = 'foo299'";
        assertEventually(() -> {
            assertThat(explain(propaQuery2), containsString("elasticsearch:test1"));

            assertQuery(propaQuery2, singletonList("/test/a299"));
        });
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

            assertQuery(propaQuery, Arrays.asList("/test/a", "/test/b"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 'foo2'", singletonList("/test/c"));
            assertQuery("select [jcr:path] from [nt:base] where [propc] = 'foo'", singletonList("/test/d"));
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
            assertQuery(query, SQL2, ImmutableList.of("/test/node-a", "/test/node-e", "/test/node-i"));
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
            assertQuery(query, SQL2, ImmutableList.of("/test/node-2", "/test/node-3", "/test/node-5", "/test/node-7"));
        });
    }

    @Test
    public void inOperandDoubleValues() throws Exception {
        String query = "select [jcr:path] from [nt:base] where [propa] in(2.0, 3.0, 5.0, 7.0)";

        createIndexOfType("Double");

        Tree test = root.getTree("/").addChild("test");
        for (int i = 0; i < 10; i++) {
            test.addChild("node-" + i).setProperty("propa", (double)i);
        }
        root.commit();

        assertEventually(() -> {
            assertThat(explain(query), containsString("{\"terms\":{\"propa\":[2.0,3.0,5.0,7.0]}}"));
            assertQuery(query, SQL2, ImmutableList.of("/test/node-2", "/test/node-3", "/test/node-5", "/test/node-7"));
        });
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
