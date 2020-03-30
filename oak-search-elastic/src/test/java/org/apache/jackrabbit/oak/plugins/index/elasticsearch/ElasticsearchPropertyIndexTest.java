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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import com.github.dockerjava.api.DockerClient;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.elasticsearch.Version;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeNotNull;

public class ElasticsearchPropertyIndexTest extends AbstractQueryTest {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchPropertyIndexTest.class);

    @Rule
    public final ElasticsearchContainer elastic =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + Version.CURRENT);

    @BeforeClass
    public static void beforeMethod() {
        DockerClient client = null;
        try {
            client = DockerClientFactory.instance().client();
        } catch (Exception e) {
            LOG.warn("Docker is not available, ElasticsearchPropertyIndexTest will be skipped");
        }
        assumeNotNull(client);
    }

    @Override
    protected ContentRepository createRepository() {
        ElasticsearchConnection coordinate = new ElasticsearchConnection(
                ElasticsearchConnection.DEFAULT_SCHEME,
                elastic.getContainerIpAddress(),
                elastic.getMappedPort(ElasticsearchConnection.DEFAULT_PORT)
        );
        ElasticsearchIndexEditorProvider editorProvider = new ElasticsearchIndexEditorProvider(coordinate,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        ElasticsearchIndexProvider indexProvider = new ElasticsearchIndexProvider(coordinate);

        // remove all indexes to avoid cost competition (essentially a TODO for fixing cost ES cost estimation)
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oiBuilder = builder.child(INDEX_DEFINITIONS_NAME);
        oiBuilder.getChildNodeNames().forEach(idxName -> oiBuilder.child(idxName).remove());

        NodeStore nodeStore = new MemoryNodeStore(builder.getNodeState());

        return new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with(indexProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(false);
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
        }, 10000);
    }

    //OAK-3825
    @Test
    public void nodeNameViaPropDefinition() throws Exception {
        //make index
        IndexDefinitionBuilder builder = createIndex();
        builder.includedPaths("/test")
                .evaluatePathRestrictions()
                .indexRule("nt:base")
                .property("nodeName", PROPDEF_PROP_NODE_NAME).propertyIndex();
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
            assertThat(explanation, containsString("{\"term\":{\":nodeName\":{\"value\":\"foo\","));
            assertQuery(propabQuery, singletonList("/test/foo"));

            assertQuery(queryPrefix + "LOCALNAME() = 'bar'", singletonList("/test/sc/bar"));
            assertQuery(queryPrefix + "LOCALNAME() LIKE 'foo'", singletonList("/test/foo"));
            assertQuery(queryPrefix + "LOCALNAME() LIKE 'camel%'", singletonList("/test/camelCase"));

            assertQuery(queryPrefix + "NAME() = 'bar'", singletonList("/test/sc/bar"));
            assertQuery(queryPrefix + "NAME() LIKE 'foo'", singletonList("/test/foo"));
            assertQuery(queryPrefix + "NAME() LIKE 'camel%'", singletonList("/test/camelCase"));
        }, 10000);
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
                containsString("elasticsearch:test1")), 10000);
    }

    @Test
    public void propertyExistenceQuery() throws Exception {
        setIndex("test1", createIndex("propa", "propb"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        root.commit();

        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where propa is not null",
                Arrays.asList("/test/a", "/test/b")), 10000);
    }

    private static IndexDefinitionBuilder createIndex(String... propNames) {
        IndexDefinitionBuilder builder = new ElasticsearchIndexDefinitionBuilder().noAsync();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        for (String propName : propNames) {
            indexRule.property(propName).propertyIndex();
        }
        return builder;
    }

    private void setIndex(String idxName, IndexDefinitionBuilder builder) {
        builder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
    }

    private String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    private static void assertEventually(Runnable r, long timeoutMillis) {
        final long start = System.currentTimeMillis();
        long lastAttempt = 0;
        int attempts = 0;

        while (true) {
            try {
                attempts++;
                lastAttempt = System.currentTimeMillis();
                r.run();
                return;
            } catch (Throwable e) {
                long elapsedTime = lastAttempt - start;
                if (elapsedTime >= timeoutMillis) {
                    String msg = String.format("Condition not satisfied after %1.2f seconds and %d attempts",
                            elapsedTime / 1000d, attempts);
                    throw new AssertionError(msg, e);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }

            }
        }
    }
}
