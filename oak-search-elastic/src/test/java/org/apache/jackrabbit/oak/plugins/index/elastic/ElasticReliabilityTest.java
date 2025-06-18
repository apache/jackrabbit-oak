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

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.LimitData;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticBulkProcessorHandler;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticReliabilityTest extends ElasticAbstractQueryTest {

    // set cache expiration and refresh to low values to avoid cached results in tests
    @Rule
    public final ProvideSystemProperty updateSystemProperties
            = new ProvideSystemProperty("oak.elastic.statsExpireSeconds", "5")
            .and("oak.elastic.statsRefreshSeconds", "1");

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0");

    private ToxiproxyContainer toxiproxy;

    private Proxy proxy;

    // Tests are hardcoded for these values
    private final static int BULK_ACTIONS_TEST = 2;
    private final static int BULK_SIZE_BYTES_TEST = 2 * 1024;

    @Override
    public void before() throws Exception {
        // Use a low value for the tests
        System.setProperty(ElasticBulkProcessorHandler.BULK_ACTIONS_PROP, Integer.toString(BULK_ACTIONS_TEST));
        System.setProperty(ElasticBulkProcessorHandler.BULK_SIZE_BYTES_PROP, Integer.toString(BULK_SIZE_BYTES_TEST));
        toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withStartupAttempts(3)
                .withNetwork(elasticRule.elastic.getNetwork());
        toxiproxy.start();
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        proxy = toxiproxyClient.createProxy("elastic", "0.0.0.0:8666", "elasticsearch:9200");
        super.before();
    }

    @After
    @Override
    public void tearDown() throws IOException {
        super.tearDown();
        if (toxiproxy.isRunning()) {
            toxiproxy.stop();
        }
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(true);
    }

    @Override
    protected ElasticConnection getElasticConnection() {
        return elasticRule.useDocker() ?
                elasticRule.getElasticConnectionForDocker(toxiproxy.getHost(), toxiproxy.getMappedPort(8666)) :
                elasticRule.getElasticConnectionFromString();
    }

    @Test
    public void connectionCutOnQuery() throws Exception {
        String indexName = UUID.randomUUID().toString();
        setIndex(indexName, createIndex("propa", "propb"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        root.commit(Map.of("sync-mode", "rt"));

        String query = "select [jcr:path] from [nt:base] where propa is not null";

        assertEventually(() -> {
            assertThat(explain(query), containsString("elasticsearch:" + indexName));
            assertQuery(query, List.of("/test/a", "/test/b"));
        });

        // simulate an upstream connection cut
        LimitData cutConnectionUpstream = proxy.toxics()
                .limitData("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0L);

        assertEventually(() -> {
            // elastic is down, query should not use it
            assertThat(explain(query), not(containsString("elasticsearch:" + indexName)));

            // result set should be correct anyway since traversal is enabled
            assertQuery(query, List.of("/test/a", "/test/b"));
        });

        // re-establish connection
        cutConnectionUpstream.remove();

        assertEventually(() -> {
            // result set should be the same as before but this time elastic should be used
            assertThat(explain(query), containsString("elasticsearch:" + indexName));
            assertQuery(query, List.of("/test/a", "/test/b"));
        });
    }

    @Test
    public void connectionCutOnIndex() throws Exception {
        String indexName = UUID.randomUUID().toString();
        setIndex(indexName, createIndex("propa", "propb"));

        // simulate an upstream connection cut
        LimitData cutConnectionUpstream = proxy.toxics()
                .limitData("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 8192L);

        System.out.println("Adding test node");
        Tree testBefore = root.getTree("/").addChild("test");
        try {
            for (int i = 0; i < 100; i++) {
                testBefore.setProperty("propa", "a" + i);
                root.commit();
            }
        } catch (CommitFailedException cfe) {
            // ignore commit failures, we expect some due to the connection cut
            System.out.println("Commit failed: " + cfe.getMessage());
            cfe.printStackTrace();
            cutConnectionUpstream.remove();
        }

        Tree testAfter = root.getTree("/").addChild("test");
        for (int i = 0; i < 10; i++) {
            testAfter.setProperty("propa", "a" + i);
            root.commit();
        }
    }
}
