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
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticBulkProcessorHandler;
import org.junit.After;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

/**
 * Tests for reliability of async indexing with connection cuts and failures.
 * This test uses Toxiproxy to simulate network issues.
 */
abstract public class ElasticReliabilityTest extends ElasticAbstractQueryTest {

    public static final String TOXIPROXY_IMAGE_NAME = "ghcr.io/shopify/toxiproxy:2.12.0";

    // set cache expiration and refresh to low values to avoid cached results in tests
    @Rule
    public final ProvideSystemProperty updateSystemProperties
            = new ProvideSystemProperty("oak.elastic.statsExpireSeconds", "5")
            .and("oak.elastic.statsRefreshSeconds", "1");

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse(TOXIPROXY_IMAGE_NAME);

    protected ToxiproxyContainer toxiproxy;

    protected Proxy proxy;

    // Tests are hardcoded for these values
    protected final static int BULK_ACTIONS_TEST = 2;
    protected final static int BULK_SIZE_BYTES_TEST = 2 * 1024;

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
}
