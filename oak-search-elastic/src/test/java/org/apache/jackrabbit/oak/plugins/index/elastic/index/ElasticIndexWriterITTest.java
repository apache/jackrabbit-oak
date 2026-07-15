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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.LimitData;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticReliabilityTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;

public class ElasticIndexWriterITTest {

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse(ElasticReliabilityTest.TOXIPROXY_IMAGE_NAME);

    protected ToxiproxyContainer toxiproxy;

    protected Proxy proxy;

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    public NodeStore nodeStore;
    private ElasticConnection connection;
    private ElasticIndexTracker indexTracker;

    protected ElasticConnection getElasticConnection() {
        return elasticRule.useDocker() ?
                elasticRule.getElasticConnectionForDocker(toxiproxy.getHost(), toxiproxy.getMappedPort(8666)) :
                elasticRule.getElasticConnectionFromString();
    }

    @Before
    public void setup() throws Exception {
        // Use a low value for the tests
        System.setProperty(ElasticBulkProcessorHandler.BULK_ACTIONS_PROP, "2");
        System.setProperty(ElasticBulkProcessorHandler.BULK_SIZE_BYTES_PROP, "2048");
        this.toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withStartupAttempts(3)
                .withNetwork(elasticRule.elastic.getNetwork());
        this.toxiproxy.start();
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        this.proxy = toxiproxyClient.createProxy("elastic", "0.0.0.0:8666", "elasticsearch:9200");

        this.connection = getElasticConnection();
        this.indexTracker = new ElasticIndexTracker(connection, new ElasticMetricHandler(StatisticsProvider.NOOP));
        this.nodeStore = new MemoryNodeStore(INITIAL_CONTENT);
    }


    @Test
    public void writerRecoversFromDisconnection() throws Exception {
        @NotNull NodeState root = nodeStore.getRoot();
        @NotNull NodeBuilder builder = root.builder();
        String indexName = "test";
        IndexDefinitionBuilder idxBuilder = new ElasticIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        idxBuilder.indexRule("nt:base")
                .property("propa")
                .propertyIndex();
        NodeState nodeState = idxBuilder.build();
        ElasticIndexDefinition definition = new ElasticIndexDefinition(root, nodeState, indexName, connection.getIndexPrefix());
        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(connection);
        bulkProcessorHandler.registerIndex(connection.getIndexPrefix() + "." + indexName, definition, builder.child("oak:index").getChildNode(indexName), CommitInfo.EMPTY, false);
        ElasticIndexWriter writer = new ElasticIndexWriter(indexTracker, connection, definition, bulkProcessorHandler,
                new ElasticRetryPolicy(10, 1000, 5, 100), false
        );

        for (int i = 0; i < 10; i++) {
            String path = "/content/" + i;
            writer.updateDocument(path, new ElasticDocument(path));
        }

        // Set a toxic to cut the connection to Elastic after further 4KB of data sent upstream
        LimitData cutConnectionUpstream = proxy.toxics()
                .limitData("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 4096L);

        for (int i = 0; i < 30; i++) {
            String path = "/content/" + i;
            writer.updateDocument(path, new ElasticDocument(path));
        }
        writer.close(System.currentTimeMillis());
    }
}
