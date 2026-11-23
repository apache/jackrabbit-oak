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

import co.elastic.clients.elasticsearch.core.GetResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.LimitData;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticReliabilityTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies acknowledged writes before and after an explicitly observed network failure.
 * Recovery permits subsequent writes; it does not implicitly replay the failed bulk.
 */
public class ElasticIndexWriterITTest {

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse(ElasticReliabilityTest.TOXIPROXY_IMAGE_NAME);

    protected ToxiproxyContainer toxiproxy;

    protected Proxy proxy;

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    @Rule
    public final ProvideSystemProperty bulkProperties =
            new ProvideSystemProperty(ElasticBulkProcessorHandler.BULK_ACTIONS_PROP, "2")
                    .and(ElasticBulkProcessorHandler.BULK_SIZE_BYTES_PROP, "2048");

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
        assumeTrue("Network fault injection requires Docker", elasticRule.useDocker());
        this.toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withStartupAttempts(3)
                .withNetwork(elasticRule.elastic.getNetwork());
        this.toxiproxy.start();
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        this.proxy = toxiproxyClient.createProxy("elastic", "0.0.0.0:8666", "elasticsearch:9200");

        this.connection = getElasticConnection();
        this.indexTracker = mock(ElasticIndexTracker.class);
        this.nodeStore = new MemoryNodeStore(INITIAL_CONTENT);
    }

    @After
    public void tearDown() throws IOException {
        try {
            if (connection != null) {
                connection.close();
            }
        } finally {
            if (toxiproxy != null) {
                toxiproxy.stop();
            }
        }
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
        NodeBuilder definitionBuilder = builder.child("oak:index").getChildNode(indexName);
        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(connection);
        try (Closeable closeHandler = bulkProcessorHandler::close) {
            ElasticIndexWriter writer = newWriter(definition, definitionBuilder, bulkProcessorHandler);
            for (int i = 0; i < 10; i++) {
                String path = "/content/" + i;
                writer.updateDocument(path, new ElasticDocument(path));
            }
            assertTrue(writer.close(System.currentTimeMillis()));
            for (int i = 0; i < 10; i++) {
                assertDocument(definition.getIndexAlias(), "/content/" + i);
            }

            LimitData cutConnectionUpstream = proxy.toxics()
                    .limitData("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0L);
            try {
                ElasticIndexWriter disconnectedWriter = newWriter(definition, definitionBuilder, bulkProcessorHandler);
                disconnectedWriter.updateDocument("/content/during-outage", new ElasticDocument("/content/during-outage"));
                IOException failure = assertThrows(IOException.class,
                        () -> disconnectedWriter.close(System.currentTimeMillis()));
                assertEquals("Service error while indexing.", failure.getMessage());
                assertNotNull(failure.getCause());
            } finally {
                cutConnectionUpstream.remove();
            }

            ElasticIndexWriter recoveredWriter = newWriter(definition, definitionBuilder, bulkProcessorHandler);
            recoveredWriter.updateDocument("/content/after-recovery", new ElasticDocument("/content/after-recovery"));
            assertTrue(recoveredWriter.close(System.currentTimeMillis()));
            assertDocument(definition.getIndexAlias(), "/content/after-recovery");
            assertFalse(connection.getClient().exists(e -> e.index(definition.getIndexAlias())
                    .id(ElasticIndexUtils.idFromPath("/content/during-outage"))).value());
        }
    }

    private ElasticIndexWriter newWriter(ElasticIndexDefinition definition, NodeBuilder definitionBuilder,
                                          ElasticBulkProcessorHandler bulkProcessorHandler) {
        bulkProcessorHandler.registerIndex(definition.getIndexAlias(), definition, definitionBuilder, CommitInfo.EMPTY, true);
        return new EagerElasticIndexWriter(indexTracker, connection, definition, bulkProcessorHandler);
    }

    private void assertDocument(String indexName, String path) throws IOException {
        GetResponse<ObjectNode> response = connection.getClient().get(
                g -> g.index(indexName).id(ElasticIndexUtils.idFromPath(path)), ObjectNode.class);
        assertTrue("Missing document: " + path, response.found());
        assertNotNull(response.source());
        assertEquals(path, response.source().path(FieldNames.PATH).asText());
    }
}
