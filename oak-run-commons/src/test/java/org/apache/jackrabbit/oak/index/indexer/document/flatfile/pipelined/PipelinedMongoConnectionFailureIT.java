/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.LimitData;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP;

public class PipelinedMongoConnectionFailureIT {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoConnectionFailureIT.class);

    private static final String MONGO_VERSION = System.getProperty("mongo.version", "5.0");
    private static final String MONGO_IMAGE = "mongo:" + MONGO_VERSION;
    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.6.0");
    // We cannot use the MongoDockerRule/MongoConnectionFactory because they don't allow customizing the docker network
    // used to launch the Mongo container.
    private static final DockerImageName MONGODB_IMAGE = DockerImageName.parse(MONGO_IMAGE);
    private static final int MONGODB_DEFAULT_PORT = 27017;

    @Rule
    public final Network network = Network.newNetwork();
    @Rule
    public final TemporaryFolder sortFolder = new TemporaryFolder();
    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
    @Rule
    public final MongoDBContainer mongoDBContainer = new MongoDBContainer(MONGODB_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("mongo")
            .withExposedPorts(MONGODB_DEFAULT_PORT);
    @Rule
    public final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE).withNetwork(network);
    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Proxy proxy;
    private String mongoUri;

    @Before
    public void before() throws Exception {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        this.proxy = toxiproxyClient.createProxy("mongo", "0.0.0.0:8666", "mongo:" + MONGODB_DEFAULT_PORT);
        this.mongoUri = "mongodb://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8666) + "/" + MongoUtils.DB;
    }

    @Test
    public void mongoDisconnectTest() throws Exception {
        // Testcontainers' MongoDBContainer starts a Mongo replica set of one node. If parallel dump is enabled, the
        // Mongo server selector will refuse to connect to the primary node, which is the only node, so it will forever
        // fail to connect to Mongo. For the time being, we test reconnections without parallel dump

        // TODO: Update the server selector policy to also connect to the primary in case there are no secondaries after some
        // grace period to allow for the discovery of replicas to complete.

        // Add a test case with more data to test the reconnection logic
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP, "false");

        try (MongoTestBackend rwBackend = PipelineITUtil.createNodeStore(false, mongoUri, builderProvider)) {
            PipelineITUtil.createContent(rwBackend.documentNodeStore);
            rwBackend.documentNodeStore.dispose();
            rwBackend.mongoDocumentStore.dispose();
        }

        Path resultWithoutInterruption;
        LOG.info("Creating a FFS: reference run without failures.");
        try (MongoTestBackend roBackend = PipelineITUtil.createNodeStore(true, mongoUri, builderProvider)) {
            var strategy = createStrategy(roBackend);
            resultWithoutInterruption = strategy.createSortedStoreFile().toPath();
        }

        Path resultWithInterruption;
        LOG.info("Creating a FFS: test run with disconnection to Mongo.");
        try (MongoTestBackend roBackend = PipelineITUtil.createNodeStore(true, mongoUri, builderProvider)) {
            LimitData cutConnectionUpstream = proxy.toxics()
                    .limitData("CUT_CONNECTION_UPSTREAM", ToxicDirection.DOWNSTREAM, 30000L);
            var scheduleExecutor = Executors.newSingleThreadScheduledExecutor();
            try {
                scheduleExecutor.schedule(() -> {
                    try {
                        LOG.info("Removing connection block");
                        cutConnectionUpstream.remove();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, 5, TimeUnit.SECONDS);

                PipelinedStrategy strategy = createStrategy(roBackend);
                resultWithInterruption = strategy.createSortedStoreFile().toPath();
            } finally {
                scheduleExecutor.shutdown();
            }
        }

        LOG.info("Comparing resulting FFS with and without Mongo disconnections: {} {}", resultWithoutInterruption, resultWithInterruption);
        Assert.assertEquals(Files.readAllLines(resultWithoutInterruption), Files.readAllLines(resultWithInterruption));
    }

    private PipelinedStrategy createStrategy(MongoTestBackend roStore) throws IOException {
        return PipelineITUtil.createStrategy(roStore, s -> true, null, sortFolder.newFolder());
    }
}
