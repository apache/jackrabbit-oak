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
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDockerRule;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_BATCH_SIZE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP_SECONDARIES_ONLY;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_MAX_NUMBER_OF_DOCUMENTS;

@RunWith(Parameterized.class)
public class PipelinedMongoConnectionFailureIT {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoConnectionFailureIT.class);

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.11.0");
    // We cannot use the MongoDockerRule/MongoConnectionFactory because they don't allow customizing the docker network
    // used to launch the Mongo container.
    private static final int MONGODB_DEFAULT_PORT = 27017;

    @Parameterized.Parameters(name = "parallelDump={0}, testUpdateContent={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[]{true, false},
                new Object[]{false, false},
                new Object[]{true, true},
                new Object[]{false, true}
        );
    }

    final boolean parallelDump;
    final boolean testUpdateContent;

    @Rule
    public final Network network = Network.newNetwork();
    @Rule
    public final MongoDBContainer mongoDBContainer = new MongoDBContainer(MongoDockerRule.getDockerImageName())
            .withNetwork(network)
            .withNetworkAliases("mongo")
            .withExposedPorts(MONGODB_DEFAULT_PORT);
    @Rule
    public final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE).withNetwork(network);
    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    @Rule
    public final TemporaryFolder sortFolder = new TemporaryFolder();
    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    private Proxy proxy;
    private String mongoUriFlaky;

    private String mongoUriReliable;

    public PipelinedMongoConnectionFailureIT(boolean parallelDump, boolean testUpdateContent) {
        this.parallelDump = parallelDump;
        this.testUpdateContent = testUpdateContent;
    }

    @BeforeClass
    public static void setup() throws IOException {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Before
    public void before() throws Exception {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        // For the logic under test
        this.proxy = toxiproxyClient.createProxy("mongo", "0.0.0.0:8666", "mongo:" + MONGODB_DEFAULT_PORT);
        this.mongoUriFlaky = "mongodb://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8666) + "/" + MongoUtils.DB;

        // For the operations that prepare and update the test data.
        toxiproxyClient.createProxy("mongo-reliable", "0.0.0.0:8667", "mongo:" + MONGODB_DEFAULT_PORT);
        this.mongoUriReliable = "mongodb://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8667) + "/" + MongoUtils.DB;

        MongoConnection c = new MongoConnection(mongoUriReliable);
        try {
            c.getDatabase().drop();
        } finally {
            c.close();
        }
    }

    @Test
    public void mongoDisconnectTest() throws Exception {
        // Testcontainers' MongoDBContainer starts a Mongo replica set of one node. In this configuration we cannot
        // configure the downloader with the replica selection policy that avoids downloading from the primary, using
        // only the secondaries. So we disable that policy here. the unit tests in PipelinedMongoDownloadTaskTest cover
        // that case.
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP_SECONDARIES_ONLY, "false");

        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP, String.valueOf(parallelDump));
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_MAX_NUMBER_OF_DOCUMENTS, String.valueOf(100));
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_BATCH_SIZE, String.valueOf(5));

        try (MongoTestBackend rwBackend = PipelineITUtil.createNodeStore(false, mongoUriReliable, builderProvider)) {
            createContent(rwBackend.documentNodeStore);
        }

        Path resultWithoutInterruption;
        LOG.info("Creating a FFS: reference run without failures.");
        try (MongoTestBackend roBackend = PipelineITUtil.createNodeStore(true, mongoUriReliable, builderProvider)) {
            PipelinedStrategy strategy = createStrategy(roBackend);
            resultWithoutInterruption = strategy.createSortedStoreFile().toPath();
        }

        Path resultWithInterruption;
        LOG.info("Creating a FFS: test run with disconnection to Mongo.");
        try (MongoTestBackend roBackend = PipelineITUtil.createNodeStore(true, mongoUriFlaky, builderProvider)) {
            // Closes connections after transmitting 50KB. This is per connection. The reconnect attempts by
            // the download task will succeed, but the new connections will once again be closed after 50KB of data.
            LimitData cutConnectionUpstream = proxy.toxics()
                    .limitData("CUT_CONNECTION_UPSTREAM", ToxicDirection.DOWNSTREAM, 300_000L);

            ScheduledExecutorService scheduleExecutor = Executors.newScheduledThreadPool(2);
            try {
                // Wait some time to allow the download to start and then start a task to update the content
                // in a loop. When the download starts, the descending download thread will go down by order of
                // _modified value, starting with the highest value at the time that the download starts. If a node
                // that was not yet downloaded is modified and there is a connection failure, this node will not be
                // downloaded in the reconnection attempt because the thread will continue downloading going down
                // from the last _modified value seen. Therefore, once the downloader finishes the ascending/descending
                // downloads, it must make one final pass to download any new nodes that were modified after the
                // download starts.
                ScheduledFuture<?> updateTask = null;
                UpdateContentTask updateContentTask = null;
                if (testUpdateContent) {
                    updateContentTask = new UpdateContentTask();
                    updateTask = scheduleExecutor.schedule(updateContentTask, 1, TimeUnit.SECONDS);
                }

                // Removes the connection block after some time
                var removeToxicTask = scheduleExecutor.schedule(() -> {
                    try {
                        LOG.info("Removing connection block");
                        cutConnectionUpstream.remove();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, 5, TimeUnit.SECONDS);

                PipelinedStrategy strategy = createStrategy(roBackend);
                resultWithInterruption = strategy.createSortedStoreFile().toPath();
                // Retrieve the results to raise an exception if the tasks failed
                if (updateTask != null) {
                    updateContentTask.stop();
                    try {
                        updateTask.get();
                    } catch (InterruptedException e) {
                        // Ignore, expected.
                    }
                }
                removeToxicTask.get();
            } finally {
                scheduleExecutor.shutdown();
                var terminated = scheduleExecutor.awaitTermination(5, TimeUnit.SECONDS);
                if (!terminated) {
                    LOG.warn("Executor did not terminate in time");
                }
            }
        }

        List<String> ffsLinesNoFailure = Files.readAllLines(resultWithoutInterruption);
        List<String> allLinesWithFailure = Files.readAllLines(resultWithInterruption);
        if (!ffsLinesNoFailure.equals(allLinesWithFailure)) {
            List<String> pathsInFFSNoFailures = ffsLinesNoFailure.stream().map(l -> l.substring(0, l.indexOf('|'))).collect(Collectors.toList());
            List<String> pathsInFFSFailures = allLinesWithFailure.stream().map(l -> l.substring(0, l.indexOf('|'))).collect(Collectors.toList());
            Assert.fail("Results differ when downloading with no failures and with failures.\n" +
                    "  No failures  : " + pathsInFFSNoFailures + "\n" +
                    "  With failures: " + pathsInFFSFailures);
        }
    }


    private PipelinedStrategy createStrategy(MongoTestBackend roStore) throws IOException {
        return PipelineITUtil.createStrategy(roStore, s -> true, null, sortFolder.newFolder());
    }

    private static final int N_TREES = 20;
    private static final int N_NODES_PER_TREE = 50;

    private static void createContent(DocumentNodeStore rwNodeStore) throws CommitFailedException {
        LOG.info("Creating content");
        String payload = "0123456789".repeat(500); // 5KB per entry. 500KB per tree, 5MB per 10 trees.
        Stopwatch start = Stopwatch.createStarted();
        Clock.Virtual clock = rwNodeStore.getClock() instanceof Clock.Virtual ? (Clock.Virtual) rwNodeStore.getClock() : null;
        for (int i = 0; i < N_TREES; i++) {
            @NotNull NodeBuilder rootBuilder = rwNodeStore.getRoot().builder();
            @NotNull NodeBuilder tree = rootBuilder.child("content").child("dam").child("parent" + i);
            for (int j = 0; j < N_NODES_PER_TREE; j++) {
                tree.child("node-" + j)
                        .setProperty("payload", payload) // Just to make it big
                        .setProperty("p1", "value-" + i + "-" + j);
            }
            rwNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            // The _modified field has a precision of 5 seconds, so we advance the virtual clock by 5000 milliseconds
            // to ensure that the next batch of documents will have a different _modified field.
            if (clock != null) {
                clock.waitUntil(clock.getTime() + 5000);
            }
        }
        LOG.info("Done creating content in {} ms", start.elapsed(TimeUnit.MILLISECONDS));
    }


    private class UpdateContentTask implements Runnable {
        private volatile boolean stop = false;

        public void stop() {
            LOG.info("Stopping update content task");
            this.stop = true;
        }

        @Override
        public void run() {
            try {
                try (MongoTestBackend mongoTestBackend = PipelineITUtil.createNodeStore(false, mongoUriReliable, builderProvider)) {
                    NodeStore nodeStore = mongoTestBackend.documentNodeStore;
                    for (int loop = 0; !stop; loop++) {
                        LOG.info("Updating content, loop {}", loop);
                        for (int i = 0; i < N_TREES && !stop; i++) {
                            @NotNull NodeBuilder rootBuilder = nodeStore.getRoot().builder();
                            @NotNull NodeBuilder parent = rootBuilder.child("content").child("dam").child("parent" + i);
                            for (int j = 0; j < N_NODES_PER_TREE; j++) {
                                parent.child("node-" + j).setProperty("p1", "modified-" + loop);
                            }
                            nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                        }
                        Thread.sleep(100);
                    }
                    LOG.info("Done updating content, stop requested");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }

}
