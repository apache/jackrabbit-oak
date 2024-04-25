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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import org.apache.jackrabbit.guava.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PipelinedMongoServerSelectorTest {

    public static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoServerSelector.class);
    private final static ClusterId TEST_CLUSTER_ID = new ClusterId("test-mongo-cluster");

    private final AtomicInteger serverId = new AtomicInteger(0);

    private final List<ServerDescription> allReplicas = List.of(
            createServerDescription(ServerType.REPLICA_SET_PRIMARY, ServerConnectionState.CONNECTED),
            createServerDescription(ServerType.REPLICA_SET_SECONDARY, ServerConnectionState.CONNECTED),
            createServerDescription(ServerType.REPLICA_SET_SECONDARY, ServerConnectionState.CONNECTED)
    );

    private final List<ServerDescription> oneSecondaryUp = List.of(
            createServerDescription(ServerType.REPLICA_SET_PRIMARY, ServerConnectionState.CONNECTED),
            createServerDescription(ServerType.REPLICA_SET_SECONDARY, ServerConnectionState.CONNECTED)
    );

    private final List<ServerDescription> noSecondaryUp = List.of(
            createServerDescription(ServerType.REPLICA_SET_PRIMARY, ServerConnectionState.CONNECTED)
    );

    private ExecutorService threadAscending;
    private ExecutorService threadDescending;
    private ExecutorService threadOther;

    @Before
    public void setUp() {
        threadAscending = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(PipelinedMongoDownloadTask.THREAD_NAME_PREFIX + "-ascending").setDaemon(true).build());
        threadDescending = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(PipelinedMongoDownloadTask.THREAD_NAME_PREFIX + "-descending").setDaemon(true).build());
        threadOther = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("mongo-server-selector-test-thread").setDaemon(true).build());
    }

    @After
    public void tearDown() {
        threadAscending.shutdown();
        threadDescending.shutdown();
        threadOther.shutdown();
    }

    @Test
    public void calledFromANonDownloaderThread() throws ExecutionException, InterruptedException {
        // If the Mongo server selector is called from a thread that is not a downloader thread, it should return all 
        // servers, that is, it is neutral.
        var t = new PipelinedMongoServerSelector();
        threadOther.submit(() -> {
            assertEquals(3, t.select(ClusterType.REPLICA_SET, allReplicas).size());
            assertEquals(2, t.select(ClusterType.REPLICA_SET, oneSecondaryUp).size());
            assertEquals(1, t.select(ClusterType.REPLICA_SET, noSecondaryUp).size());
        }).get();
    }

    @Test
    public void allMongoReplicasUp() throws ExecutionException, InterruptedException {
        var mongoServerSelector = new PipelinedMongoServerSelector();

        var firstThreadSelection = new AtomicReference<ServerDescription>();

        // All nodes available, a downloader thread asks for a connection. It should get one of the secondaries
        threadAscending.submit(() -> {
            var firstSelection = mongoServerSelector.select(ClusterType.REPLICA_SET, allReplicas);
            assertEquals(1, firstSelection.size());
            var firstSecondarySelected = firstSelection.get(0);
            assertEquals(ServerType.REPLICA_SET_SECONDARY, firstSecondarySelected.getType());
            assertTrue(allReplicas.contains(firstSecondarySelected));

            firstThreadSelection.set(firstSecondarySelected);

            // One secondary is taken, the other is available.
            // If the thread asks again for a connection, it receives the same replica.
            var secondSelection = mongoServerSelector.select(ClusterType.REPLICA_SET, allReplicas);
            assertEquals(1, secondSelection.size());
            assertEquals(firstSecondarySelected, secondSelection.get(0));
        }).get();

        // Another thread tries to select a replica. It should receive the secondary that is still available.
        threadDescending.submit(() -> {
            var serverSelection = mongoServerSelector.select(ClusterType.REPLICA_SET, allReplicas);
            assertEquals(1, serverSelection.size());
            var firstSecondarySelected = serverSelection.get(0);
            assertEquals(ServerType.REPLICA_SET_SECONDARY, firstSecondarySelected.getType());
            assertTrue(allReplicas.contains(firstSecondarySelected));
            assertNotEquals(firstThreadSelection.get(), firstSecondarySelected);
        }).get();
    }

    @Test
    public void primaryAndOneSecondaryUp() throws ExecutionException, InterruptedException {
        var mongoServerSelector = new PipelinedMongoServerSelector();
        mongoServerSelector.clusterDescriptionChanged(new ClusterDescriptionChangedEvent(
                        TEST_CLUSTER_ID,
                        new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.REPLICA_SET, oneSecondaryUp),
                        new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.REPLICA_SET, oneSecondaryUp)
                )
        );

        var firstThreadSelection = new AtomicReference<ServerDescription>();

        threadAscending.submit(() -> {
            // Should select one of the secondaries
            var firstSelection = mongoServerSelector.select(ClusterType.REPLICA_SET, oneSecondaryUp);
            assertEquals(1, firstSelection.size());
            var firstSecondarySelected = firstSelection.get(0);
            assertEquals(ServerType.REPLICA_SET_SECONDARY, firstSecondarySelected.getType());
            assertTrue(oneSecondaryUp.contains(firstSecondarySelected));
            firstThreadSelection.set(firstSecondarySelected);
        }).get();

        // Another thread tries to select a replica. But the only secondary available is taken, so the selection algorithm
        // returns an empty list.
        threadDescending.submit(() -> {
            var serverSelection = mongoServerSelector.select(ClusterType.REPLICA_SET, oneSecondaryUp);
            assertTrue(serverSelection.isEmpty());
        }).get();

        // The first thread finishes, so the secondary is available again.
        threadAscending.submit(mongoServerSelector::threadFinished).get();

        threadDescending.submit(() -> {
            var serverSelection = mongoServerSelector.select(ClusterType.REPLICA_SET, oneSecondaryUp);
            assertEquals(1, serverSelection.size());
            assertEquals(firstThreadSelection.get(), serverSelection.get(0));
        }).get();
    }

    @Test
    public void onlyPrimaryUp() throws ExecutionException, InterruptedException {
        var mongoServerSelector = new PipelinedMongoServerSelector();
        threadAscending.submit(() -> {
            // If there is only the primary, do not select it.
            var firstSelection = mongoServerSelector.select(ClusterType.REPLICA_SET, noSecondaryUp);
            assertTrue(firstSelection.isEmpty());
        }).get();
    }

    @Test
    public void mongoScaling() throws ExecutionException, InterruptedException {
        ArrayList<ServerDescription> clusterState = new ArrayList<>();
        clusterState.add(createServerDescription(ServerType.REPLICA_SET_PRIMARY, ServerConnectionState.CONNECTED));
        clusterState.add(createServerDescription(ServerType.REPLICA_SET_SECONDARY, ServerConnectionState.CONNECTED));
        clusterState.add(createServerDescription(ServerType.REPLICA_SET_SECONDARY, ServerConnectionState.CONNECTED));

        /* A scaling of Mongo happens in the following way
         * 1. Initial state, three replicas available, one primary and two secondaries.
         *    Downloader threads are connected each to a secondary.
         * 2. One secondary is taken down. That thread fails, unregisters and tries to get another secondary.
         *    But fails because there is no other secondary available
         * 3. After some time, a new replica comes up as a new secondary. The thread that was not able to connect to any
         *    replica, can now connect to this new secondary
         * 4. The second secondary of the original replicas is taken down. The thread that was connected to it fails.
         * 5. After some time, a new secondary comes up and the thread that was disconnected can connect to it.
         * 6. One of the secondaries (both are new replicas) is promoted to primary and the previous primary (which is
         *    still an old replica) is demoted to secondary and taken down to be replaced. The thread that is connected
         *    to the secondary that is promoted to primary should disconnect from it and try to get a new secondary.
         * 7. After a while, a new replica comes up with the role of secondary and the thread that was idle should
         *    connect to it.
         * 8. Scaling is complete.
         */
        var allUpClusterDescription = new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.REPLICA_SET, List.copyOf(clusterState));
        var mongoServerSelector = new PipelinedMongoServerSelector();
        mongoServerSelector.clusterDescriptionChanged(
                new ClusterDescriptionChangedEvent(TEST_CLUSTER_ID, allUpClusterDescription, allUpClusterDescription)
        );

        var ascendingThreadReplica = new AtomicReference<ServerDescription>();
        var descendingThreadReplica = new AtomicReference<ServerDescription>();
        threadAscending.submit(() -> {
            var selection = mongoServerSelector.select(ClusterType.REPLICA_SET, clusterState);
            assertEquals(1, selection.size());
            ascendingThreadReplica.set(selection.get(0));
        }).get();
        threadDescending.submit(() -> {
            var selection = mongoServerSelector.select(ClusterType.REPLICA_SET, clusterState);
            assertEquals(1, selection.size());
            descendingThreadReplica.set(selection.get(0));
        }).get();

        ServerDescription firstSecondaryDown = ascendingThreadReplica.get();
        ServerDescription secondSecondaryDown = descendingThreadReplica.get();
        ServerDescription originalPrimary = clusterState.stream().filter(ServerDescription::isPrimary).findFirst().get();

        // ****************************
        // One secondary goes down
        // ****************************
        // All threads have a secondary assigned. Take down the replica of the ascending thread.
        clusterState.remove(firstSecondaryDown);
        var oneSecondaryDownClusterDescription = new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.REPLICA_SET, List.copyOf(clusterState));
        mongoServerSelector.clusterDescriptionChanged(
                new ClusterDescriptionChangedEvent(TEST_CLUSTER_ID, allUpClusterDescription, oneSecondaryDownClusterDescription)
        );

        // The ascending thread should not be able to connect to any secondary
        threadAscending.submit(() -> {
            var selection = mongoServerSelector.select(ClusterType.REPLICA_SET, clusterState);
            assertTrue(selection.isEmpty());
            ascendingThreadReplica.set(null);
        }).get();

        // The descending thread should still have a secondary assigned
        threadDescending.submit(() -> {
            var selection = mongoServerSelector.select(ClusterType.REPLICA_SET, clusterState);
            assertEquals(1, selection.size());
            assertEquals(descendingThreadReplica.get(), selection.get(0));
        }).get();

        // ************************************************
        // The first secondary is replaced by a new replica
        // ************************************************
        clusterState.add(firstSecondaryDown);
        // The ascending thread should now be able to connect to the new secondary
        threadAscending.submit(() -> {
            var selection = mongoServerSelector.select(ClusterType.REPLICA_SET, List.copyOf(clusterState));
            assertEquals(1, selection.size());
            ascendingThreadReplica.set(selection.get(0));
            assertNotEquals(descendingThreadReplica.get(), selection.get(0));
            assertEquals(firstSecondaryDown, selection.get(0));
        }).get();

        // Taking down the second secondary is similar to the first one, so skipping testing that.
        // Test the replacement of the primary.
        // Demote the current primary to secondary and promote the new replica to primary.
        var demotedPrimary = updateServerType(originalPrimary, ServerType.REPLICA_SET_SECONDARY);
        var promotedSecondary = updateServerType(firstSecondaryDown, ServerType.REPLICA_SET_PRIMARY);
        clusterState.set(clusterState.indexOf(originalPrimary), demotedPrimary);
        clusterState.set(clusterState.indexOf(firstSecondaryDown), promotedSecondary);

        // Update the cluster description
        var descAfterPrimaryDemoted = new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.REPLICA_SET, List.copyOf(clusterState));
        mongoServerSelector.clusterDescriptionChanged(
                new ClusterDescriptionChangedEvent(TEST_CLUSTER_ID, descAfterPrimaryDemoted, oneSecondaryDownClusterDescription)
        );

        // The ascending thread was connected to the secondary that is now promoted to primary.
        threadAscending.submit(() -> {
            // The thread should detect that it is connected to a primary and disconnect from it.
            assertTrue("Failed to detect that it is connected to primary", mongoServerSelector.isConnectedToPrimary());
            mongoServerSelector.threadFinished();

            // When it tries to connect again, it should receive a secondary
            var selection = mongoServerSelector.select(ClusterType.REPLICA_SET, clusterState);
            assertEquals(1, selection.size());
            ascendingThreadReplica.set(selection.get(0));
            assertEquals(demotedPrimary.getSetName(), selection.get(0).getSetName());
        }).get();
    }

    public ServerDescription createServerDescription(ServerType type, ServerConnectionState state) {
        var id = serverId.incrementAndGet();
        return ServerDescription
                .builder()
                .ok(true)
                .setName("mongo-server-" + id)
                .address(new ServerAddress("localhost", 20000 + id))
                .state(state)
                .type(type)
                .build();
    }

    private ServerDescription updateServerType(ServerDescription serverDescription, ServerType newType) {
        return ServerDescription.builder()
                .ok(serverDescription.isOk())
                .setName(serverDescription.getSetName())
                .address(serverDescription.getAddress())
                .state(serverDescription.getState())
                .type(newType)
                .build();
    }
}