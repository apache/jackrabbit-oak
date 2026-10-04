/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.service.Cluster;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.IdleStrategy;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A member must not run on an Aeron log that is not the one its Oak store was built from: replay would skip
 * new entries as "already applied" or apply them on top of unrelated state.
 */
public class LogStoreConsistencyTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private AeronConsensusEngine engine;

    @After
    public void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    public void freshStoreAndFreshLogStartNormally() throws Exception {
        start(new MemoryNodeStore(), clusterDir());
        termEvent(0, 0);
    }

    @Test
    public void emptyMemberJoiningAnExistingClusterReplaysFromZero() throws Exception {
        start(new MemoryNodeStore(), tempFolder.newFolder("no-recording-log"));
        termEvent(0, 0);
        termEvent(1, 4_096);
        termEvent(2, 9_000);
    }

    @Test
    public void populatedStoreOnAnEmptyLogWithoutSnapshotFailsToStart() throws Exception {
        assertStartFails(storeAt(5_000, 2), clusterDir());
    }

    @Test
    public void logEndingInAnEarlierTermThanTheStoreFailsToStart() throws Exception {
        assertStartFails(storeAt(5_000, 3), clusterDir(0, 1));
    }

    @Test
    public void storeConsistentWithItsLogStartsAndReplays() throws Exception {
        start(storeAt(5_000, 1), clusterDir(0, 1));
        termEvent(0, 0);
        termEvent(1, 1_000);
        termEvent(2, 5_000);
    }

    @Test
    public void newTermStartingBeforeTheStoreWatermarkStopsTheService() throws Exception {
        start(storeAt(5_000, 1), clusterDir(0, 1));
        termEvent(0, 0);
        termEvent(1, 1_000);
        try {
            termEvent(2, 3_000);
            fail("Expected the log/store mismatch to stop the service");
        } catch (AgentTerminationException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("--fresh"));
        }
    }

    @Test
    public void olderTermStartingAfterTheStoreWatermarkStopsTheService() throws Exception {
        start(storeAt(5_000, 3), clusterDir(0, 1, 2, 3));
        try {
            termEvent(1, 6_000);
            fail("Expected the log/store mismatch to stop the service");
        } catch (AgentTerminationException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("does not match"));
        }
    }

    private void assertStartFails(MemoryNodeStore store, File clusterDir) throws Exception {
        try {
            start(store, clusterDir);
            fail("Expected the log/store mismatch to fail startup");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("does not match"));
            assertTrue(e.getMessage(), e.getMessage().contains("--fresh"));
        }
    }

    private void start(MemoryNodeStore store, File clusterDir) throws Exception {
        engine = new AeronConsensusEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), store, "http://self:8080",
            Collections.emptyList(), mock(EthereumWallet.class), tempFolder.newFolder().getAbsolutePath(), null);
        Cluster cluster = mock(Cluster.class, RETURNS_DEEP_STUBS);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(cluster.idleStrategy()).thenReturn(mock(IdleStrategy.class));
        when(cluster.context().clusterDir()).thenReturn(clusterDir);
        engine.onStart(cluster, null);
    }

    private void termEvent(long leadershipTermId, long termBaseLogPosition) {
        engine.onNewLeadershipTermEvent(leadershipTermId, termBaseLogPosition, 0L, termBaseLogPosition, 0, 1,
            TimeUnit.MILLISECONDS, 1);
    }

    private static MemoryNodeStore storeAt(long position, long term) throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder root = store.getRoot().builder();
        root.child("oak-chain");
        new AppliedLogPosition(position, 0, term).writeTo(root);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return store;
    }

    /** A cluster directory whose recording log holds TERM entries for the given terms (none = empty log). */
    private File clusterDir(long... terms) throws Exception {
        File dir = tempFolder.newFolder();
        try (RecordingLog recordingLog = new RecordingLog(dir, true)) {
            long base = 0;
            for (long term : terms) {
                recordingLog.appendTerm(7L, term, base, 0L);
                base += 1_000;
            }
        }
        return dir;
    }
}
