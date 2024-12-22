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
package org.apache.jackrabbit.oak.plugins.blob;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.openmbean.TabularData;

import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Adds BlobGC tests related to retrieving oldest checkpoint reference
 */
public class BlobGCCheckpointRefTest extends BlobGCTest {
    protected CheckpointMBean checkpointMBean;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        checkpointMBean = new MemoryStoreCheckpointMBean(cluster.nodeStore, clock);
        WhiteboardUtils.registerMBean(wb, CheckpointMBean.class, checkpointMBean,
            CheckpointMBean.TYPE, "Test checkpoint mbean");
    }

    @Test
    public void gcCheckpointHeld() throws Exception {
        log.info("Staring gcCheckpointHeld()");

        long afterSetupTime = clock.getTime();
        log.info("afterSetupTime {}", afterSetupTime);

        checkpointMBean.createCheckpoint(100);
        Set<String> afterCheckpointBlobs = createBlobs(cluster.blobStore, 2, 100);
        Set<String> present = Sets.union(cluster.blobStoreState.blobsPresent, afterCheckpointBlobs);
        long maxGcAge = checkpointMBean.getOldestCheckpointCreationTimestamp() - afterSetupTime;

        log.info("{} blobs remaining : {}", present.size(), present);

        Set<String> existingAfterGC = executeGarbageCollection(cluster, cluster.getCollector(maxGcAge), false);
        assertTrue(Sets.symmetricDifference(present, existingAfterGC).isEmpty());
    }

    @Test
    public void gcCheckpointHeldNoAddition() throws Exception {
        log.info("Staring gcCheckpointHeldNoAddition()");

        long afterSetupTime = clock.getTime();
        log.info("afterSetupTime {}", afterSetupTime);

        checkpointMBean.createCheckpoint(100);
        long maxGcAge = checkpointMBean.getOldestCheckpointCreationTimestamp() - afterSetupTime;

        Set<String> existingAfterGC = executeGarbageCollection(cluster, cluster.getCollector(maxGcAge), false);
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
    }

    @Test
    public void gcCheckpointHeldMaxAgeChange() throws Exception {
        log.info("Staring gcCheckpointHeldMaxAgeChange()");

        long afterSetupTime = clock.getTime();
        log.info("{} afterSetupTime time", afterSetupTime);

        checkpointMBean.createCheckpoint(100);
        Set<String> afterCheckpointBlobs = createBlobs(cluster.blobStore, 2, 100);
        cluster.blobStoreState.blobsPresent.addAll(afterCheckpointBlobs);

        log.info("{} blobs added : {}", cluster.blobStoreState.blobsAdded.size(), cluster.blobStoreState.blobsAdded);
        log.info("{} blobs remaining : {}", cluster.blobStoreState.blobsPresent.size(), cluster.blobStoreState.blobsPresent);

        long maxGcAge = checkpointMBean.getOldestCheckpointCreationTimestamp() - afterSetupTime;
        log.info("Max age configured {}", maxGcAge);

        Set<String> existingAfterGC = executeGarbageCollection(cluster, cluster.getCollector(maxGcAge), false);
        assertTrue(Sets.symmetricDifference(cluster.blobStoreState.blobsPresent, existingAfterGC).isEmpty());
    }

    /**
     * CheckpointMBean implementation for MemoryNodeStore
     */
    static class MemoryStoreCheckpointMBean implements CheckpointMBean {
        private static final String CREATION_DATE = "creationDate";
        private final Clock clock;
        private final NodeStore nodeStore;

        public MemoryStoreCheckpointMBean(NodeStore nodeStore, Clock clock) {
            this.nodeStore = nodeStore;
            this.clock = clock;
        }

        @Override public TabularData listCheckpoints() {
            throw new UnsupportedOperationException("Operation not supported");
        }

        @Override public long getOldestCheckpointCreationTimestamp() {
            Iterable<String> checkpoints = nodeStore.checkpoints();
            long minCreationDate = Long.MAX_VALUE;
            for (String checkpoint : checkpoints) {
                Map<String, String> chkInfo = nodeStore.checkpointInfo(checkpoint);

                if (chkInfo.containsKey(CREATION_DATE) &&
                    Long.valueOf(chkInfo.get(CREATION_DATE)) < minCreationDate) {
                    minCreationDate = Long.valueOf(chkInfo.get(CREATION_DATE));
                }
            }

            if (minCreationDate == Long.MAX_VALUE) {
                minCreationDate = 0;
            }

            return minCreationDate;
        }

        @Override public Date getOldestCheckpointCreationDate() {
            return new Date(getOldestCheckpointCreationTimestamp());
        }

        @Override public String createCheckpoint(long lifetime) {
            Map<String, String> props = new HashMap<>();
            props.put(CREATION_DATE, String.valueOf(clock.getTime()));
            String checkpoint = nodeStore.checkpoint(lifetime, props);

            return checkpoint;
        }

        @Override public boolean releaseCheckpoint(String id) {
            return nodeStore.release(id);
        }
    }
}
