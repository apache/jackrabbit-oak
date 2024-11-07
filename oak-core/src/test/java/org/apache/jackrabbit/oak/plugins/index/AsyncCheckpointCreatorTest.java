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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class AsyncCheckpointCreatorTest {

    @Test
    public void testAsync() {
        NodeStore store = new MemoryNodeStore();
        int minConcurrentCheckpoints = 3;
        int maxConcurrentCheckpoints = 5;
        AsyncCheckpointCreator task = new AsyncCheckpointCreator(
                store, "test", 1000, minConcurrentCheckpoints, maxConcurrentCheckpoints);
        Map<Integer, String> checkpointMap = new LinkedHashMap<>();
        for (int i = 0; i < minConcurrentCheckpoints; i++) {
            List<String> checkpointList = new ArrayList<>();
            sleepOneMillisecond();
            task.run();
            for(String checkpoint : store.checkpoints()) {
                if (!checkpointMap.containsValue(checkpoint)) {
                    checkpointMap.put(i + 1, checkpoint);
                }
                checkpointList.add(checkpoint);
            }
            Assert.assertEquals(i + 1, checkpointList.size());
        }
        // Task run post the minConcurrentCheckpoints should not result in additional
        // checkpoints since the oldest checkpoint should get deleted
        for (int j = 0; j < 2; j++) {
            List<String> checkpointList = new ArrayList<>();
            sleepOneMillisecond();
            task.run();
            for (String checkpoint : store.checkpoints()) {
                checkpointList.add(checkpoint);
            }
            Assert.assertFalse(checkpointList.contains(checkpointMap.get(j + 1)));
            Assert.assertEquals(minConcurrentCheckpoints, checkpointList.size());
        }
    }
    
    /**
     * The "oldest" checkpoint is removed, but for this to work reliably, the
     * checkpoints need to be at least 1 ms apart. So here we wait at least 1 ms,
     * such that the checkpoints are not on the same millisecond. This is a bit a
     * hack, but I think it's safer to change the test case than to change the code.
     */
    private static void sleepOneMillisecond() {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
            long time = System.currentTimeMillis();
            if (time - start >= 1) {
                break;
            }
        }
    }
}
