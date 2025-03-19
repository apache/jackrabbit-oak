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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Calendar;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class IndexUtilsTest {
    
    // all relevant package TLDs
    private static final String[] ALL_CLASSES_IGNORED = new String[] {"org", "com", "sun", "jdk", "java"};
    
    // all packages used with Oak
    private static final String[] OAK_CLASSES_IGNORED = new String[] {"org.apache.jackrabbit", "java.lang", "sun.reflect", "jdk"};

    private static final String CHECKPOINT_CREATOR_KEY = "creator";

    private static final String CHECKPOINT_CREATED_TIMESTAMP_KEY = "created";

    @Test
    public void asyncName() throws Exception {
        assertNull(IndexUtils.getAsyncLaneName(EMPTY_NODE, "/fooIndex"));

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("async", List.of("async2", "sync"), Type.STRINGS);
        assertEquals("async2", IndexUtils.getAsyncLaneName(builder.getNodeState(), "/fooIndex"));

        builder.setProperty("async", List.of("async3"), Type.STRINGS);
        assertEquals("async3", IndexUtils.getAsyncLaneName(builder.getNodeState(), "/fooIndex"));
    }

    @Test
    public void getCaller() {
        assertNotNull(IndexUtils.getCaller(null));
        assertNotNull(IndexUtils.getCaller(new String[0]));
        
        assertEquals("(internal)",IndexUtils.getCaller(ALL_CLASSES_IGNORED));
        
        String caller = IndexUtils.getCaller(OAK_CLASSES_IGNORED);
        assertTrue(caller.startsWith("org.junit.runners"));
    }

    @Test
    public void checkpointFilterAndSorting() throws Exception {
        NodeStore store = null;
        Set<String> checkpointSet = new LinkedHashSet<>();
        try {
            store = new MemoryNodeStore();

            // Check all works fines if no checkpoints present
            Map<String, Map<String, String>> noCheckpointMap = IndexUtils.getFilteredCheckpoints(store, entry -> "checkpoint-helper-test-process-non-existing".equals(entry.getValue().get(CHECKPOINT_CREATOR_KEY)));
            assertEquals(0, noCheckpointMap.size());

            // Create checkpoints
            String checkpoint1 = store.checkpoint(1800000L, getMetaDataMap("checkpoint-helper-test-process"));
            checkpointSet.add(checkpoint1);
            Thread.sleep(1000);
            String checkpoint2 = store.checkpoint(1800000L, getMetaDataMap("checkpoint-helper-test-process-2"));
            checkpointSet.add(checkpoint2);
            Thread.sleep(1000);
            String checkpoint3 = store.checkpoint(1800000L, getMetaDataMap("checkpoint-helper-test-process"));
            checkpointSet.add(checkpoint3);
            Thread.sleep(1000);
            String checkpoint4 = store.checkpoint(1800000L, getMetaDataMap("checkpoint-helper-test-process"));
            checkpointSet.add(checkpoint4);

            // Check for happy case
            Map<String, Map<String, String>> filteredCheckpointMap = IndexUtils.getFilteredCheckpoints(store, entry -> "checkpoint-helper-test-process".equals(entry.getValue().get("creator")));

            assertEquals(3, filteredCheckpointMap.size());
            for (String checkpoint : filteredCheckpointMap.keySet()) {
                assertEquals("checkpoint-helper-test-process",filteredCheckpointMap.get(checkpoint).get(CHECKPOINT_CREATOR_KEY));
            }
            // Check sorting now
            Map<String, Map<String, String>> sortedCheckpointMap = IndexUtils.getSortedCheckpointMap(filteredCheckpointMap, CHECKPOINT_CREATED_TIMESTAMP_KEY);
            assertEquals(3, sortedCheckpointMap.size());
            Iterator<String> sortedCheckpointIt = sortedCheckpointMap.keySet().iterator();
            assertEquals(checkpoint1, sortedCheckpointIt.next());
            assertEquals(checkpoint3, sortedCheckpointIt.next());
            assertEquals(checkpoint4, sortedCheckpointIt.next());

            // Check for negative edge cases
            Map<String, Map<String, String>> filteredCheckpointMap2 = IndexUtils.getFilteredCheckpoints(store, entry -> "checkpoint-helper-test-process-non-existing".equals(entry.getValue().get(CHECKPOINT_CREATOR_KEY)));
            assertEquals(0, filteredCheckpointMap2.size());

            // Create a checkpoint with incorrect metadata
            Map<String, String> checkpointMetadata = getMetaDataMap("checkpoint-helper-test-process");
            checkpointMetadata.remove(CHECKPOINT_CREATED_TIMESTAMP_KEY);
            String latestFilteredCheckpointWithNoTimestamp = store.checkpoint(1800000L, checkpointMetadata);
            checkpointSet.add(latestFilteredCheckpointWithNoTimestamp);

            // Modify the predicate in the filter method here to also include the check that created exists in the checkpoint metadata
            Map<String, Map<String, String>> filteredCheckpointMap3 = IndexUtils.getFilteredCheckpoints(store, entry -> "checkpoint-helper-test-process".equals(entry.getValue().get(CHECKPOINT_CREATOR_KEY)) && entry.getValue().containsKey(CHECKPOINT_CREATED_TIMESTAMP_KEY));
            assertEquals(3, filteredCheckpointMap3.size());
            Assert.assertFalse(filteredCheckpointMap3.containsKey(latestFilteredCheckpointWithNoTimestamp));

            Map<String, String> checkpointMetadata2 = getMetaDataMap("checkpoint-helper-test-process-3");
            checkpointMetadata2.remove(CHECKPOINT_CREATED_TIMESTAMP_KEY);
            String latestFilteredCheckpointWithNoTimestamp2 = store.checkpoint(1800000L, checkpointMetadata2);
            checkpointSet.add(latestFilteredCheckpointWithNoTimestamp2);
            Map<String, Map<String, String>> filteredCheckpointMap4 = IndexUtils.getFilteredCheckpoints(store, entry -> "checkpoint-helper-test-process-3".equals(entry.getValue().get(CHECKPOINT_CREATOR_KEY)));
            assertEquals(1, filteredCheckpointMap4.size());
            Assert.assertTrue(filteredCheckpointMap4.containsKey(latestFilteredCheckpointWithNoTimestamp2));
        } finally {
            for (String checkpoint : checkpointSet) {
                store.release(checkpoint);
            }
        }

    }

    public static Map<String, String> getMetaDataMap(String creator) {
        Map<String, String> checkpointMetaData = new LinkedHashMap<>();
        checkpointMetaData.put(CHECKPOINT_CREATOR_KEY, creator);
        checkpointMetaData.put(CHECKPOINT_CREATED_TIMESTAMP_KEY, String.valueOf(Calendar.getInstance().getTimeInMillis()));
        return checkpointMetaData;
    }
}