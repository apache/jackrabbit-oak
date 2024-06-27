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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Calendar;
import java.util.Set;

public class AsyncCheckpointCreator implements Runnable {

    /**
     * Name of service property which determines the name of Async task
     */
    public static final String PROP_ASYNC_NAME = "oak.checkpoint.async";

    private final String name;
    private long checkpointLifetimeInSeconds;
    private long minConcurrentCheckpoints;
    private final NodeStore store;
    public static final String CHECKPOINT_CREATOR_KEY = "creator";
    public static final String CHECKPOINT_CREATED_KEY = "created";
    public static final String CHECKPOINT_CREATED_TIMESTAMP_KEY= "created-epoch";
    public static final String CHECKPOINT_THREAD_KEY = "thread";
    public static final String CHECKPOINT_NAME_KEY = "name";
    private static final Logger log = LoggerFactory
            .getLogger(AsyncCheckpointCreator.class);

    public AsyncCheckpointCreator(NodeStore store, String name, long checkpointLifetimeInSeconds, long minConcurrentCheckpoints) {
        this.store = store;
        this.name = name;
        this.checkpointLifetimeInSeconds = checkpointLifetimeInSeconds;
        this.minConcurrentCheckpoints = minConcurrentCheckpoints;
    }

    public String getName() {
        return name;
    }

    protected long getCheckpointLifetimeInSeconds() {
        return checkpointLifetimeInSeconds;
    }

    protected long getMinConcurrentCheckpoints() {
        return minConcurrentCheckpoints;
    }

    @Override
    public void run() {
        Calendar cal = Calendar.getInstance();
        String creationTimeStamp = String.valueOf(cal.getTimeInMillis());
        String creationTimeISOFormat = ISO8601.format(cal);
        String checkpoint = store.checkpoint(checkpointLifetimeInSeconds * 1000, ImmutableMap.of(
                CHECKPOINT_CREATOR_KEY, AsyncCheckpointCreator.class.getSimpleName(),
                CHECKPOINT_CREATED_KEY, creationTimeISOFormat,
                CHECKPOINT_CREATED_TIMESTAMP_KEY, creationTimeStamp,
                CHECKPOINT_THREAD_KEY, Thread.currentThread().getName(),
                CHECKPOINT_NAME_KEY, name));
        log.info("[{}] Created checkpoint {} with creation time {}", name, checkpoint, creationTimeISOFormat);

        // Get a sorted checkpoint set on created-epoch field from the list of filtered checkpoint based on the creating process.
        Set<String> sortedCheckpointSet = IndexUtils.getSortedCheckpointMap(IndexUtils.getFilteredCheckpoints(store,
                entry -> name.equals(entry.getValue().get(CHECKPOINT_NAME_KEY))), CHECKPOINT_CREATED_TIMESTAMP_KEY).keySet();
        int counter = sortedCheckpointSet.size();
        // Iterate over the sortedCheckpointSet and delete any checkpoints more than concurrentCheckpoints
        for (String cp : sortedCheckpointSet) {
            // Delete the checkpoint as long as the checkpoint count is greater than concurrentCheckpoints
            if (counter > minConcurrentCheckpoints) {
                if(store.release(cp) ) {
                    log.info("[{}] Deleted checkpoint {}", name, cp);
                } else {
                    log.warn("[{}] Unable to delete checkpoint {}. Removal will be attempted again in next run.", name, cp);
                }
            } else {
                break;
            }
            counter --;
        }

    }

}
