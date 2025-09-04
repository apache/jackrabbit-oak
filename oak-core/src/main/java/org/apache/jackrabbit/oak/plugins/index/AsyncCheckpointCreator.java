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


import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Calendar;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * This class is responsible for creating and deleting checkpoints asynchronously.
 * The number of minimum concurrent checkpoints to keep in the system, along with the default lifetime of a checkpoint
 * can be configured.
 * When executed, this class should create one checkpoint in a single run with a configurable name.
 * Following the creation of the checkpoint, it should try to delete checkpoints with the given name,
 * in case the total number of such checkpoints is greater than the configured minimum concurrent checkpoints.
 * By default, this task is registered using AsyncCheckpointService
 */
public class AsyncCheckpointCreator implements Runnable {

    /**
     * Name of service property which determines the name of this Async task
     */
    public static final String PROP_ASYNC_NAME = "oak.checkpoint.async";

    private final String name;
    private final long checkpointLifetimeInSeconds;
    private final long minConcurrentCheckpoints;
    private final long maxConcurrentCheckpoints;
    private final NodeStore store;
    public static final String CHECKPOINT_CREATOR_KEY = "creator";
    public static final String CHECKPOINT_CREATED_KEY = "created";
    public static final String CHECKPOINT_CREATED_TIMESTAMP_KEY= "created-epoch";
    public static final String CHECKPOINT_THREAD_KEY = "thread";
    public static final String CHECKPOINT_NAME_KEY = "name";
    private static final Logger log = LoggerFactory
            .getLogger(AsyncCheckpointCreator.class);

    public AsyncCheckpointCreator(@NotNull NodeStore store, @NotNull String name, long checkpointLifetimeInSeconds, long minConcurrentCheckpoints, long maxConcurrentCheckpoints) {
        this.store = store;
        this.name = name;
        this.checkpointLifetimeInSeconds = checkpointLifetimeInSeconds;
        this.minConcurrentCheckpoints = minConcurrentCheckpoints;
        // maxConcurrentCheckpoints should at least be 1 more than minConcurrentCheckpoints
        if (maxConcurrentCheckpoints <= minConcurrentCheckpoints) {
            log.warn("[{}] Max concurrent checkpoints is less than or equal to min concurrent checkpoints. " +
                    "Setting max concurrent checkpoints to min concurrent checkpoints + 1.", this.name);
            this.maxConcurrentCheckpoints = minConcurrentCheckpoints + 1;
        } else {
            this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
        }
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

    protected long getMaxConcurrentCheckpoints() {
        return maxConcurrentCheckpoints;
    }

    @Override
    public void run() {
        Map<String, Map<String, String>> filteredCheckpointMap = IndexUtils.getFilteredCheckpoints(store,
                entry -> name.equals(entry.getValue().get(CHECKPOINT_NAME_KEY)));
        // If the number of checkpoints created by this task are equal to or greater than the maxConcurrentCheckpoints, skip
        // creation of a new checkpoint.
        // This could happen in case the deletion of older checkpoints failed in multiple previous runs.
        if (filteredCheckpointMap.size() >= maxConcurrentCheckpoints) {
            log.warn("[{}] Skipping checkpoint creation as the number of concurrent checkpoints is already at max limit {}", name, maxConcurrentCheckpoints);
        } else {
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            String creationTimeStamp = String.valueOf(cal.getTimeInMillis());
            String creationTimeISOFormat = ISO8601.format(cal);
            String checkpoint = store.checkpoint(checkpointLifetimeInSeconds * 1000, Map.of(
                    CHECKPOINT_CREATOR_KEY, AsyncCheckpointCreator.class.getSimpleName(),
                    CHECKPOINT_CREATED_KEY, creationTimeISOFormat,
                    CHECKPOINT_CREATED_TIMESTAMP_KEY, creationTimeStamp,
                    CHECKPOINT_THREAD_KEY, Thread.currentThread().getName(),
                    CHECKPOINT_NAME_KEY, name));
            log.info("[{}] Created checkpoint {} with creation time {}", name, checkpoint, creationTimeISOFormat);
        }

        // Get a list of checkpoints filtered on the basis of CHECKPOINT_NAME_KEY (name). This is done using the
        // getFilteredCheckpoints in the IndexUtils, which gets all the checkpoints in the node store and then filters the list based on
        // the provided predicate using the checkpoint info map associated with the checkpoints.
        // The filtered checkpoints list is then sorted based on the CHECKPOINT_CREATED_TIMESTAMP_KEY (created-epoch).
        // Both the initial filtering and sorting is done based on the information from the associated checkpoint info map of a given checkpoint.
        Set<String> sortedCheckpointSet = IndexUtils.getSortedCheckpointMap(IndexUtils.getFilteredCheckpoints(store,
                entry -> name.equals(entry.getValue().get(CHECKPOINT_NAME_KEY))), CHECKPOINT_CREATED_TIMESTAMP_KEY).keySet();
        int counter = sortedCheckpointSet.size();
        // Iterate over the sortedCheckpointSet which is sorted based on the creation timestamp -> the oldest first
        // We try and delete the checkpoints as long as the counter is greater than minConcurrentCheckpoints
        // This ensures that the system always has concurrent checkpoints equal to or greater than the configured minConcurrentCheckpoints
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
            // Decrement the counter irrespective of the outcome of the checkpoint deletion.
            // If we don't decrement the counter in case of a failure while trying to delete a checkpoint,
            // the next iteration will try to delete a comparatively newer checkpoint and keep the older one in the system (which we don't want).
            // The checkpoint that failed to get deleted in this case should get deleted in the next run.
            counter--;
        }

    }

}
