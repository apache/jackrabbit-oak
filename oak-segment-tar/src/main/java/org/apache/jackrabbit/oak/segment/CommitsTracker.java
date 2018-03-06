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

package org.apache.jackrabbit.oak.segment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * A simple tracker for the source of commits (writes) in
 * {@link SegmentNodeStore}. It provides two basic functionalities:
 * <ul>
 * <li>exposes the number of commits executed per thread</li>
 * <li>exposes the threads (and possibly their details - i.e., stack traces)
 * currently waiting on the commit semaphore
 * </ul>
 * 
 * This class delegates thread-safety to its underlying state variables. 
 */
class CommitsTracker {
    private static final int DEFAULT_COMMITS_COUNT_MAP_SIZE = 20;
    private static final boolean DEFAULT_COLLECT_STACK_TRACES = true;

    private volatile boolean collectStackTraces;

    private final ConcurrentMap<String, String> queuedWritersMap;
    private final ConcurrentMap<String, Long> commitsCountMap;

    CommitsTracker() {
        this(DEFAULT_COMMITS_COUNT_MAP_SIZE, DEFAULT_COLLECT_STACK_TRACES);
    }

    CommitsTracker(int commitsCountMapMaxSize, boolean collectStackTraces) {
        this.collectStackTraces = collectStackTraces;
        this.commitsCountMap = new ConcurrentLinkedHashMap.Builder<String, Long>()
                .maximumWeightedCapacity(commitsCountMapMaxSize).build();
        this.queuedWritersMap = new ConcurrentHashMap<>();
    }

    public void trackQueuedCommitOf(Thread t) {
        String writerDetails = "N/A";
        if (collectStackTraces) {
            StringBuilder threadDetails = new StringBuilder();
            Stream.of(t.getStackTrace()).forEach(threadDetails::append);
            writerDetails = threadDetails.toString();
        }

        queuedWritersMap.put(t.getName(), writerDetails);
    }

    public void trackDequedCommitOf(Thread t) {
        queuedWritersMap.remove(t.getName());
    }

    public void trackExecutedCommitOf(Thread t) {
        commitsCountMap.compute(t.getName(), (w, v) -> v == null ? 1 : v + 1);
    }

    public void setCollectStackTraces(boolean flag) {
        this.collectStackTraces = flag;
    }
    
    public Map<String, String> getQueuedWritersMap() {
        return new HashMap<>(queuedWritersMap);
    }

    public Map<String, Long> getCommitsCountMap() {
        return new HashMap<>(commitsCountMap);
    }

}
