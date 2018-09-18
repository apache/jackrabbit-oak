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

import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.jackrabbit.oak.segment.file.Scheduler;

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
class CommitsTracker implements Closeable {
    private final boolean collectStackTraces;
    private final String[] threadGroups;
    private final ConcurrentMap<String, String> queuedWritersMap;
    private final ConcurrentMap<String, Long> commitsCountPerThreadGroup;
    private final ConcurrentMap<String, Long> commitsCountOtherThreads;
    private final ConcurrentMap<String, Long> commitsCountPerThreadGroupLastMinute;
    private final Scheduler commitsTrackerScheduler = new Scheduler("CommitsTracker background tasks");

    CommitsTracker(String[] threadGroups, int otherWritersLimit, boolean collectStackTraces) {
        this.threadGroups = threadGroups;
        this.collectStackTraces = collectStackTraces;
        this.commitsCountPerThreadGroup = new ConcurrentHashMap<>();
        this.commitsCountPerThreadGroupLastMinute = new ConcurrentHashMap<>();
        this.commitsCountOtherThreads = new ConcurrentLinkedHashMap.Builder<String, Long>()
                .maximumWeightedCapacity(otherWritersLimit).build();
        this.queuedWritersMap = new ConcurrentHashMap<>();

        commitsTrackerScheduler.scheduleWithFixedDelay("TarMK commits tracker stats resetter", 1, MINUTES,
                this::resetStatistics);
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
        String group = findGroupFor(t);

        if (group.equals("other")) {
            commitsCountOtherThreads.compute(t.getName(), (w, v) -> v == null ? 1 : v + 1);
        }

        commitsCountPerThreadGroup.compute(group, (w, v) -> v == null ? 1 : v + 1);
    }

    private String findGroupFor(Thread t) {
        if (threadGroups == null) {
            return "other";
        }
        
        for (String group : threadGroups) {
            if (t.getName().matches(group)) {
                return group;
            }
        }

        return "other";
    }

    private void resetStatistics() {
        commitsCountPerThreadGroupLastMinute.clear();
        commitsCountPerThreadGroupLastMinute.putAll(commitsCountPerThreadGroup);
        commitsCountPerThreadGroup.clear();
        commitsCountOtherThreads.clear();
    }

    @Override
    public void close() {
        commitsTrackerScheduler.close();
    }

    public Map<String, String> getQueuedWritersMap() {
        return new HashMap<>(queuedWritersMap);
    }

    public Map<String, Long> getCommitsCountPerGroupLastMinute() {
        return new HashMap<>(commitsCountPerThreadGroupLastMinute);
    }

    public Map<String, Long> getCommitsCountOthers() {
        return new HashMap<>(commitsCountOtherThreads);
    }
    
    Map<String, Long> getCommitsCountPerGroup() {
        return new HashMap<>(commitsCountPerThreadGroup);
    }
}
