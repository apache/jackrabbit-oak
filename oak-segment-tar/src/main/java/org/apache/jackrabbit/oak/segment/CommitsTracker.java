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

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Queues.newConcurrentLinkedQueue;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
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
    private final String[] threadGroups;
    private final int otherWritersLimit;
    private final ConcurrentMap<String, Commit> queuedWritersMap;
    private final Queue<Commit> commits = newConcurrentLinkedQueue();

    static final class Commit {
        private final long timeStamp;
        private final String threadName;
        private final WeakReference<Thread> thread;

        Commit(long timeStamp, Thread thread) {
            this.timeStamp = timeStamp;
            this.threadName = thread.getName();
            this.thread = new WeakReference<>(thread);
        }

        String getStackTrace() {
            Thread t = thread.get();
            if (t != null) {
                StringBuilder threadDetails = new StringBuilder();
                Stream.of(t.getStackTrace()).forEach(threadDetails::append);
                return threadDetails.toString();
            } else {
                return "N/A";
            }
        }

        String getThreadName() {
            return threadName;
        }

        long getTimeStamp() {
            return timeStamp;
        }
    }

    CommitsTracker(String[] threadGroups, int otherWritersLimit) {
        this.threadGroups = threadGroups;
        this.otherWritersLimit = otherWritersLimit;
        this.queuedWritersMap = new ConcurrentHashMap<>();
    }

    public void trackQueuedCommitOf(Thread t) {
        queuedWritersMap.put(t.getName(), new Commit(System.currentTimeMillis(), t));
    }

    public void trackDequedCommitOf(Thread t) {
        queuedWritersMap.remove(t.getName());
    }

    public void trackExecutedCommitOf(Thread thread) {
        long t = System.currentTimeMillis();
        final Iterator<Commit> it = commits.iterator();

        // Purge the queue
        // Avoiding removeIf allows us to bail out early. See OAK-7885
        while (it.hasNext()) {
            if (it.next().timeStamp < t - 60000) {
                it.remove();
            } else {
                break;
            }
        }
        commits.offer(new Commit(t, thread));
    }

    public Map<String, Commit> getQueuedWritersMap() {
        return new HashMap<>(queuedWritersMap);
    }

    private String findGroupFor(String thread) {
        if (threadGroups == null) {
            return "other";
        }

        for (String group : threadGroups) {
            if (thread.matches(group)) {
                return group;
            }
        }

        return "other";
    }

    public Map<String, Long> getCommitsCountPerGroupLastMinute() {
        Map<String, Long> commitsPerGroup = newHashMap();
        long t = System.currentTimeMillis() - 60000;
        for (Commit commit : commits) {
            if (commit.timeStamp > t) {
                String group = findGroupFor(commit.threadName);
                if (!"other".equals(group)) {
                    commitsPerGroup.compute(group, (w, v) -> v == null ? 1 : v + 1);
                }
            }
        }
        return commitsPerGroup;
    }

    public Map<String, Long> getCommitsCountOthers() {
        Map<String, Long> commitsOther = new ConcurrentLinkedHashMap.Builder<String, Long>()
                .maximumWeightedCapacity(otherWritersLimit).build();
        long t = System.currentTimeMillis() - 60000;
        for (Commit commit : commits) {
            if (commit.timeStamp > t) {
                String group = findGroupFor(commit.threadName);
                if ("other".equals(group)) {
                    commitsOther.compute(commit.threadName, (w, v) -> v == null ? 1 : v + 1);
                }
            }
        }
        return commitsOther;
    }
}
