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

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Math.min;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

public class CommitsTrackerTest {

    private static class CommitTask {
        private final CommitsTracker commitsTracker;
        private final Thread thread;

        CommitTask(CommitsTracker commitsTracker) {
            this.commitsTracker = commitsTracker;
            this.thread = new Thread();
        }

        CommitTask(CommitsTracker commitsTracker, String threadName) {
            this.commitsTracker = commitsTracker;
            this.thread = new Thread(threadName);
        }

        public void queued() {
            commitsTracker.trackQueuedCommitOf(thread);
        }

        public void dequeue() {
            commitsTracker.trackDequedCommitOf(thread);
        }

        public void executed() {
            commitsTracker.trackExecutedCommitOf(thread);
        }

        public String getThreadName() {
            return thread.getName();
        }
    }

    @Test
    public void testCommitsCountOthers() throws InterruptedException {
        CommitsTracker commitsTracker = new CommitsTracker(new String[] {}, 10, true);

        List<CommitTask> queued = newArrayList();
        for (int k = 0; k < 20; k++) {
            CommitTask commitTask = new CommitTask(commitsTracker);
            queued.add(commitTask);
            commitTask.queued();
            assertEquals(queued.size(), commitsTracker.getQueuedWritersMap().size());
            assertEquals(0, commitsTracker.getCommitsCountOthers().size());
            assertTrue(commitsTracker.getCommitsCountPerGroupLastMinute().isEmpty());
        }

        List<CommitTask> executed = newArrayList();
        for (int k = 0; k < 13; k ++) {
            CommitTask commitTask = queued.remove(0);
            executed.add(commitTask);
            commitTask.dequeue();

            commitTask.executed();
            assertEquals(queued.size(), commitsTracker.getQueuedWritersMap().size());
            assertEquals(min(10, executed.size()), commitsTracker.getCommitsCountOthers().size());
            assertTrue(commitsTracker.getCommitsCountPerGroupLastMinute().isEmpty());
        }
    }

    @Test
    public void testCommitsCountPerGroup() throws InterruptedException {
        String[] groups = new String[] { "Thread-1.*", "Thread-2.*", "Thread-3.*" };
        CommitsTracker commitsTracker = new CommitsTracker(groups, 10, false);

        for (int k = 0; k < 40; k++) {
            CommitTask commitTask = new CommitTask(commitsTracker, "Thread-" + (10 + k));
            commitTask.queued();
            commitTask.dequeue();
            commitTask.executed();
        }

        Map<String, Long> commitsCountPerGroup = commitsTracker.getCommitsCountPerGroupLastMinute();
        assertEquals(3, commitsCountPerGroup.size());

        for (String group : groups) {
            Long groupCount = commitsCountPerGroup.get(group);
            assertNotNull(groupCount);
            assertEquals(10, (long) groupCount);
        }

        assertEquals(10, commitsTracker.getCommitsCountOthers().size());
    }
}
