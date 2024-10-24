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

import static java.lang.Math.min;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.segment.CommitsTracker.Commit;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Test;

public class CommitsTrackerTest {

    private static class CommitTask {
        private final CommitsTracker commitsTracker;
        private final Thread thread;
        private final GCGeneration gcGeneration;

        CommitTask(CommitsTracker commitsTracker, GCGeneration gcGeneration) {
            this.commitsTracker = commitsTracker;
            this.thread = new Thread();
            this.gcGeneration = gcGeneration;
        }

        CommitTask(CommitsTracker commitsTracker, String threadName, GCGeneration gcGeneration) {
            this.commitsTracker = commitsTracker;
            this.thread = new Thread(threadName);
            this.gcGeneration = gcGeneration;
        }

        public void queued() {
            commitsTracker.trackQueuedCommitOf(thread, () -> gcGeneration);
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

        public GCGeneration getGcGeneration() {
            return gcGeneration;
        }
    }

    @Test
    public void testCommitsCountOthers() throws InterruptedException {
        final int OTHER_WRITERS_LIMIT = 10;
        CommitsTracker commitsTracker = new CommitsTracker(new String[] {}, OTHER_WRITERS_LIMIT);

        List<CommitTask> queued = new ArrayList<>();
        for (int k = 0; k < 20; k++) {
            CommitTask commitTask = new CommitTask(commitsTracker, newGCGeneration(k, k, false));
            queued.add(commitTask);
            commitTask.queued();
            assertNull(commitsTracker.getCurrentWriter());
            assertEquals(queued.size(), commitsTracker.getQueuedWritersMap().size());
            assertEquals(0, commitsTracker.getCommitsCountOthers().size());
            assertTrue(commitsTracker.getCommitsCountPerGroupLastMinute().isEmpty());
        }

        List<CommitTask> executed = new ArrayList<>();
        for (int k = 0; k < OTHER_WRITERS_LIMIT + 3; k++) {
            CommitTask commitTask = queued.remove(0);
            executed.add(commitTask);
            commitTask.dequeue();
            Commit currentWriter = commitsTracker.getCurrentWriter();
            assertNotNull(currentWriter);
            assertEquals(commitTask.getGcGeneration(), currentWriter.getGCGeneration());
            assertEquals(commitTask.getThreadName(), commitsTracker.getCurrentWriter().getThreadName());

            commitTask.executed();
            assertNull(commitsTracker.getCurrentWriter());
            assertEquals(queued.size(), commitsTracker.getQueuedWritersMap().size());
            assertEquals(min(OTHER_WRITERS_LIMIT, executed.size()), commitsTracker.getCommitsCountOthers().size());
            assertTrue(commitsTracker.getCommitsCountPerGroupLastMinute().isEmpty());
        }
    }

    @Test
    public void testCommitsCountPerGroup() throws InterruptedException {
        String[] groups = new String[] { "Thread-1.*", "Thread-2.*", "Thread-3.*" };
        CommitsTracker commitsTracker = new CommitsTracker(groups, 10);

        for (int k = 0; k < 40; k++) {
            CommitTask commitTask = new CommitTask(
                    commitsTracker,
                    "Thread-" + (10 + k),
                    GCGeneration.NULL);
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
