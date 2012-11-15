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
package org.apache.jackrabbit.mongomk.impl.command;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Tests for concurrent conflicting commit scenarios.
 */
public class ConcurrentConflictingCommitCommandTest extends BaseMongoMicroKernelTest {

    /**
     * Test that concurrent update to root ends up with a conflict exception.
     */
    @Test
    public void rootUpdate() throws Exception {
        int n = 2;
        CountDownLatch latch = new CountDownLatch(n - 1);
        CommitCommand cmd1 = new WaitingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a1\" : {}", null), latch);
        CommitCommand cmd2 = new NotifyingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a2\" : {}", null), latch);

        ExecutorService executorService = Executors.newFixedThreadPool(n);
        Future<Long> future1 = executorService.submit(new CommitCallable(cmd1));
        Thread.sleep(1000);
        Future<Long> future2 = executorService.submit(new CommitCallable(cmd2));
        try {
            future1.get();
            future2.get();
        } catch (Exception expected) {
            // cmd2 updated root by adding /a2, so this is expected.
        }
    }

    /**
     * Test that a commit does not end up with a conflict exception when there
     * is another concurrent commit with a disjoint affected path.
     */
    @Test
    public void subPathUpdate1() throws Exception {
        mk.commit("/", "+\"a1\" : {}", null, null);
        mk.commit("/", "+\"a2\" : {}", null, null);

        int n = 2;
        CountDownLatch latch = new CountDownLatch(n - 1);
        CommitCommand cmd1 = new WaitingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a1/b1\" : {}", null), latch);
        CommitCommand cmd2 = new NotifyingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a2/b1\" : {}", null), latch);

        ExecutorService executorService = Executors.newFixedThreadPool(n);
        Future<Long> future1 = executorService.submit(new CommitCallable(cmd1));
        Thread.sleep(1000);
        Future<Long> future2 = executorService.submit(new CommitCallable(cmd2));
        try {
            future1.get();
            future2.get();
        } catch (Exception e) {
            fail("Not expected: " + e);
        }
    }

    /**
     * Test that a commit does not end up with a conflict exception when there
     * are two concurrent commits with disjoint affected paths.
     */
    @Test
    public void subPathUpdate2() throws Exception {
        mk.commit("/", "+\"a1\" : {}", null, null);
        mk.commit("/", "+\"a2\" : {}", null, null);
        mk.commit("/", "+\"a3\" : {}", null, null);

        int n = 3;
        CountDownLatch latch = new CountDownLatch(n - 1);
        CommitCommand cmd1 = new WaitingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a1/b1\" : {}", null), latch);
        CommitCommand cmd2 = new NotifyingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a2/b1\" : {}", null), latch);
        CommitCommand cmd3 = new NotifyingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a3/b1\" : {}", null), latch);


        ExecutorService executorService = Executors.newFixedThreadPool(n);
        Future<Long> future1 = executorService.submit(new CommitCallable(cmd1));
        Thread.sleep(1000);
        Future<Long> future2 = executorService.submit(new CommitCallable(cmd2));
        Future<Long> future3 = executorService.submit(new CommitCallable(cmd3));
        try {
            future1.get();
            future2.get();
            future3.get();
        } catch (Exception e) {
            fail("Not expected: " + e);
        }
    }

    /**
     * Test that a commit ends up with a conflict exception when there are two
     * concurrent commits with one disjoint but other overlapping affected path.
     */
    @Test
    public void subPathUpdate3() throws Exception {
        mk.commit("/", "+\"a1\" : {}", null, null);
        mk.commit("/", "+\"a2\" : {}", null, null);

        int n = 3;
        CountDownLatch latch = new CountDownLatch(n - 1);
        CommitCommand cmd1 = new WaitingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a1/b1\" : {}", null), latch);
        CommitCommand cmd2 = new NotifyingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a2/b1\" : {}", null), latch);
        CommitCommand cmd3 = new NotifyingCommitCommand(getNodeStore(),
                CommitBuilder.build("/", "+\"a1/b2\" : {}", null), latch);


        ExecutorService executorService = Executors.newFixedThreadPool(n);
        Future<Long> future1 = executorService.submit(new CommitCallable(cmd1));
        Thread.sleep(1000);
        Future<Long> future2 = executorService.submit(new CommitCallable(cmd2));
        Future<Long> future3 = executorService.submit(new CommitCallable(cmd3));
        try {
            future1.get();
            future2.get();
            future3.get();
        } catch (Exception expected) {
            // cmd1 and cmd3 update the same root, so this is expected.
        }
    }

    /**
     * A CommitCommand that simply waits on the waitLock until notified.
     */
    private static class WaitingCommitCommand extends CommitCommand {

        private final CountDownLatch latch;

        public WaitingCommitCommand(MongoNodeStore nodeStore, Commit commit,
                CountDownLatch latch) {
            super(nodeStore, commit);
            this.latch = latch;
        }

        @Override
        protected boolean saveAndSetHeadRevision() throws Exception {
            try {
                latch.await();
                return super.saveAndSetHeadRevision();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    /**
     * A CommitCommand that notifies on the waitLock.
     */
    private static class NotifyingCommitCommand extends CommitCommand {

        private final CountDownLatch latch;

        public NotifyingCommitCommand(MongoNodeStore nodeStore, Commit commit,
                CountDownLatch latch) {
            super(nodeStore, commit);
            this.latch = latch;
        }

        @Override
        protected boolean saveAndSetHeadRevision() throws Exception {
            try {
                boolean result = super.saveAndSetHeadRevision();
                return result;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            } finally {
                latch.countDown();
            }
        }
    }

    /**
     * A Callable test simply executes the command.
     */
    private static class CommitCallable implements Callable<Long> {

        private final CommitCommand command;

        public CommitCallable(CommitCommand  command) {
            this.command = command;
        }

        @Override
        public Long call() throws Exception {
            return command.execute();
        }
    }
}
