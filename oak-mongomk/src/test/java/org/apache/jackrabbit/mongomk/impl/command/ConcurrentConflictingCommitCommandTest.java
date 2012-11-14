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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.junit.Ignore;
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
        Commit commit1 = CommitBuilder.build("/", "+\"a\" : {}", null);
        Commit commit2 = CommitBuilder.build("/", "+\"b\" : {}", null);
        Object waitLock = new Object();
        CommitCommand cmd1 = new WaitingCommitCommand(getNodeStore(), commit1, waitLock);
        CommitCommand cmd2 = new NotifyingCommitCommand(getNodeStore(), commit2, waitLock);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<Long> future1 = executorService.submit(new CommitCallable(cmd1));
        Thread.sleep(1000);
        executorService.submit(new CommitCallable(cmd2));
        try {
            future1.get();
        }
        catch (Exception expected) {
            // commit2 updated root by adding /b, so this is expected.
        }
    }

    /**
     * Test that concurrent update to subpaths does not end up with a conflict
     * exception.
     */
    @Test
    @Ignore // FIXME - See OAK-440
    public void subPathUpdate() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);
        mk.commit("/", "+\"b\" : {}", null, null);

        Commit commit1 = CommitBuilder.build("/", "+\"a/c\" : {}", null);
        Commit commit2 = CommitBuilder.build("/", "+\"b/d\" : {}", null);
        Object waitLock = new Object();
        CommitCommand cmd1 = new WaitingCommitCommand(getNodeStore(), commit1, waitLock);
        CommitCommand cmd2 = new NotifyingCommitCommand(getNodeStore(), commit2, waitLock);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
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
     * A CommitCommand that simply waits on the waitLock until notified.
     */
    private static class WaitingCommitCommand extends CommitCommand {

        private final Object waitLock;

        public WaitingCommitCommand(MongoNodeStore nodeStore, Commit commit, Object waitLock) {
            super(nodeStore, commit);
            this.waitLock = waitLock;
        }

        @Override
        protected boolean saveAndSetHeadRevision() throws Exception {
            try {
                synchronized (waitLock) {
                    waitLock.wait();
                }
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

        private final Object waitLock;

        public NotifyingCommitCommand(MongoNodeStore nodeStore, Commit commit, Object waitLock) {
            super(nodeStore, commit);
            this.waitLock = waitLock;
        }

        @Override
        protected boolean saveAndSetHeadRevision() throws Exception {
            try {
                boolean result = super.saveAndSetHeadRevision();
                synchronized (waitLock) {
                    waitLock.notifyAll();
                }
                return result;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
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
