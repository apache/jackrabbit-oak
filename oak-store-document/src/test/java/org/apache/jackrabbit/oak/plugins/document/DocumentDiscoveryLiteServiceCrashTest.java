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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import junitx.util.PrivateAccessor;

import static org.junit.Assert.assertNotNull;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class DocumentDiscoveryLiteServiceCrashTest
        extends BaseDocumentDiscoveryLiteServiceTest {

    private static final int TEST_WAIT_TIMEOUT = 10000;

    private Clock clock;
    private DocumentStore store;
    private String wd1;
    private DocumentNodeStore ns1;
    private String wd2;
    private DocumentNodeStore ns2;

    @Before
    public void setup() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        ClusterNodeInfo.setClock(clock);
        store = new MemoryDocumentStore();
        wd1 = UUID.randomUUID().toString();
        wd2 = UUID.randomUUID().toString();
    }

    @After
    public void reset() {
        ns1.dispose();
        ns2.dispose();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void testTwoNodesWithCrashAndLongduringRecovery() throws Throwable {
        doTestTwoNodesWithCrashAndLongduringDeactivation(false);
    }

    @Test
    public void testTwoNodesWithCrashAndLongduringRecoveryAndBacklog() throws Throwable {
        doTestTwoNodesWithCrashAndLongduringDeactivation(true);
    }

    private void doTestTwoNodesWithCrashAndLongduringDeactivation(boolean withBacklog) throws Throwable {
        ns1 = newDocumentNodeStore(store, wd1);
        SimplifiedInstance s1 = createInstance(ns1, wd1);
        ViewExpectation e1 = new ViewExpectation(s1);
        e1.setActiveIds(ns1.getClusterId());
        waitFor(e1, TEST_WAIT_TIMEOUT, "first should see itself active");
        ns1.runBackgroundOperations();

        ns2 = newDocumentNodeStore(store, wd2);
        SimplifiedInstance s2 = createInstance(ns2, wd2);
        ViewExpectation e2 = new ViewExpectation(s2);
        e2.setActiveIds(ns1.getClusterId(), ns2.getClusterId());
        waitFor(e2, TEST_WAIT_TIMEOUT, "second should see both active");
        ns2.runBackgroundOperations();

        ns1.runBackgroundReadOperations();
        // now ns1 should also see both active
        ViewExpectation e3 = new ViewExpectation(s1);
        e3.setActiveIds(ns1.getClusterId(), ns2.getClusterId());
        waitFor(e3, TEST_WAIT_TIMEOUT, "first should see both as active");

        // before crashing s2, make sure that s1's lastRevRecovery thread
        // doesn't run
        s1.stopLastRevThread();
        if (withBacklog) {
            // if we want to do backlog testing, then s2 should write
            // something
            // before it crashes, so here it comes:
            s2.addNode("/foo/bar");
            s2.setProperty("/foo/bar", "prop", "value");
        }

        // then wait 2 sec
        clock.waitUntil(clock.getTime() + 2000);

        s2.crash();

        // then wait 2 sec
        clock.waitUntil(clock.getTime() + 2000);

        // at this stage, while s2 has crashed, we have stopped s1's
        // lastRevRecoveryThread, so we should still see both as active
        logger.info(s1.getClusterViewStr());
        final ViewExpectation expectation1AfterCrashBeforeLastRevRecovery = new ViewExpectation(s1);
        expectation1AfterCrashBeforeLastRevRecovery.setActiveIds(ns1.getClusterId(), ns2.getClusterId());
        waitFor(expectation1AfterCrashBeforeLastRevRecovery, TEST_WAIT_TIMEOUT, "first should still see both as active");

        // the next part is a bit tricky: we want to fine-control the
        // lastRevRecoveryThread's acquire/release locking.
        // the chosen way to do this is to make heavy use of mockito and two
        // semaphores:
        // when acquireRecoveryLock is called, that thread should wait for the
        // waitBeforeLocking semaphore to be released
        final MissingLastRevSeeker missingLastRevUtil = (MissingLastRevSeeker) PrivateAccessor
                .getField(s1.ns.getLastRevRecoveryAgent(), "missingLastRevUtil");
        assertNotNull(missingLastRevUtil);
        MissingLastRevSeeker mockedLongduringMissingLastRevUtil = mock(MissingLastRevSeeker.class, delegatesTo(missingLastRevUtil));
        final Semaphore waitBeforeLocking = new Semaphore(0);
        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                logger.info("going to waitBeforeLocking");
                waitBeforeLocking.acquire();
                logger.info("done with waitBeforeLocking");
                return missingLastRevUtil.acquireRecoveryLock((Integer) invocation.getArguments()[0],
                        (Integer) invocation.getArguments()[1]);
            }
        }).when(mockedLongduringMissingLastRevUtil).acquireRecoveryLock(anyInt(), anyInt());
        PrivateAccessor.setField(s1.ns.getLastRevRecoveryAgent(), "missingLastRevUtil", mockedLongduringMissingLastRevUtil);

        // so let's start the lastRevThread again and wait for that
        // waitBeforeLocking semaphore to be hit
        s1.startLastRevThread();
        waitFor(new Expectation() {

            @Override
            public String fulfilled() throws Exception {
                if (!waitBeforeLocking.hasQueuedThreads()) {
                    return "no thread queued";
                }
                return null;
            }

        }, TEST_WAIT_TIMEOUT, "lastRevRecoveryThread should acquire a lock");

        // at this stage the crashed s2 is still not in recovery mode, so let's
        // check:
        logger.info(s1.getClusterViewStr());
        final ViewExpectation expectation1AfterCrashBeforeLastRevRecoveryLocking = new ViewExpectation(s1);
        expectation1AfterCrashBeforeLastRevRecoveryLocking.setActiveIds(ns1.getClusterId(), ns2.getClusterId());
        waitFor(expectation1AfterCrashBeforeLastRevRecoveryLocking, TEST_WAIT_TIMEOUT, "first should still see both as active");

        // one thing, before we let the waitBeforeLocking go, setup the release
        // semaphore/mock:
        final Semaphore waitBeforeUnlocking = new Semaphore(0);
        Mockito.doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) throws InterruptedException {
                logger.info("Going to waitBeforeUnlocking");
                waitBeforeUnlocking.acquire();
                logger.info("Done with waitBeforeUnlocking");
                missingLastRevUtil.releaseRecoveryLock(
                        (Integer) invocation.getArguments()[0],
                        (Boolean) invocation.getArguments()[1]);
                return null;
            }
        }).when(mockedLongduringMissingLastRevUtil).releaseRecoveryLock(anyInt(), anyBoolean());

        // let go (or tschaedere loh)
        waitBeforeLocking.release();

        // then, right after we let the waitBeforeLocking semaphore go, we
        // should see s2 in recovery mode
        final ViewExpectation expectation1AfterCrashWhileLastRevRecoveryLocking = new ViewExpectation(s1);
        expectation1AfterCrashWhileLastRevRecoveryLocking.setActiveIds(ns1.getClusterId());
        expectation1AfterCrashWhileLastRevRecoveryLocking.setDeactivatingIds(ns2.getClusterId());
        waitFor(expectation1AfterCrashWhileLastRevRecoveryLocking, TEST_WAIT_TIMEOUT, "first should still see s2 as recovering");

        // ok, meanwhile, the lastRevRecoveryAgent should have hit the ot
        waitFor(new Expectation() {

            @Override
            public String fulfilled() throws Exception {
                if (!waitBeforeUnlocking.hasQueuedThreads()) {
                    return "no thread queued";
                }
                return null;
            }

        }, TEST_WAIT_TIMEOUT, "lastRevRecoveryThread should want to release a lock");

        // so then, we should still see the same state
        waitFor(expectation1AfterCrashWhileLastRevRecoveryLocking, TEST_WAIT_TIMEOUT, "first should still see s2 as recovering");

        logger.info("Waiting 2 sec");
        clock.waitUntil(clock.getTime() + 2000);
        logger.info("Waiting done");

        // first, lets check to see what the view looks like - should be
        // unchanged:
        waitFor(expectation1AfterCrashWhileLastRevRecoveryLocking, TEST_WAIT_TIMEOUT, "first should still see s2 as recovering");

        // let waitBeforeUnlocking go
        logger.info("releasing waitBeforeUnlocking, state: " + s1.getClusterViewStr());
        waitBeforeUnlocking.release();
        logger.info("released waitBeforeUnlocking");

        if (!withBacklog) {
            final ViewExpectation expectationWithoutBacklog = new ViewExpectation(s1);
            expectationWithoutBacklog.setActiveIds(ns1.getClusterId());
            expectationWithoutBacklog.setInactiveIds(ns2.getClusterId());
            waitFor(expectationWithoutBacklog, TEST_WAIT_TIMEOUT, "finally we should see s2 as completely inactive");
        } else {
            // wait just 2 sec to see if the bgReadThread is really stopped
            logger.info("sleeping 2 sec");
            clock.waitUntil(clock.getTime() + 2000);
            logger.info("sleeping 2 sec done, state: " + s1.getClusterViewStr());

            // when that's the case, check the view - it should now be in a
            // special 'final=false' mode
            final ViewExpectation expectationBeforeBgRead = new ViewExpectation(s1);
            expectationBeforeBgRead.setActiveIds(ns1.getClusterId());
            expectationBeforeBgRead.setDeactivatingIds(ns2.getClusterId());
            expectationBeforeBgRead.setFinal(false);
            waitFor(expectationBeforeBgRead, TEST_WAIT_TIMEOUT, "first should only see itself after shutdown");

            // ook, now we explicitly do a background read to get out of the
            // backlog situation
            ns1.runBackgroundReadOperations();

            final ViewExpectation expectationAfterBgRead = new ViewExpectation(s1);
            expectationAfterBgRead.setActiveIds(ns1.getClusterId());
            expectationAfterBgRead.setInactiveIds(ns2.getClusterId());
            waitFor(expectationAfterBgRead, TEST_WAIT_TIMEOUT, "finally we should see s2 as completely inactive");
        }
    }

    private DocumentNodeStore newDocumentNodeStore(DocumentStore store,
                                                   String workingDir) {
        String prevWorkingDir = ClusterNodeInfo.WORKING_DIR;
        try {
            // ensure that we always get a fresh cluster[node]id
            ClusterNodeInfo.WORKING_DIR = workingDir;

            return new DocumentMK.Builder()
                    .clock(clock)
                    .setAsyncDelay(0)
                    .setDocumentStore(store)
                    .setLeaseCheck(false)
                    .getNodeStore();
        } finally {
            ClusterNodeInfo.WORKING_DIR = prevWorkingDir;
        }
    }

}
