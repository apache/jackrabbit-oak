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

import static org.junit.Assert.assertNotNull;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Semaphore;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;

import junitx.util.PrivateAccessor;

/**
 * Tests for the DocumentDiscoveryLiteService
 */
public class DocumentDiscoveryLiteServiceTest extends BaseDocumentDiscoveryLiteServiceTest {

    @Test
    public void testActivateDeactivate() throws Exception {
        // then test normal start with a DocumentNodeStore
        DocumentMK mk1 = createMK(1, 0);
        DocumentDiscoveryLiteService discoveryLite = new DocumentDiscoveryLiteService();
        PrivateAccessor.setField(discoveryLite, "nodeStore", mk1.nodeStore);
        BundleContext bc = mock(BundleContext.class);
        ComponentContext c = mock(ComponentContext.class);
        when(c.getBundleContext()).thenReturn(bc);
        discoveryLite.activate(c);
        verify(c, times(0)).disableComponent(DocumentDiscoveryLiteService.COMPONENT_NAME);
        discoveryLite.deactivate();
    }

    @Test
    public void testOneNode() throws Exception {
        final SimplifiedInstance s1 = createInstance();
        final ViewExpectation expectation = new ViewExpectation(s1);
        expectation.setActiveIds(s1.ns.getClusterId());
        waitFor(expectation, 2000, "see myself as active");
    }

    @Test
    public void testTwoNodesWithCleanShutdown() throws Exception {
        final SimplifiedInstance s1 = createInstance();
        final SimplifiedInstance s2 = createInstance();
        final ViewExpectation expectation1 = new ViewExpectation(s1);
        final ViewExpectation expectation2 = new ViewExpectation(s2);
        expectation1.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        expectation2.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1, 2000, "first should see both as active");
        waitFor(expectation2, 2000, "second should see both as active");

        s2.shutdown();
        final ViewExpectation expectation1AfterShutdown = new ViewExpectation(s1);
        expectation1AfterShutdown.setActiveIds(s1.ns.getClusterId());
        expectation1AfterShutdown.setInactiveIds(s2.ns.getClusterId());
        waitFor(expectation1AfterShutdown, 2000, "first should only see itself after shutdown");
    }

    @Test
    public void testTwoNodesWithCrash() throws Throwable {
        final SimplifiedInstance s1 = createInstance();
        final SimplifiedInstance s2 = createInstance();
        final ViewExpectation expectation1 = new ViewExpectation(s1);
        final ViewExpectation expectation2 = new ViewExpectation(s2);
        expectation1.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        expectation2.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1, 2000, "first should see both as active");
        waitFor(expectation2, 2000, "second should see both as active");

        s2.crash();

        final ViewExpectation expectation1AfterShutdown = new ViewExpectation(s1);
        expectation1AfterShutdown.setActiveIds(s1.ns.getClusterId());
        expectation1AfterShutdown.setInactiveIds(s2.ns.getClusterId());
        waitFor(expectation1AfterShutdown, 2000, "first should only see itself after shutdown");
    }

    @Test
    public void testTwoNodesWithCrashAndLongduringRecovery() throws Throwable {
        doTestTwoNodesWithCrashAndLongduringDeactivation(false);
    }

    @Test
    public void testTwoNodesWithCrashAndLongduringRecoveryAndBacklog() throws Throwable {
        doTestTwoNodesWithCrashAndLongduringDeactivation(true);
    }

    void doTestTwoNodesWithCrashAndLongduringDeactivation(boolean withBacklog) throws Throwable {
        final int TEST_WAIT_TIMEOUT = 10000;
        final SimplifiedInstance s1 = createInstance();
        final SimplifiedInstance s2 = createInstance();
        final ViewExpectation expectation1 = new ViewExpectation(s1);
        final ViewExpectation expectation2 = new ViewExpectation(s2);
        expectation1.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        expectation2.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1, TEST_WAIT_TIMEOUT, "first should see both as active");
        waitFor(expectation2, TEST_WAIT_TIMEOUT, "second should see both as active");

        // before crashing s2, make sure that s1's lastRevRecovery thread
        // doesn't run
        s1.stopLastRevThread();
        if (withBacklog) {
            // plus also stop s1's backgroundReadThread - in case we want to
            // test backlog handling
            s1.stopBgReadThread();

            // and then, if we want to do backlog testing, then s2 should write
            // something
            // before it crashes, so here it comes:
            s2.addNode("/foo/bar");
            s2.setProperty("/foo/bar", "prop", "value");
        }

        // then crash s2
        s2.crash();

        // then wait 2 sec
        Thread.sleep(2000);

        // at this stage, while s2 has crashed, we have stopped s1's
        // lastRevRecoveryThread, so we should still see both as active
        logger.info(s1.getClusterViewStr());
        final ViewExpectation expectation1AfterCrashBeforeLastRevRecovery = new ViewExpectation(s1);
        expectation1AfterCrashBeforeLastRevRecovery.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
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
        expectation1AfterCrashBeforeLastRevRecoveryLocking.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1AfterCrashBeforeLastRevRecoveryLocking, TEST_WAIT_TIMEOUT, "first should still see both as active");

        // one thing, before we let the waitBeforeLocking go, setup the release
        // semaphore/mock:
        final Semaphore waitBeforeUnlocking = new Semaphore(0);
        Mockito.doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) throws InterruptedException {
                logger.info("Going to waitBeforeUnlocking");
                waitBeforeUnlocking.acquire();
                logger.info("Done with waitBeforeUnlocking");
                missingLastRevUtil.releaseRecoveryLock((Integer) invocation.getArguments()[0]);
                return null;
            }
        }).when(mockedLongduringMissingLastRevUtil).releaseRecoveryLock(anyInt());

        // let go (or tschaedere loh)
        waitBeforeLocking.release();

        // then, right after we let the waitBeforeLocking semaphore go, we
        // should see s2 in recovery mode
        final ViewExpectation expectation1AfterCrashWhileLastRevRecoveryLocking = new ViewExpectation(s1);
        expectation1AfterCrashWhileLastRevRecoveryLocking.setActiveIds(s1.ns.getClusterId());
        expectation1AfterCrashWhileLastRevRecoveryLocking.setDeactivatingIds(s2.ns.getClusterId());
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

        logger.info("Waiting 1,5sec");
        Thread.sleep(1500);
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
            expectationWithoutBacklog.setActiveIds(s1.ns.getClusterId());
            expectationWithoutBacklog.setInactiveIds(s2.ns.getClusterId());
            waitFor(expectationWithoutBacklog, TEST_WAIT_TIMEOUT, "finally we should see s2 as completely inactive");
        } else {
            // wait just 2 sec to see if the bgReadThread is really stopped
            logger.info("sleeping 2 sec");
            Thread.sleep(2000);
            logger.info("sleeping 2 sec done, state: " + s1.getClusterViewStr());

            // when that's the case, check the view - it should now be in a
            // special 'final=false' mode
            final ViewExpectation expectationBeforeBgRead = new ViewExpectation(s1);
            expectationBeforeBgRead.setActiveIds(s1.ns.getClusterId());
            expectationBeforeBgRead.setDeactivatingIds(s2.ns.getClusterId());
            expectationBeforeBgRead.setFinal(false);
            waitFor(expectationBeforeBgRead, TEST_WAIT_TIMEOUT, "first should only see itself after shutdown");

            // ook, now we explicitly do a background read to get out of the
            // backlog situation
            s1.ns.runBackgroundReadOperations();

            final ViewExpectation expectationAfterBgRead = new ViewExpectation(s1);
            expectationAfterBgRead.setActiveIds(s1.ns.getClusterId());
            expectationAfterBgRead.setInactiveIds(s2.ns.getClusterId());
            waitFor(expectationAfterBgRead, TEST_WAIT_TIMEOUT, "finally we should see s2 as completely inactive");
        }
    }

    /**
     * This test creates a large number of documentnodestores which it starts,
     * runs, stops in a random fashion, always testing to make sure the
     * clusterView is correct
     */
    @Test
    public void testSmallStartStopFiesta() throws Throwable {
        logger.info("testSmallStartStopFiesta: start, seed="+SEED);
        final int LOOP_CNT = 5; // with too many loops have also seen mongo
                                 // connections becoming starved thus test
                                 // failed
        doStartStopFiesta(LOOP_CNT);
    }

}
