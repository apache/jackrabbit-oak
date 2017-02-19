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
package org.apache.jackrabbit.oak.jcr.observation;

import ch.qos.logback.classic.Level;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static javax.jcr.observation.Event.NODE_ADDED;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ObservationQueueFullWarnTest extends AbstractRepositoryTest {
    private static final int OBS_QUEUE_LENGTH = 5;
    private static final String OBS_QUEUE_FULL_WARN = "Revision queue is full. Further revisions will be compacted.";

    private static final String TEST_NODE = "test_node";
    private static final String TEST_NODE_TYPE = "oak:Unstructured";
    private static final String TEST_PATH = '/' + TEST_NODE;

    private static final long OBS_TIMEOUT_PER_ITEM = 1000;
    private static final long CONDITION_TIMEOUT = OBS_QUEUE_LENGTH * OBS_TIMEOUT_PER_ITEM;

    private Session observingSession;
    private ObservationManager observationManager;

    private final BlockableListener listener = new BlockableListener();

    private static final Logger LOG = LoggerFactory.getLogger(ObservationQueueFullWarnTest.class);

    private final Semaphore blockObservation = new Semaphore(1);

    private final AtomicInteger numAddedNodes = new AtomicInteger(0);
    private final AtomicInteger numObservedNodes = new AtomicInteger(0);

    public ObservationQueueFullWarnTest(NodeStoreFixture fixture) {
        super(fixture);
        LOG.info("fixture: {}", fixture);
    }

    @Override
    protected Jcr initJcr(Jcr jcr) {
        return jcr.withObservationQueueLength(OBS_QUEUE_LENGTH);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();

        session.getRootNode().addNode(TEST_NODE, TEST_NODE_TYPE);
        session.save();

        Map<String, Object> attrs = new HashMap<String, Object>();
        attrs.put(RepositoryImpl.REFRESH_INTERVAL, 0);
        observingSession = ((JackrabbitRepository) getRepository()).login(new SimpleCredentials("admin", "admin".toCharArray()), null, attrs);
        observationManager = observingSession.getWorkspace().getObservationManager();
    }

    @After
    public void tearDown() {
        observingSession.logout();
    }

    @Test
    public void warnOnQueueFull() throws RepositoryException, InterruptedException, ExecutionException {
        LogCustomizer customLogs = LogCustomizer.forLogger(ChangeProcessor.class.getName())
                .filter(Level.WARN)
                .contains(OBS_QUEUE_FULL_WARN)
                .create();

        observationManager.addEventListener(listener, NODE_ADDED, TEST_PATH, true, null, null, false);
        try {
            customLogs.starting();
            addNodeToFillObsQueue();
            assertTrue("Observation queue full warning must get logged", customLogs.getLogs().size() > 0);
            customLogs.finished();
        } finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void warnOnRepeatedQueueFull() throws RepositoryException, InterruptedException, ExecutionException {
        LogCustomizer warnLogs = LogCustomizer.forLogger(ChangeProcessor.class.getName())
                .filter(Level.WARN)
                .contains(OBS_QUEUE_FULL_WARN)
                .create();
        LogCustomizer debugLogs = LogCustomizer.forLogger(ChangeProcessor.class.getName())
                .filter(Level.DEBUG)
                .contains(OBS_QUEUE_FULL_WARN)
                .create();
        LogCustomizer logLevelSetting = LogCustomizer.forLogger(ChangeProcessor.class.getName())
                .enable(Level.DEBUG)
                .create();
        logLevelSetting.starting();

        long oldWarnLogInterval = ChangeProcessor.QUEUE_FULL_WARN_INTERVAL;
        //Assumption is that 10 (virtual) minutes won't pass by the time we move from one stage of queue fill to next.
        ChangeProcessor.QUEUE_FULL_WARN_INTERVAL = TimeUnit.MINUTES.toMillis(10);

        Clock oldClockInstance = ChangeProcessor.clock;
        Clock virtualClock = new Clock.Virtual();
        ChangeProcessor.clock = virtualClock;
        virtualClock.waitUntil(System.currentTimeMillis());

        observationManager.addEventListener(listener, NODE_ADDED, TEST_PATH, true, null, null, false);
        try {
            //Create first level WARN message
            addNodeToFillObsQueue();
            emptyObsQueue();

            //Don't wait, fill up the queue again
            warnLogs.starting();
            debugLogs.starting();
            addNodeToFillObsQueue();
            assertTrue("Observation queue full warning must not logged until some time has past since last log",
                    warnLogs.getLogs().size() == 0);
            assertTrue("Observation queue full warning should get logged on debug though in the mean time",
                    debugLogs.getLogs().size() > 0);
            warnLogs.finished();
            debugLogs.finished();
            emptyObsQueue();

            //Wait some time so reach WARN level again
            virtualClock.waitUntil(virtualClock.getTime() + ChangeProcessor.QUEUE_FULL_WARN_INTERVAL);

            warnLogs.starting();
            debugLogs.starting();
            addNodeToFillObsQueue();
            assertTrue("Observation queue full warning must get logged after some time has past since last log",
                    warnLogs.getLogs().size() > 0);
            warnLogs.finished();
            debugLogs.finished();
        } finally {
            observationManager.removeEventListener(listener);
            ChangeProcessor.clock = oldClockInstance;
            ChangeProcessor.QUEUE_FULL_WARN_INTERVAL = oldWarnLogInterval;

            logLevelSetting.finished();
        }
    }

    private void addANode(String prefix) throws RepositoryException {
        Session session = getAdminSession();
        Node parent = session.getNode(TEST_PATH);
        String nodeName = prefix + numAddedNodes.get();
        parent.addNode(nodeName);
        session.save();
        numAddedNodes.incrementAndGet();
    }

    private void addNodeToFillObsQueue()
            throws RepositoryException {
        blockObservation.acquireUninterruptibly();
        try {
            for (int i = 0; i <= OBS_QUEUE_LENGTH; i++) {
                addANode("n");
            }
        } finally {
            blockObservation.release();
        }
    }

    private interface Condition {
        boolean evaluate();
    }

    private boolean waitFor(long timeout, Condition c)
            throws InterruptedException {
        long end = System.currentTimeMillis() + timeout;
        long remaining = end - System.currentTimeMillis();
        while (remaining > 0) {
            if (c.evaluate()) {
                return true;
            }

            //Add another node only when num_pending_to_be_observed nodes is
            //less that observation queue. This is done to let all observation finish
            //up in case last few event were dropped due to full observation queue
            //(which is ok as the next event that comes in gets diff-ed with last
            //processed revision)
            if (numAddedNodes.get() < numObservedNodes.get() + OBS_QUEUE_LENGTH) {
                try {
                    addANode("addedWhileWaiting");
                } catch (RepositoryException e) {
                    LOG.warn("exception while adding during wait: {}", e);
                }
            }
            Thread.sleep(OBS_TIMEOUT_PER_ITEM/10);//The constant is exaggerated
            remaining = end - System.currentTimeMillis();
        }
        return c.evaluate();
    }

    private void emptyObsQueue() throws InterruptedException {
        boolean notTimedOut = waitFor(CONDITION_TIMEOUT, new Condition() {
            @Override
            public boolean evaluate() {
                return numObservedNodes.get()==numAddedNodes.get();
            }
        });
        assertTrue("Listener didn't process events within time-out", notTimedOut);
    }

    private class BlockableListener implements EventListener {
        @Override
        public void onEvent(EventIterator events) {
            blockObservation.acquireUninterruptibly();
            while (events.hasNext()) {
                Event event = events.nextEvent();
                if (event.getType() == Event.NODE_ADDED) {
                    numObservedNodes.incrementAndGet();
                }
            }
            blockObservation.release();
        }
    }
}
