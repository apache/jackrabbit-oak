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
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static javax.jcr.observation.Event.NODE_ADDED;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ObservationQueueFullWarnTest extends AbstractRepositoryTest {
    static final int OBS_QUEUE_LENGTH = 5;
    static final String OBS_QUEUE_FULL_WARN = "Revision queue is full. Further revisions will be compacted.";

    private static final String TEST_NODE = "test_node";
    private static final String TEST_NODE_TYPE = "oak:Unstructured";
    private static final String TEST_PATH = '/' + TEST_NODE;

    private static final long CONDITION_TIMEOUT = 10*1000;

    private Session observingSession;
    private ObservationManager observationManager;

    public ObservationQueueFullWarnTest(NodeStoreFixture fixture) {
        super(fixture);
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

        Map<String,Object> attrs = new HashMap<>();
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

        final LoggingListener listener = new LoggingListener();
        observationManager.addEventListener(listener, NODE_ADDED, TEST_PATH, true, null, null, false);
        try {
            Node n = getAdminSession().getNode(TEST_PATH);

            customLogs.starting();
            addNodeToFillObsQueue(n, 0, listener);
            assertTrue("Observation queue full warning must gets logged", customLogs.getLogs().size() > 0);
            customLogs.finished();
        }
        finally {
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

        final LoggingListener listener = new LoggingListener();
        observationManager.addEventListener(listener, NODE_ADDED, TEST_PATH, true, null, null, false);
        try {
            Node n = getAdminSession().getNode(TEST_PATH);
            int nodeNameCounter = 0;

            //Create first level WARN message
            nodeNameCounter = addNodeToFillObsQueue(n, nodeNameCounter, listener);
            emptyObsQueueABit(listener);

            //Don't wait, fill up the queue again
            warnLogs.starting();
            debugLogs.starting();
            nodeNameCounter = addNodeToFillObsQueue(n, nodeNameCounter, listener);
            assertTrue("Observation queue full warning must not logged until some time has past since last log",
                    warnLogs.getLogs().size() == 0);
            assertTrue("Observation queue full warning should get logged on debug though in the mean time",
                    debugLogs.getLogs().size() > 0);
            warnLogs.finished();
            debugLogs.finished();
            emptyObsQueueABit(listener);

            //Wait some time so reach WARN level again
            virtualClock.waitUntil(virtualClock.getTime() + ChangeProcessor.QUEUE_FULL_WARN_INTERVAL);

            warnLogs.starting();
            debugLogs.starting();
            addNodeToFillObsQueue(n, nodeNameCounter, listener);
            assertTrue("Observation queue full warning must get logged after some time has past since last log",
                    warnLogs.getLogs().size() > 0);
            warnLogs.finished();
            debugLogs.finished();
        }
        finally {
            observationManager.removeEventListener(listener);
            ChangeProcessor.clock = oldClockInstance;
            ChangeProcessor.QUEUE_FULL_WARN_INTERVAL = oldWarnLogInterval;

            logLevelSetting.finished();
        }
    }

    private void emptyObsQueueABit(final LoggingListener listener) throws InterruptedException {
        //Let queue empty up a bit.
        boolean notTimedOut = listener.waitFor(CONDITION_TIMEOUT, new Condition() {
            @Override
            public boolean evaluate() {
                return listener.numAdded >= 2;
            }
        });
        listener.numAdded = 0;
        assertTrue("Listener didn't process events within time-out", notTimedOut);
    }

    private interface Condition {
        boolean evaluate();
    }

    private static int addNodeToFillObsQueue(Node parent, int nodeNameCounter, LoggingListener listener)
            throws RepositoryException {
        listener.blockObservation.acquireUninterruptibly();
        try {
            for (int i = 0; i <= OBS_QUEUE_LENGTH; i++, nodeNameCounter++) {
                parent.addNode("n" + nodeNameCounter);
                parent.getSession().save();
            }
            return nodeNameCounter;
        } finally {
            listener.blockObservation.release();
        }
    }

    private class LoggingListener implements EventListener {

        private volatile int numAdded = 0;

        Semaphore blockObservation = new Semaphore(1);

        @Override
        public synchronized void onEvent(EventIterator events) {
            blockObservation.acquireUninterruptibly();
            while (events.hasNext()) {
                events.nextEvent();
                numAdded++;
            }
            blockObservation.release();

            notifyAll();
        }

        synchronized boolean waitFor(long timeout, Condition c)
                throws InterruptedException {
            long end = System.currentTimeMillis() + timeout;
            long remaining = end - System.currentTimeMillis();
            while (remaining > 0) {
                if (c.evaluate()) {
                    return true;
                }
                wait(remaining);
                remaining = end - System.currentTimeMillis();
            }
            return false;
        }
    }
}
