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
import org.apache.jackrabbit.api.observation.JackrabbitEvent;
import org.apache.jackrabbit.oak.commons.PathUtils;
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
import java.util.concurrent.atomic.AtomicLong;

import static javax.jcr.observation.Event.NODE_ADDED;
import static org.junit.Assert.assertEquals;
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

        Map<String, Object> attrs = new HashMap<>();
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

    @Test
    public void testQueueFullThenFlushing() throws Exception {
        final Semaphore semaphore = new Semaphore(0);
        final AtomicLong counter = new AtomicLong(0);
        final AtomicLong localCounter = new AtomicLong(0);
        EventListener listeners = new EventListener() {

            @Override
            public void onEvent(EventIterator events) {
                try {
                    semaphore.acquire();
                    long numEvents = events.getSize();
                    counter.addAndGet(numEvents);
                    System.out.println("GOT: "+numEvents + " - COUNTER: "+counter.get());
                    while(events.hasNext()) {
                        Event e = events.nextEvent();
                        System.out.println(" - " + e);
                        if (PathUtils.getName(e.getPath()).startsWith("local")) {
                            if (e instanceof JackrabbitEvent && !((JackrabbitEvent)e).isExternal()) {
                                localCounter.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new Error(e);
                } catch (RepositoryException e) {
                    throw new Error(e);
                }
            }
        };
        Session session = getAdminSession();
        Node root = session.getNode("/");
        root.addNode("testNode");
        session.save();

        observationManager.addEventListener(listeners, Event.PROPERTY_ADDED, "/", true, null, null, false);
        
        // OAK-6639 : the above addEventListener registers an Observer with the NodeStore
        // that (an Observable in general) in turn as the very first activity does a contentChanged call (with
        // CommitInfo.EMPTY_EXTERNAL) to 'initialize' the Observer
        // (see eg https://github.com/apache/jackrabbit-oak/blob/2634dbde9aedc2549f0512285e9abee5858b256f/oak-store-spi/src/main/java/org/apache/jackrabbit/oak/spi/commit/ChangeDispatcher.java#L66)
        // normally that initial call should be processed very quickly by the
        // BackgroundObserver, but it seems like there are some cases where
        // this (main) thread gets priority and is able to do the 6 session.save
        // calls before the BackgroundObserver is able to dequeue the 'init-token'.
        // in *that* case the queue overfills unexpectedly.
        // To avoid this, give the BackgroundObserver 2sec here to process the
        // init-token, so that the test can actually start with an empty BackgroundObserver queue
        Thread.sleep(2000);

        int propCounter = 0;
        // send out 6 events (or in general: queue length + 1):
        // event #0 will get delivered but stalls at the listener (queue empty though)
        // event #1-#5 will fill the queue - all must remain "local"
        for(int i=0; i<OBS_QUEUE_LENGTH + 1; i++, propCounter++) {
            root = session.getNode("/");
            root.getNode("testNode").setProperty("local" + propCounter, propCounter);
            System.out.println("storing: /testNode/local" + propCounter);
            session.save();
        }

        // release the listener to consume 6 events
        semaphore.release(OBS_QUEUE_LENGTH+1);

        boolean notTimedOut = waitFor(2000, new Condition() {
            @Override
            public boolean evaluate() {
                return (OBS_QUEUE_LENGTH+1)==counter.get();
            }
        });
        assertTrue("Listener didn't process " + (OBS_QUEUE_LENGTH+1) + " events within time-out", notTimedOut);
        assertEquals("Just filled queue must not convert local->external", OBS_QUEUE_LENGTH+1, localCounter.get());

        counter.set(0);

        // send out 7 events (or in general: queue length + 2):
        // event #0 will get delivered but stalls at the listener (queue empty though)
        // event #1-#5 will fill the queue
        // event #6 will not fit in the queue anymore (queue full)
        for(int i=0; i<OBS_QUEUE_LENGTH + 2; i++, propCounter++) {
            root = session.getNode("/");
            root.getNode("testNode").setProperty("p" + propCounter, propCounter);
            System.out.println("storing: /testNode/p" + propCounter);
            session.save();
        }

        // release the listener
        semaphore.release(100); // ensure acquire will no longer block during this test -> pass 100

        notTimedOut = waitFor(2000, new Condition() {
            @Override
            public boolean evaluate() {
                return (OBS_QUEUE_LENGTH+2)==counter.get();
            }
        });
        assertTrue("Listener didn't process " + (OBS_QUEUE_LENGTH+2) + " events within time-out", notTimedOut);

        root = session.getNode("/");
        root.getNode("testNode").setProperty("p" + propCounter, propCounter);
        System.out.println("storing: /testNode/p" + propCounter);
        session.save();

        notTimedOut = waitFor(1000, new Condition() {
            @Override
            public boolean evaluate() {
                return (OBS_QUEUE_LENGTH+3)==counter.get();
            }
        });
        assertTrue("Listener didn't process " + (OBS_QUEUE_LENGTH+3) + " events within time-out", notTimedOut);
    }
}
