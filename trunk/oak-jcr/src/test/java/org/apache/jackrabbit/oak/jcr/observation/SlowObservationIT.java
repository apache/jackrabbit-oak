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

import static javax.jcr.observation.Event.NODE_ADDED;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.h2.util.Profiler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An slow test case that that tries to test the commit rate limiter.
 */
@RunWith(Parameterized.class)
// Don't run "Parallelized" as this causes tests to timeout in "weak" environments
public class SlowObservationIT extends AbstractRepositoryTest {
    
    static final Logger LOG = LoggerFactory.getLogger(SlowObservationIT.class);
    
    private static final int OBSERVER_COUNT = 3;
    
    private static final boolean NO_DELAY_JUST_BLOCK = false;

    private static final boolean PROFILE = Boolean.getBoolean("oak.profile");
    
    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;
    private static final String TEST2_NODE = "test_node2";
    private static final String TEST2_PATH = '/' + TEST2_NODE;
    
    public SlowObservationIT(NodeStoreFixture fixture) {
        super(fixture);
    }
    
    @Override
    protected Jcr initJcr(Jcr jcr) {
        CommitRateLimiter limiter = new CommitRateLimiter() {
            
            long lastLog;
            
            @Override
            public void setDelay(long delay) {
                long now = System.currentTimeMillis();
                if (now > lastLog + 1000) {
                    log("Delay " + delay);
                    lastLog = now;
                }
                super.setDelay(delay);
            }
            
            @Override
            protected void delay() throws CommitFailedException {
                if (!NO_DELAY_JUST_BLOCK) {
                    // default behavior
                    super.delay();
                    return;
                }
                if (getBlockCommits() && isThreadBlocking()) {
                    synchronized (this) {           
                        try {
                            while (getBlockCommits()) {
                                wait(1000);
                            }
                        } catch (InterruptedException e) {
                            throw new CommitFailedException(
                                    CommitFailedException.OAK, 2, "Interrupted while waiting to commit", e);
                        }
                    }
                }
            }
            
        };
        return super.initJcr(jcr).with(limiter);
    }

    @Before
    public void setup() throws RepositoryException {
        if (!isDocumentNodeStore()) {
            return;
        }
        Session session = getAdminSession();
        
        Node nodetypeIndex = session.getRootNode().getNode("oak:index").getNode("nodetype");
        nodetypeIndex.remove();

        Node testNode;
        testNode = session.getRootNode().addNode(TEST_NODE, "oak:Unstructured");
        testNode.setProperty("test", 0);
        testNode = session.getRootNode().addNode(TEST2_NODE, "oak:Unstructured");
        testNode.setProperty("test", 0);
        session.save();
    }

    private boolean isDocumentNodeStore() {
        // SegmentNodeStore can result in deadlocks, because
        // the CommitRateLimiter may be blocking inside a sychronized block
        return fixture.toString().indexOf("DocumentNodeStore") >= 0;
    }

    @Test
    public void observation() throws Exception {
        if (!isDocumentNodeStore()) {
            return;
        }
        AtomicBoolean saveInObservation = new AtomicBoolean();
        saveInObservation.set(true);
        ArrayList<MyListener> listeners = new ArrayList<MyListener>();
        for (int i = 0; i < OBSERVER_COUNT; i++) {
            Session observingSession = createAdminSession();
            MyListener listener = new MyListener(i, observingSession, saveInObservation);
            listener.open();
            listeners.add(listener);
        }
        log("Starting...");
        Profiler prof = null;
        long start = System.currentTimeMillis();
        for (int i = 1;; i++) {
            if (prof == null && PROFILE) {
                // prof = new Profiler().startCollecting();
            }
            long time = System.currentTimeMillis() - start;
            if (time > 20 * 1000) {
                if (saveInObservation.get()) {
                    log("Disable saves in observation now");
                    saveInObservation.set(false);
                }
            }
            if (time > 30 * 1000) {
                break;
            }
            Node testNode;
            if (i % 100 < 52) {
                // in 52% of the cases, use testNode
                testNode = getNode(TEST_PATH);
            } else {
                // in 48% of the cases, use testNode2
                testNode = getNode(TEST2_PATH);
            }
            String a = "c-" + (i / 40);
            String b = "c-" + (i % 40);
            Node x;
            if (testNode.hasNode(a)) {
                x = testNode.getNode(a);
            } else {
                x = testNode.addNode(a, "oak:Unstructured");
            }
            Node t = x.addNode(b, "oak:Unstructured");
            for (int j = 0; j < 10; j++) {
                t.addNode("c-" + j, "oak:Unstructured");
            }
            long saveTime = System.currentTimeMillis();
            getAdminSession().save();
            saveTime = System.currentTimeMillis() - saveTime;
            if (saveTime > 100 || i % 200 == 0) {
                if (prof != null) {
                    log(prof.getTop(1));
                    prof = null;
                }
                log("Save #" + i + " took " + saveTime + " ms");
            }
        }
        log("Stopping...");
        for (MyListener listener : listeners) {
            listener.stop();
            listener.waitUntilDone();
            listener.close();
        }
        log("Done");
        if (PROFILE) {
            printFullThreadDump();
        }
    }

    //------------------------------------------------------------< private >---

    private Node getNode(String path) throws RepositoryException {
        return getAdminSession().getNode(path);
    }
    
    /**
     * A simple listener that writes in 10% of the cases.
     */
    private static class MyListener implements EventListener {
        
        private final AtomicBoolean saveInObservation;
        private final int id;
        private final Session session;
        private ObservationManager observationManager;
        private volatile boolean stopped;
        private volatile boolean done;
        private Exception exception;
        
        MyListener(int id, Session session, AtomicBoolean saveInObservation) {
            this.id = id;
            this.session = session;
            this.saveInObservation = saveInObservation;
        }
        
        public void open() throws RepositoryException {
            observationManager = session.getWorkspace().getObservationManager();
            observationManager.addEventListener(this, NODE_ADDED, "/" + TEST_NODE, true, null, null, false);
        }

        public void close() throws RepositoryException {
            observationManager.removeEventListener(this);
        }

        public void stop() {
            stopped = true;
        }
        
        public void waitUntilDone() throws Exception {
            while (!done) {
                synchronized (this) {
                    wait(1000);
                }
            }
            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public void onEvent(EventIterator events) {
            while (!stopped && events.hasNext()) {
                Event event = events.nextEvent();
                try {
                    String path = event.getPath();
                    // Thread.currentThread().setName("Observer path " + path);
                    // System.out.println("observer " + path);
                    if (Math.abs(path.hashCode() % OBSERVER_COUNT) != id) {
                        // if it's not for "my" observer, ignore
                        continue;
                    }
                    if (Math.random() > 0.5) {
                        // do nothing 50% of the time
                        continue;
                    }
                    // else save something as well: 5 times setProperty
                    if (saveInObservation.get()) {
                        if (session.getRootNode().hasNode(path.substring(1))) {
                            Node n = session.getNode(path);
                            // System.out.println("observer save "+ path);                            
                            for (int i = 0; i < 5; i++) {
                                n.setProperty("x", i);
                                session.save();
                            }
                        }
                    }
                } catch (Exception e) {
                    log("Error " + e);
                    LOG.error("Observation listener error", e);
                    exception = e;
                }
            }
            done = true;
            synchronized (this) {
                notifyAll();
            }
        }
    }
    
    static void log(String message) {
        if (PROFILE) {
            System.out.println(message);
        }
        LOG.info(message);
    }
    
    public static void printFullThreadDump() {
        log(new Timestamp(System.currentTimeMillis()).toString()
                .substring(0, 19));
        log("Full thread dump " +
                System.getProperty("java.vm.name") + 
                " (" + System.getProperty("java.vm.version") + "):");
        log("");
        for (Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces()
                .entrySet()) {
            Thread t = e.getKey();
            log(String.format("\"%s\"%s prio=%d tid=0x%x", 
                    t.getName(), 
                    t.isDaemon() ? " daemon" : "",
                    t.getPriority(),
                    t.getId()));
            log("    java.lang.Thread.State: " + t.getState());
            for (StackTraceElement s : e.getValue()) {
                log("\tat " + s);
            }
            log("");
        }
    }

}
