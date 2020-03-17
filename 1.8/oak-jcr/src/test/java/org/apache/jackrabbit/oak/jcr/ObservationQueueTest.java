/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.fixture.DocumentMongoFixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.cluster.AbstractClusterTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.getServices;

@Ignore("Ignore long running ObservationQueueTest")
public class ObservationQueueTest extends AbstractClusterTest {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationQueueTest.class);

    static final long RUNTIME = TimeUnit.MINUTES.toMillis(10);
    static final int NUM_WRITERS = 10;
    static final int NUM_READERS = 10;
    static final int NUM_OBSERVERS = 10;
    static final int MAX_NODES_PER_WRITE = 30;
    static final int QUEUE_LENGTH = 1000;

    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final long MB = 1024 * 1024;
    private static final int NUM_CHILDREN = 100;

    private List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
    private List<Session> sessions = Lists.newArrayList();
    private List<Thread> writers = Lists.newArrayList();
    private List<Thread> readers = Lists.newArrayList();
    private List<Thread> observers = Lists.newArrayList();
    private List<Thread> loggers = Lists.newArrayList();

    @BeforeClass
    public static void dropDB() {
        MongoUtils.dropDatabase("oak-test");
    }

    @After
    public void logoutSessions() throws Exception {
        for (Session s : sessions) {
            s.logout();
        }
    }

    @Test
    public void heavyLoad() throws Throwable {
        List<Whiteboard> whiteboards = Lists.newArrayList(w1, w2);
        Iterator<Repository> repos = Iterators.cycle(r1, r2);
        AtomicLong commitCounter = new AtomicLong();
        for (int i = 0; i < NUM_WRITERS; i++) {
            Session s = loginUser(repos.next());
            Node n = s.getRootNode().addNode("session-" + i, "oak:Unstructured");
            s.save();
            writers.add(new Thread(new Writer(n, commitCounter)));
        }
        for (int i = 0; i < NUM_READERS; i++) {
            Session s = loginUser(repos.next());
            readers.add(new Thread(new Reader(s)));
        }
        AtomicInteger queueLength = new AtomicInteger();
        loggers.add(new Thread(new QueueLogger(whiteboards, queueLength, commitCounter)));
        for (int i = 0; i < NUM_OBSERVERS; i++) {
            Session s = loginUser(repos.next());
            observers.add(new Thread(new Observer(s, queueLength)));
        }
        for (Thread t : Iterables.concat(writers, readers, observers, loggers)) {
            t.start();
        }
        for (Thread t : Iterables.concat(writers, readers)) {
            t.join();
        }
        LOG.info("Writes stopped. Waiting for observers...");
        for (Thread t : Iterables.concat(observers, loggers)) {
            t.join();
        }
        for (Throwable t : exceptions) {
            throw t;
        }
    }

    @Override
    protected NodeStoreFixture getFixture() {
        return new DocumentMongoFixture() {
            @Override
            public NodeStore createNodeStore(int clusterNodeId) {
                return new DocumentMK.Builder().setClusterId(clusterNodeId)
                        .setMongoDB(MongoUtils.getConnection("oak-test").getDB())
                        .setPersistentCache("target/persistentCache" + clusterNodeId + ",time,size=128")
                        .setJournalCache("target/journalCache" + clusterNodeId + ",time,size=128")
                        .memoryCacheSize(128 * MB)
                        .setExecutor(Executors.newCachedThreadPool())
                        .getNodeStore();
            }
        };
    }

    @Override
    protected void prepareTestData(Session s) throws RepositoryException {
        UserManager uMgr = ((JackrabbitSession) s).getUserManager();
        User user = uMgr.createUser(USER, PASSWORD);
        s.save();
        AccessControlManager acMgr = s.getAccessControlManager();
        JackrabbitAccessControlList tmpl = AccessControlUtils.getAccessControlList(acMgr, "/");
        tmpl.addEntry(user.getPrincipal(), new Privilege[]{acMgr.privilegeFromName(Privilege.JCR_ALL)},
                true, Collections.<String, Value>emptyMap());
        acMgr.setPolicy(tmpl.getPath(), tmpl);
        s.save();
    }

    @Override
    protected Jcr customize(Jcr jcr) {
        return super.customize(jcr).withObservationQueueLength(QUEUE_LENGTH);
    }

    private Session loginUser(Repository repo) throws RepositoryException {
        Session s = repo.login(new SimpleCredentials(USER, PASSWORD.toCharArray()));
        sessions.add(s);
        return s;
    }

    private static void log(String msg, Object... args) {
        LOG.debug(msg, args);
    }

    private static void logRead(Node n) throws RepositoryException {
        log("Read node {}", n.getPath());
    }

    private class Reader extends Task {

        private final Random r;

        public Reader(Session s)
                throws RepositoryException {
            super(s);
            this.r = new Random();
        }

        @Override
        void perform() throws Exception {
            s.refresh(false);
            List<Node> nodes = Lists.newArrayList();
            for (NodeIterator it = s.getRootNode().getNodes(); it.hasNext(); ) {
                Node n = it.nextNode();
                if (n.getName().startsWith("session-")) {
                    nodes.add(n);
                }
            }
            if (nodes.isEmpty()) {
                return;
            }
            Node node = nodes.get(r.nextInt(nodes.size()));
            for (int i = 0; i < 2 && node != null; i++) {
                try {
                    node = node.getNode("node-" + r.nextInt(NUM_CHILDREN));
                    logRead(node);
                } catch (PathNotFoundException e) {
                    // ignore
                }
            }
            if (node != null && node.hasProperty("c")) {
                long count = node.getProperty("c").getLong();
                logRead(node.getNode("node-" + r.nextInt((int) count)));
            }
        }
    }

    private class Writer extends Task {

        private final Random r;
        private final Node node;
        private final AtomicLong commitCounter;

        public Writer(Node node, AtomicLong commitCounter) throws RepositoryException {
            super(node.getSession());
            this.r = new Random();
            this.node = node;
            this.commitCounter = commitCounter;
        }

        @Override
        void perform() throws Exception {
            Node p = node;
            for (int i = 0; i < 2; i++) {
                p = JcrUtils.getOrAddNode(p, "node-" + r.nextInt(NUM_CHILDREN), "oak:Unstructured");
            }
            // set property or add node?
            if (r.nextBoolean()) {
                long v = p.setProperty("p", r.nextInt()).getLong();
                log("Set property to {} on {}", v, p.getPath());
            } else {
                int numNodes = r.nextInt(MAX_NODES_PER_WRITE) + 1;
                int depth = (int) Math.ceil(Math.log(numNodes) / Math.log(3));
                long count = JcrUtils.getLongProperty(p, "c", 0);
                Node n = p.addNode("node-" + count, "oak:Unstructured");
                p.setProperty("c", ++count);
                createNodes(n, new AtomicInteger(--numNodes), depth);
                log("Add node {}", n.getPath());
            }
            s.save();
            commitCounter.incrementAndGet();
        }

        void createNodes(Node parent, AtomicInteger remaining, int depth)
                throws RepositoryException {
            depth--;
            for (int i = 0; i < 3 && remaining.get() > 0; i++) {
                Node n = parent.addNode("node-" + i);
                log("Add node {}", n.getPath());
                remaining.decrementAndGet();
                if (depth > 0) {
                    createNodes(n, remaining, depth);
                }
            }
        }
    }

    private class Observer extends Task implements EventListener {

        private final AtomicLong numEvents = new AtomicLong();
        private final AtomicInteger queueLength;

        public Observer(Session s, AtomicInteger queueLength)
                throws RepositoryException {
            super(s);
            this.queueLength = queueLength;
            s.getWorkspace().getObservationManager()
                    .addEventListener(this,
                            Event.NODE_ADDED | Event.PROPERTY_ADDED,
                            "/", true, null, null, false);
        }

        @Override
        void perform() throws Exception {
            Thread.sleep(1000);
        }

        @Override
        protected boolean running() {
            return super.running() || queueLength.get() > 0;
        }

        @Override
        public void onEvent(EventIterator events) {
            while (events.hasNext()) {
                numEvents.incrementAndGet();
                try {
                    Event e = events.nextEvent();
                    String p = e.getPath();
                    log("Event received {}", p);
                    if (e.getType() == Event.PROPERTY_ADDED) {
                        p = PathUtils.getParentPath(p);
                    }
                    s.getNode(p);
                } catch (RepositoryException e) {
                    exceptions.add(e);
                }
            }
        }
    }

    private class QueueLogger extends Task {

        private final List<Whiteboard> whiteboards;
        private final AtomicInteger queueLength;
        private final AtomicLong commitCounter;

        QueueLogger(List<Whiteboard> whiteboards,
                    AtomicInteger queueLength,
                    AtomicLong commitCounter) {
            super(null);
            this.whiteboards = whiteboards;
            this.queueLength = queueLength;
            this.commitCounter = commitCounter;
        }

        @Override
        void perform() throws Exception {
            List<String> stats = Lists.newArrayList();
            for (Whiteboard w : whiteboards) {
                stats.add(queueStats(w));
            }
            LOG.info("Observation queue stats: {}, commits: {}",
                    stats, commitCounter.get());
            Thread.sleep(1000);
        }

        @Override
        protected boolean running() {
            return super.running() || queueLength.get() > 0;
        }

        private String queueStats(Whiteboard w) {
            int len = -1;
            int ext = -1;
            for (BackgroundObserverMBean bean : getServices(w, BackgroundObserverMBean.class)) {
                len = Math.max(bean.getQueueSize(), len);
                ext = Math.max(bean.getExternalEventCount(), ext);
            }
            if (len >= 0) {
                queueLength.set(len);
            }
            int numListeners = 0;
            long micros = 0;
            for (EventListenerMBean bean : getServices(w, EventListenerMBean.class)) {
                micros += bean.getMicrosecondsPerEventDelivery();
                numListeners++;
            }
            List<String> stats = Lists.newArrayList();
            stats.add(String.valueOf(len));
            stats.add(String.valueOf(ext));
            stats.add(String.valueOf(micros / numListeners));
            return stats.toString();
        }
    }

    abstract class Task implements Runnable {

        final Session s;
        long end;

        public Task(Session s) {
            this.s = s;
        }

        protected boolean running() {
            return System.currentTimeMillis() < end;
        }

        @Override
        public void run() {
            try {
                end = System.currentTimeMillis() + RUNTIME;
                while (running()) {
                    perform();
                }
            } catch (Throwable t) {
                exceptions.add(t);
            }
        }

        abstract void perform() throws Exception;
    }
}
