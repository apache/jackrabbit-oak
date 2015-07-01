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

package org.apache.jackrabbit.oak.benchmark;

import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.getServices;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

public class ObservationTest extends Benchmark {
    public static final int EVENT_TYPES = NODE_ADDED | NODE_REMOVED | NODE_MOVED |
            PROPERTY_ADDED | PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;
    private static final int EVENTS_PER_NODE = 2; // NODE_ADDED and PROPERTY_ADDED
    private static final int SAVE_INTERVAL = Integer.getInteger("saveInterval", 100);
    private static final int OUTPUT_RESOLUTION = 100;
    private static final int LISTENER_COUNT = Integer.getInteger("listenerCount", 100);
    private static final int WRITER_COUNT = Integer.getInteger("writerCount", 1);
    private static final String PATH_FILTER = System.getProperty("pathFilter");

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        for (RepositoryFixture fixture : fixtures) {
            if (fixture.isAvailable(1)) {
                System.out.format("%s: Observation throughput benchmark%n", fixture);
                try {
                    final AtomicReference<Whiteboard> whiteboardRef = new AtomicReference<Whiteboard>();
                    Repository[] cluster;
                    if (fixture instanceof OakRepositoryFixture) {
                        cluster = ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                            @Override
                            public Jcr customize(Oak oak) {
                                whiteboardRef.set(oak.getWhiteboard());
                                return new Jcr(oak);
                            }
                        });
                    } else {
                        cluster = fixture.setUpCluster(1);
                    }
                    try {
                        run(cluster[0], whiteboardRef.get());
                    } finally {
                        fixture.tearDownCluster();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void run(Repository repository, @Nullable Whiteboard whiteboard)
            throws RepositoryException, ExecutionException, InterruptedException {
        Session session = createSession(repository);
        long t0 = System.currentTimeMillis();
        try {
            observationThroughput(repository, whiteboard);
        } finally {
            System.out.println("Time elapsed: " + (System.currentTimeMillis() - t0) + " ms");
            session.logout();
        }
    }

    public void observationThroughput(final Repository repository,
                                      @Nullable Whiteboard whiteboard)
            throws RepositoryException, InterruptedException, ExecutionException {
        long t = 0;
        final AtomicInteger eventCount = new AtomicInteger();
        final AtomicInteger nodeCount = new AtomicInteger();

        List<Session> sessions = Lists.newArrayList();
        List<EventListener> listeners = Lists.newArrayList();

        List<String> testPaths = Lists.newArrayList();
        Session s = createSession(repository);
        String path = "/path/to/observation/benchmark-" + AbstractTest.TEST_ID;
        try {
            Node testRoot = JcrUtils.getOrCreateByPath(path, null, s);
            for (int i = 0; i < WRITER_COUNT; i++) {
                testPaths.add(testRoot.addNode("session-" + i).getPath());
            }
            s.save();
        } finally {
            s.logout();
        }

        String pathFilter = PATH_FILTER == null ? path : PATH_FILTER;
        System.out.println("Path filter for event listener: " + pathFilter);
        ExecutorService service = Executors.newFixedThreadPool(WRITER_COUNT);
        try {
            for (int k = 0; k < LISTENER_COUNT; k++) {
                sessions.add(createSession(repository));
                listeners.add(new Listener(eventCount));
                ObservationManager obsMgr = sessions.get(k).getWorkspace().getObservationManager();
                obsMgr.addEventListener(listeners.get(k), EVENT_TYPES, pathFilter, true, null, null, false);
            }
            // also add a listener on the root node
            addRootListener(repository, sessions, listeners);

            List<Future<Object>> createNodes = Lists.newArrayList();
            for (final String p : testPaths) {
                createNodes.add(service.submit(new Callable<Object>() {
                    private final Session session = createSession(repository);
                    private int numNodes = 0;

                    @Override
                    public Object call() throws Exception {
                        try {
                            Node testRoot = session.getNode(p);
                            createChildren(testRoot, 100);
                            for (Node m : JcrUtils.getChildNodes(testRoot)) {
                                createChildren(m, 100 / WRITER_COUNT);
                                for (Node n : JcrUtils.getChildNodes(m)) {
                                    createChildren(n, 5);
                                }
                            }
                            session.save();
                        } finally {
                            session.logout();
                        }
                        return null;
                    }

                    private void createChildren(Node node, int count)
                            throws RepositoryException {
                        for (int c = 0; c < count; c++) {
                            node.addNode("n" + c);
                            nodeCount.incrementAndGet();
                            if (++numNodes % SAVE_INTERVAL == 0) {
                                node.getSession().save();
                            }
                        }
                    }
                }));
            }

            System.out.println("ms      #node   nodes/s #event  event/s event-ratio queue external");
            while (!isDone(createNodes) || (eventCount.get() / LISTENER_COUNT < nodeCount.get() * EVENTS_PER_NODE)) {
                long t0 = System.currentTimeMillis();
                Thread.sleep(OUTPUT_RESOLUTION);
                t += System.currentTimeMillis() - t0;

                int nc = nodeCount.get();
                int ec = eventCount.get() / LISTENER_COUNT;
                int[] ql = getObservationQueueLength(whiteboard);

                double nps = (double) nc / t * 1000;
                double eps = (double) ec / t * 1000;
                double epn = (double) ec / nc / EVENTS_PER_NODE;

                System.out.format(
                        "%7d %7d %7.1f %7d %7.1f %7.2f %7d %7d%n",
                           t, nc,  nps, ec,  eps,  epn, ql[0], ql[1]);
            }
            get(createNodes);
        } finally {
            for (int k = 0; k < sessions.size(); k++) {
                sessions.get(k).getWorkspace().getObservationManager()
                        .removeEventListener(listeners.get(k));
                sessions.get(k).logout();
            }
            service.shutdown();
            service.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    private void addRootListener(Repository repository,
                                 List<Session> sessions,
                                 List<EventListener> listeners)
            throws RepositoryException {
        Session s = createSession(repository);
        sessions.add(s);
        Listener listener = new Listener(new AtomicInteger());
        ObservationManager obsMgr = s.getWorkspace().getObservationManager();
        obsMgr.addEventListener(listener, EVENT_TYPES, "/", true, null, null, false);
        listeners.add(listener);
    }

    private static int[] getObservationQueueLength(@Nullable Whiteboard wb) {
        if (wb == null) {
            return new int[]{-1, -1};
        }
        int len = -1;
        int ext = -1;
        for (BackgroundObserverMBean bean : getServices(wb, BackgroundObserverMBean.class)) {
            len = Math.max(bean.getQueueSize(), len);
            ext = Math.max(bean.getExternalEventCount(), ext);
        }
        return new int[]{len, ext};
    }

    private static boolean isDone(Iterable<Future<Object>> futures) {
        for (Future f : futures) {
            if (!f.isDone()) {
                return false;
            }
        }
        return true;
    }

    private static void get(Iterable<Future<Object>> futures)
            throws ExecutionException, InterruptedException {
        for (Future f : futures) {
            f.get();
        }
    }

    private static Session createSession(Repository repository)
            throws RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    private static class Listener implements EventListener {
        private final AtomicInteger eventCount;

        public Listener(AtomicInteger eventCount) {
            this.eventCount = eventCount;
        }

        @Override
        public void onEvent(EventIterator events) {
            for (; events.hasNext(); events.nextEvent()) {
                eventCount.incrementAndGet();
            }
        }
    }
}
