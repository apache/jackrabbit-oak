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

package org.apache.jackrabbit.oak.jcr;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.jcr.observation.Event.NODE_ADDED;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.jcr.NodeStoreFixture.DocumentFixture;
import org.apache.jackrabbit.oak.jcr.NodeStoreFixture.SegmentFixture;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scalability test asserting certain operations scale linearly in the
 * size of their input.
 */
@Ignore("WIP OAK-1413")
@RunWith(Parameterized.class)
public class LargeOperationIT {
    private static final Logger LOG = LoggerFactory.getLogger(LargeOperationIT.class);

    /** Scales defining the input sizes against which the tests run */
    private static final Iterable<Integer> SEGMENT_SCALES = Lists.newArrayList(
            1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576);
    private static final Iterable<Integer> MONGO_SCALES = Lists.newArrayList(
            128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072);

    private final NodeStoreFixture fixture;
    private final Iterable<Integer> scales;

    private NodeStore nodeStore;
    private Repository repository;
    private Session session;

    public LargeOperationIT(NodeStoreFixture fixture, Iterable<Integer> scales) {
        this.fixture = fixture;
        this.scales = scales;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() throws IOException {
        File file = new File(new File("target"), "tar." + System.nanoTime());
        SegmentStore segmentStore = new FileStore(file, 266, true);

        List<Object[]> fixtures = Lists.newArrayList();
        SegmentFixture segmentFixture = new SegmentFixture(segmentStore);
        if (segmentFixture.isAvailable()) {
            fixtures.add(new Object[]{segmentFixture, SEGMENT_SCALES});
        }
        DocumentFixture documentFixture = new DocumentFixture();
        if (documentFixture.isAvailable()) {
            fixtures.add(new Object[]{documentFixture, MONGO_SCALES});
        }
        return fixtures;
    }

    private Session createAdminSession() throws RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    @Before
    public void setup() throws RepositoryException {
        nodeStore = fixture.createNodeStore();
        repository  = new Jcr(nodeStore).createRepository();
        session = createAdminSession();
    }

    @After
    public void tearDown() {
        session.logout();
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        fixture.dispose(nodeStore);
    }

    /**
     * Calculate the quotients of subsequent elements of an input {@code sequence}
     * @param sequence  input sequence
     * @return  sequence of quotients of {@code sequence}
     */
    private static Iterable<Double> quotients(Iterable<Double> sequence) {
        Double prev = null;
        List<Double> quotients = Lists.newArrayList();
        for (double current : sequence) {
            if (prev != null) {
                quotients.add(current / prev);
            }
            prev = current;
        }
        return quotients;
    }

    /**
     * Calculate the logarithmic bound for the given {@code scales} applying an
     * {@code offset} to account for errors.
     */
    private static Iterable<Double> getLogarithmicBound(Iterable<Integer> scales, int offset) {
        Double prev = null;
        List<Double> bound = Lists.newArrayList();
        for (double current : scales) {
            if (prev != null) {
                bound.add(Math.log(current * offset) / Math.log(prev));
            }
            prev = current;
        }
        return bound;
    }

    /**
     * Assert that {@code sequence} is bounded by {@code bound}
     */
    private static void assertBounded(String message,
            Iterable<Double> sequence, Iterable<Double> bound) {
        Iterator<Double> max = bound.iterator();
        for (double value : sequence) {
            if (value > max.next()) {
                fail(message + " The sequence exceeds its bound. " +
                        "Expected " + sequence + " <= " + bound);
            }
        }
    }

    /**
     * Assert that large commits scale linearly wrt. to the number of changed items.
     * @throws RepositoryException
     * @throws InterruptedException
     */
    @Test
    public void largeCommit() throws RepositoryException, InterruptedException {
        final Node n = session.getRootNode().addNode("large-commit", "oak:Unstructured");
        final ContentGenerator contentGenerator = new ContentGenerator();

        ArrayList<Double> executionTimes = Lists.newArrayList();
        for (int scale : scales) {
            ScalabilityTest test = new ScalabilityTest(scale) {
                @Override
                void before(int scale) throws RepositoryException {
                    contentGenerator.addNodes(n, scale);
                }

                @Override
                void run(int scale) throws RepositoryException {
                    session.save();
                }
            };
            double t = test.run();
            executionTimes.add(t);
            LOG.info("Committing {} node took {} ns/node", scale, t);
        }
        Iterable<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        Iterable<Double> bound = getLogarithmicBound(scales, 10);
        assertBounded("Commit does not scale logarithmically.", quotients, bound);
    }

    /**
     * Assert copy scales linearly with the number of items copied
     * @throws RepositoryException
     * @throws InterruptedException
     */
    @Test
    public void largeCopy() throws RepositoryException, InterruptedException {
        final Node n = session.getRootNode().addNode("large-copy", "oak:Unstructured");
        final ContentGenerator contentGenerator = new ContentGenerator(1000);

        ArrayList<Double> executionTimes = Lists.newArrayList();
        for (int scale : scales) {
            ScalabilityTest test = new ScalabilityTest(scale) {
                @Override
                void before(int scale) throws RepositoryException {
                    Node s = n.addNode("s" + scale);
                    contentGenerator.addNodes(s, scale);
                }

                @Override
                void run(int scale) throws RepositoryException {
                    session.getWorkspace().copy("/large-copy/s" + scale, "/large-copy/t" + scale);
                }
            };
            double t = test.run();
            executionTimes.add(t);
            LOG.info("Copying {} node took {} ns/node", scale, t);
        }
        Iterable<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        Iterable<Double> bound = getLogarithmicBound(scales, 10);
        assertBounded("Copy does not scale logarithmically.", quotients, bound);
    }

    /**
     * Assert move scales linearly with the number of items copied
     * @throws RepositoryException
     * @throws InterruptedException
     */
    @Test
    public void largeMove() throws RepositoryException, InterruptedException {
        final Node n = session.getRootNode().addNode("large-move", "oak:Unstructured");
        final ContentGenerator contentGenerator = new ContentGenerator(1000);

        ArrayList<Double> executionTimes = Lists.newArrayList();
        for (int scale : scales) {
            ScalabilityTest test = new ScalabilityTest(scale) {
                @Override
                void before(int scale) throws RepositoryException {
                    Node s = n.addNode("s" + scale);
                    contentGenerator.addNodes(s, scale);
                }

                @Override
                void run(int scale) throws RepositoryException {
                    session.getWorkspace().move("/large-move/s" + scale, "/large-move/t" + scale);
                }
            };
            double t = test.run();
            executionTimes.add(t);
            LOG.info("Moving {} node took {} ns/node", scale, t);
        }
        Iterable<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        Iterable<Double> bound = getLogarithmicBound(scales, 10);
        assertBounded("Move does not scale logarithmically.", quotients, bound);
    }

    /**
     * Assert adding many siblings scales linearly with the number of added siblings.
     * @throws RepositoryException
     * @throws InterruptedException
     */
    @Test
    public void manySiblings() throws RepositoryException, InterruptedException {
        final Node n = session.getRootNode().addNode("many-siblings", "oak:Unstructured");

        ArrayList<Double> executionTimes = Lists.newArrayList();
        for (int scale : scales) {
            ScalabilityTest test = new ScalabilityTest(scale) {
                @Override
                void before(int scale) throws RepositoryException {
                    n.addNode("s" + scale);
                }

                @Override
                void run(int scale) throws RepositoryException {
                    Node s = n.getNode("s" + scale);
                    for (int k = 0; k < scale; k++) {
                        s.addNode("s" + k);
                    }
                    session.save();
                }
            };
            double t = test.run();
            executionTimes.add(t);
            LOG.info("Adding {} siblings took {} ns/node", scale, t);
        }
        Iterable<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        Iterable<Double> bound = getLogarithmicBound(scales, 10);
        assertBounded("Adding siblings does not scale logarithmically.", quotients, bound);
    }

    /**
     * Assert processing of pending observation events scales linearly with the
     * number of pending events.
     * @throws RepositoryException
     * @throws InterruptedException
     */
    @Test
    public void largeNumberOfPendingEvents() throws RepositoryException, InterruptedException {
        final Node n = session.getRootNode().addNode("pending-events", "oak:Unstructured");
        final ContentGenerator contentGenerator = new ContentGenerator(1000);

        ArrayList<Double> executionTimes = Lists.newArrayList();
        for (int scale : scales) {
            final Observer observer = new Observer(scale, 100);
            try {
                ScalabilityTest test = new ScalabilityTest(scale) {
                    @Override
                    void before(int scale) throws RepositoryException {
                        contentGenerator.addNodes(n, scale);
                    }

                    @Override
                    void run(int scale) throws InterruptedException {
                        observer.waitForEvents(scale);
                    }
                };
                double t = test.run();
                executionTimes.add(t);
                LOG.info("{} pending events took {} ns/event to process", scale, t);
            } finally {
                try {
                    observer.dispose();
                } catch (Exception ignore) {}
            }
        }
        Iterable<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        Iterable<Double> bound = getLogarithmicBound(scales, 10);
        assertBounded("Processing pending events does not scale logarithmically.", quotients, bound);
    }

    @Test
    public void slowListener() throws RepositoryException, ExecutionException, InterruptedException {
        Node n = session.getRootNode().addNode("slow-events", "oak:Unstructured");
        final DelayedEventHandling delayedEventHandling = new DelayedEventHandling(n, 100, 10);
        Future<Void> result = delayedEventHandling.start();

        try {
            ArrayList<Double> executionTimes = Lists.newArrayList();
            for (int scale : scales) {
                ScalabilityTest test = new ScalabilityTest(scale) {
                    @Override
                    void run(int scale) throws InterruptedException {
                        delayedEventHandling.waitForNodes(scale);
                    }
                };
                double t = test.run();
                executionTimes.add(t);
                LOG.info("Adding {} nodes took {} ns/node", scale, t);
            }
            Iterable<Double> quotients = quotients(executionTimes);
            LOG.info("Scaling quotients: {}", quotients);
            Iterable<Double> bound = getLogarithmicBound(scales, 10);
            assertBounded("Adding nodes does not scale logarithmically in the face of slow " +
                    "observation listeners.", quotients, bound);
        } finally {
            delayedEventHandling.stop();
            result.get();
        }
    }

    //------------------------------------------------------------< ContentGenerator >---

    private static class ContentGenerator {
        private static final int FAN_OUT = 10;
        private final int saveInterval;

        private int count;

        public ContentGenerator(int saveInterval) {
            this.saveInterval = saveInterval;
        }

        public ContentGenerator() {
            this(Integer.MAX_VALUE);
        }

        public void addNodes(Node node, int count) throws RepositoryException {
            LOG.info("Adding {} nodes to {}", count, node.getPath());
            this.count = count;
            while (createContent(node));
            if (saveInterval < Integer.MAX_VALUE) {
                node.getSession().save();
            }
        }

        private boolean createContent(Node node) throws RepositoryException {
            NodeIterator nodes = node.getNodes();
            if (nodes.hasNext()) {
                while (nodes.hasNext()) {
                    if (!createContent(nodes.nextNode())) {
                        return false;
                    }
                }
                return true;
            } else {
                boolean result = true;
                for (int c = 0; c < FAN_OUT && (result = addNode(node)); c++);
                return result;
            }
        }

        boolean addNode(Node node) throws RepositoryException {
            node.addNode("n" + count--);
            if (count % saveInterval == 0) {
                node.getSession().save();
            }
            if (count % 1000 == 0) {
                LOG.debug("add {}", node.getPath());
            }
            return !isDone();
        }

        boolean isDone() {
            return count == 0;
        }
    }

    //------------------------------------------------------------< ScalabilityTest >---

    private abstract static class ScalabilityTest {
        private final int scale;

        protected ScalabilityTest(int scale) {
            this.scale = scale;
        }

        void before(int scale) throws RepositoryException {}
        abstract void run(int scale) throws RepositoryException, InterruptedException;
        void after(int scale) {}

        public long run() throws RepositoryException, InterruptedException {
            before(scale);
            long t0 = System.nanoTime();
            run(scale);
            long dt = System.nanoTime() - t0;
            after(scale);
            return dt / scale;
        }
    }

    //------------------------------------------------------------< Observer >---

    private class Observer implements EventListener {
        private final CountDownLatch start = new CountDownLatch(1);
        private final int eventCount;
        private final int listenerCount;
        private final Session[] sessions;

        private CountDownLatch done;

        public Observer(int eventCount, int listenerCount) throws RepositoryException {
            this.eventCount = eventCount;
            this.listenerCount = listenerCount;
            this.sessions = new Session[listenerCount];
            for (int k = 0; k < sessions.length; k++) {
                sessions[k] = createAdminSession();
                sessions[k].getWorkspace().getObservationManager().addEventListener(
                        this, NODE_ADDED, "/", true, null, null, false);
            }
        }

        public void waitForEvents(int scale) throws InterruptedException {
            done = new CountDownLatch(scale);
            start.countDown();
            done.await();
        }

        public void dispose() {
            for (Session session : sessions) {
                session.logout();
            }
        }

        @Override
        public void onEvent(EventIterator events) {
            try {
                start.await();
                while (events.hasNext()) {
                    events.nextEvent();
                    done.countDown();
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    //------------------------------------------------------------< DelayedEventHandling >---

    private class DelayedEventHandling implements EventListener {
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final Semaphore openEvents = new Semaphore(0);
        private final AtomicReference<CountDownLatch> nodeCounter =
                new AtomicReference<CountDownLatch>(new CountDownLatch(0));
        private final Node node;
        private final int listenerCount;
        private final int saveInterval;

        private volatile boolean done;

        private DelayedEventHandling(Node node, int listenerCount, int saveInterval) {
            this.node = node;
            this.listenerCount = listenerCount;
            this.saveInterval = saveInterval;
        }

        public Future<Void> start() throws RepositoryException {
            return executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    final Session[] sessions = new Session[listenerCount];
                    ContentGenerator contentGenerator = new ContentGenerator(saveInterval) {
                        int nodeCount;

                        @Override
                        boolean addNode(Node node) throws RepositoryException {
                            boolean result = super.addNode(node);
                            if (++nodeCount % 2 == 0) {
                                openEvents.release(sessions.length);
                            }
                            nodeCounter.get().countDown();
                            return result;
                        }

                        @Override
                        boolean isDone() {
                            return done;
                        }
                    };

                    for (int k = 0; k < sessions.length; k++) {
                        sessions[k] = createAdminSession();
                        sessions[k].getWorkspace().getObservationManager().addEventListener(
                                DelayedEventHandling.this, NODE_ADDED, "/", true, null, null, false);
                    }
                    try {
                        contentGenerator.addNodes(node, Integer.MAX_VALUE);
                    } finally {
                        for (Session session : sessions) {
                            session.logout();
                        }
                    }
                    return null;
                }
            });
        }

        public void stop() {
            done = true;
        }

        public void waitForNodes(int count) throws InterruptedException {
            CountDownLatch counter = new CountDownLatch(count);
            nodeCounter.set(counter);
            counter.await();
        }

        @Override
        public void onEvent(EventIterator events) {
            try {
                while (events.hasNext()) {
                    while (!done && !openEvents.tryAcquire(10, MILLISECONDS));
                    if (done) {
                        break;
                    }
                    events.nextEvent();
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

    }
}
