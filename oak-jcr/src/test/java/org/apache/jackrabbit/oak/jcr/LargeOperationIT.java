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

import static javax.jcr.observation.Event.NODE_ADDED;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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

    /** Maximum quotient of non linearity allowed */
    private static final double MAX_QUOTIENT = 1.2;

    /** Scales for running the tests against */
    private static final int[] SEGMENT_SCALES = new int[] {
            1024, 4096, 16384, 65536, 262144, 1048576};
    private static final int[] MONGO_SCALES = new int[] {
            128, 512, 2048, 8192, 32768, 131072};

    private final NodeStoreFixture fixture;
    private final int[] scales;

    private NodeStore nodeStore;
    private Repository repository;
    private Session session;

    public LargeOperationIT(NodeStoreFixture fixture, int[] scales) {
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

    private static List<Double> quotients(Iterable<Long> sequence) {
        Double prev = null;
        List<Double> quotients = Lists.newArrayList();
        for (long current : sequence) {
            if (prev != null) {
                quotients.add(current / prev);
            }
            prev = (double) current;
        }
        return quotients;
    }

    private static void assertMaxQuotient(String message, Iterable<Double> quotients) {
        for (double value : quotients) {
            if (value >= MAX_QUOTIENT) {
                fail(message + " The quotients between subsequent operations " +
                        "should all be below " + MAX_QUOTIENT + ". Quotients: " + quotients);
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

        ArrayList<Long> executionTimes = Lists.newArrayList();
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
            long t = test.run();
            executionTimes.add(t);
            LOG.info("Committing {} node took {} ns/node", scale, t);
        }
        List<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        assertMaxQuotient("Commit does not scale linearly.", quotients);
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

        ArrayList<Long> executionTimes = Lists.newArrayList();
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
            long t = test.run();
            executionTimes.add(t);
            LOG.info("Copying {} node took {} ns/node", scale, t);
        }
        List<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        assertMaxQuotient("Copy does not scale linearly.", quotients);
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

        ArrayList<Long> executionTimes = Lists.newArrayList();
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
            long t = test.run();
            executionTimes.add(t);
            LOG.info("Moving {} node took {} ns/node", scale, t);
        }
        List<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        assertMaxQuotient("Move does not scale linearly.", quotients);
    }

    /**
     * Assert adding many siblings scales linearly with the number of added siblings.
     * @throws RepositoryException
     * @throws InterruptedException
     */
    @Test
    public void manySiblings() throws RepositoryException, InterruptedException {
        final Node n = session.getRootNode().addNode("many-siblings", "oak:Unstructured");

        ArrayList<Long> executionTimes = Lists.newArrayList();
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
            long t = test.run();
            executionTimes.add(t);
            LOG.info("Adding {} siblings took {} ns/node", scale, t);
        }
        List<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        assertMaxQuotient("Adding siblings does not scale linearly.", quotients);
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

        ArrayList<Long> executionTimes = Lists.newArrayList();
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
                long t = test.run();
                executionTimes.add(t);
                LOG.info("{} pending events took {} ns/event to process", scale, t);
            } finally {
                observer.dispose();
            }
        }
        List<Double> quotients = quotients(executionTimes);
        LOG.info("Scaling quotients: {}", quotients);
        assertMaxQuotient("Processing pending events does not scale linearly.", quotients);
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
                    events.nextEvent().getPath();
                    done.countDown();
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

}
