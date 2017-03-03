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

import static java.lang.Math.log;
import static java.lang.Math.pow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.jcr.observation.Event.NODE_ADDED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

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

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.exception.MathInternalError;
import org.apache.commons.math3.exception.NotPositiveException;
import org.apache.commons.math3.exception.NullArgumentException;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.exception.util.LocalizedFormats;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.fixture.DocumentMongoFixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.session.RefreshStrategy;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.segment.fixture.SegmentTarFixture;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

/**
 * Scalability test asserting certain operations scale not worse than {@code O(n log n)}
 * in the size of their input.
 *
 * These tests are disabled by default due to their long running time. On the command line
 * specify {@code -DLargeOperationIT=true} to enable them.
 */
@RunWith(Parameterized.class)
public class LargeOperationIT {
    private static final Logger LOG = LoggerFactory.getLogger(LargeOperationIT.class);
    private static final boolean enabled = Boolean.getBoolean(LargeOperationIT.class.getSimpleName());

    /**
     * Significance level for the binomial test being performed to establish
     * the {@code O(n log n)} performance bound.
     * @see #assertOnLgn(String, Iterable, java.util.List, boolean)
     */
    public static final double ALPHA = 0.05;

    /** Scales defining the input sizes against which the tests run */
    private static final Iterable<Integer> SEGMENT_SCALES = createSequence(1024, 1048576, 40);
    private static final Iterable<Integer> MONGO_SCALES = createSequence(128, 131072, 40);

    private final NodeStoreFixture fixture;
    private final Iterable<Integer> scales;

    private NodeStore nodeStore;
    private Repository repository;
    private Session session;

    public LargeOperationIT(NodeStoreFixture fixture, Iterable<Integer> scales) {
        assumeTrue(enabled);
        this.fixture = fixture;
        this.scales = scales;
    }

    /**
     * Create a geometrically increasing sequence of values
     * @param from    first value. Must be > 0
     * @param to      last value. Must be > {@code from}
     * @param count   number of values
     * @return  geometrically increasing sequence of {@code count} values
     * between {@code from} and {@code to}
     */
    private static List<Integer> createSequence(int from, int to, int count) {
        double slope = pow(to / (double) from, 1 / ((double) count - 1));
        List<Integer> seq = Lists.newArrayList();
        int c = 0;
        int v = from;
        do {
            seq.add(v);
            v = (int) (slope * v);
        } while (++c < count);
        return seq;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() throws Exception {
        List<Object[]> fixtures = Lists.newArrayList();
        SegmentTarFixture segmentFixture = new SegmentTarFixture();
        if (segmentFixture.isAvailable()) {
            fixtures.add(new Object[] {segmentFixture, SEGMENT_SCALES});
        }
        DocumentMongoFixture documentFixture = new DocumentMongoFixture();
        if (documentFixture.isAvailable()) {
            fixtures.add(new Object[]{documentFixture, MONGO_SCALES});
        }
        return fixtures;
    }

    private Session createAdminSession() throws RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    private static void safeLogout(Session session) {
        try {
            session.logout();
        } catch (Exception ignore) {}
    }

    @Before
    public void setup() throws RepositoryException {
        // Disable noisy logging we want to ignore for these tests
        ((LoggerContext)LoggerFactory.getILoggerFactory())
                .getLogger(DocumentNodeStore.class).setLevel(Level.ERROR);
        ((LoggerContext)LoggerFactory.getILoggerFactory())
                .getLogger("org.apache.jackrabbit.oak.jcr.observation.ChangeProcessor").setLevel(Level.ERROR);
        ((LoggerContext)LoggerFactory.getILoggerFactory())
                .getLogger(RefreshStrategy.class).setLevel(Level.ERROR);

        nodeStore = fixture.createNodeStore();
        repository  = new Jcr(nodeStore).createRepository();
        session = createAdminSession();
    }

    @After
    public void tearDown() {
        safeLogout(session);
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        fixture.dispose(nodeStore);
    }

    /**
     * Assert that the actual runtime performance is bounded by {@code O(n log n)} where
     * {@code n} is the size of the input.
     * <p>
     * This is done by comparing the slope of the measured running times against the
     * slope of {@code n log n}  (i.e. {@code d/dn n log n = 1 + log n}) for the respective
     * input size. The number of values for which the measured running time does not exceed that
     * bound is used as a test statistic for the subsequent
     * <a href="http://en.wikipedia.org/wiki/Binomial_test">binomial test</a>. The test passes
     * if the binomial test with a significance level of {@link #ALPHA} passes and fails otherwise.
     *
     * @param name    name of the test
     * @param scales  the sizes of the inputs
     * @param executionTimes  the execution times corresponding to the {@code scales}
     * @param knownIssue  log when the assertion doesn't hold but don't throw {@link AssertionError}
     */
    private static void assertOnLgn(String name, Iterable<Integer> scales,
            List<Double> executionTimes, boolean knownIssue) {
        Double n0 = null;
        Double t0 = null;
        int successes = 0;
        Iterator<Integer> ns = scales.iterator();
        for (double t : executionTimes) {
            double n = ns.next();
            if (n0 != null) {
                double dt = (t - t0) / (n - n0);  // slope of the measured running times
                double bound = 1 + log(n);        // bound of the slope for the respective input size
                if (dt < bound) {
                    successes++;
                }
            }
            n0 = n;
            t0 = t;
        }

        // number of trials is one less due to the numeric differentiation
        int trials = executionTimes.size() - 1;
        double p = new BinomialTest().binomialTest(
                trials, successes, 0.5, BinomialTest.AlternativeHypothesis.GREATER_THAN);

        boolean pass = p <= ALPHA;
        if (pass) {
            LOG.info("{} scales O(n lg n). p-value={} <= " + ALPHA, name, p);
        } else {
            LOG.error("{} does not scale O(n lg n). p-value={} > " + ALPHA, name, p);
        }
        LOG.info("Number of trials={}, Number of successes={}", trials, successes);
        LOG.info("scales={}", scales);
        LOG.info("executionTimes={}", executionTimes);
        assertTrue(name + "does not scale O(n lg n). p-value=" + p + " > " + ALPHA, knownIssue || pass);
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
        assertOnLgn("large commit", scales, executionTimes, false);
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
        boolean knownIssue = fixture.getClass() == DocumentMongoFixture.class;  // FIXME OAK-1698
        assertOnLgn("large copy", scales, executionTimes, knownIssue);
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
        boolean knownIssue = fixture.getClass() == DocumentMongoFixture.class;  // FIXME OAK-1698
        assertOnLgn("large move", scales, executionTimes, knownIssue);
    }

    @Test
    public void largeRemove() throws RepositoryException, InterruptedException {
        final Node n = session.getRootNode().addNode("large-remove", "oak:Unstructured");
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
                    session.getNode("/large-remove/s" + scale).remove();
                    session.save();
                }
            };
            double t = test.run();
            executionTimes.add(t);
            LOG.info("Removing {} node took {} ns/node", scale, t);
        }
        boolean knownIssue = fixture.getClass() == DocumentMongoFixture.class;  // FIXME OAK-1698
        assertOnLgn("large remove", scales, executionTimes, knownIssue);
    }

    /**
     * Assert adding siblings scales linearly with the number of already existing siblings.
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
                    Node s = n.addNode("s" + scale);
                    for (int k = 0; k < scale; k++) {
                        s.addNode("s" + k);
                    }
                }

                @Override
                void run(int scale) throws RepositoryException {
                    Node s = n.getNode("s" + scale);
                    for (int k = 0; k < 100; k++) {
                        s.addNode("t" + k);
                    }
                    session.save();
                }
            };
            double t = test.run();
            executionTimes.add(t);
            LOG.info("Adding 100 siblings next to {} siblings took {} ns/node", scale, t);
        }
        boolean knownIssue = fixture.getClass() == DocumentMongoFixture.class;  // FIXME OAK-1698
        assertOnLgn("many siblings", scales, executionTimes, knownIssue);
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
        boolean knownIssue = fixture.getClass() == DocumentMongoFixture.class;  // FIXME OAK-1698
        assertOnLgn("large number of pending events", scales, executionTimes, knownIssue);
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
            boolean knownIssue = fixture.getClass() == DocumentMongoFixture.class;  // FIXME OAK-1698
            assertOnLgn("slow listeners", scales, executionTimes, knownIssue);
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
                safeLogout(session);
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

        public Future<Void> start() {
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
                            safeLogout(session);
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

    //------------------------------------------------------------< BinomialTest >---
    // FIXME this class is copied from commons-math3:3.3-SNAPSHOT. Remove once 3.3 is released

    /**
     * Implements binomial test statistics.
     * <p>
     * Exact test for the statistical significance of deviations from a
     * theoretically expected distribution of observations into two categories.
     *
     * @see <a href="http://en.wikipedia.org/wiki/Binomial_test">Binomial test (Wikipedia)</a>
     * @version $Id: BinomialTest.java 1532638 2013-10-16 04:29:31Z psteitz $
     * @since 3.3
     */
    private static class BinomialTest {

        /**
         * Represents an alternative hypothesis for a hypothesis test.
         *
         * @version $Id: AlternativeHypothesis.java 1531128 2013-10-10 22:09:25Z tn $
         * @since 3.3
         */
        public enum AlternativeHypothesis {

            /**
             * Represents a two-sided test. H0: p=p0, H1: p &ne; p0
             */
            TWO_SIDED,

            /**
             * Represents a right-sided test. H0: p &le; p0, H1: p &gt; p0.
             */
            GREATER_THAN,

            /**
             * Represents a left-sided test. H0: p &ge; p0, H1: p &lt; p0.
             */
            LESS_THAN
        }

        /**
         * Returns whether the null hypothesis can be rejected with the given confidence level.
         * <p>
         * <strong>Preconditions</strong>:
         * <ul>
         * <li>Number of trials must be &ge; 0.</li>
         * <li>Number of successes must be &ge; 0.</li>
         * <li>Number of successes must be &le; number of trials.</li>
         * <li>Probability must be &ge; 0 and &le; 1.</li>
         * </ul>
         *
         * @param numberOfTrials number of trials performed
         * @param numberOfSuccesses number of successes observed
         * @param probability assumed probability of a single trial under the null hypothesis
         * @param alternativeHypothesis type of hypothesis being evaluated (one- or two-sided)
         * @param alpha significance level of the test
         * @return true if the null hypothesis can be rejected with confidence {@code 1 - alpha}
         * @throws org.apache.commons.math3.exception.NotPositiveException if {@code numberOfTrials} or {@code numberOfSuccesses} is negative
         * @throws org.apache.commons.math3.exception.OutOfRangeException if {@code probability} is not between 0 and 1
         * @throws org.apache.commons.math3.exception.MathIllegalArgumentException if {@code numberOfTrials} &lt; {@code numberOfSuccesses} or
         * if {@code alternateHypothesis} is null.
         * @see org.apache.jackrabbit.oak.jcr.LargeOperationIT.BinomialTest.AlternativeHypothesis
         */
        public boolean binomialTest(int numberOfTrials, int numberOfSuccesses, double probability,
                AlternativeHypothesis alternativeHypothesis, double alpha) {
            double pValue = binomialTest(numberOfTrials, numberOfSuccesses, probability, alternativeHypothesis);
            return pValue < alpha;
        }

        /**
         * Returns the <i>observed significance level</i>, or
         * <a href="http://www.cas.lancs.ac.uk/glossary_v1.1/hyptest.html#pvalue">p-value</a>,
         * associated with a <a href="http://en.wikipedia.org/wiki/Binomial_test"> Binomial test</a>.
         * <p>
         * The number returned is the smallest significance level at which one can reject the null hypothesis.
         * The form of the hypothesis depends on {@code alternativeHypothesis}.</p>
         * <p>
         * The p-Value represents the likelihood of getting a result at least as extreme as the sample,
         * given the provided {@code probability} of success on a single trial. For single-sided tests,
         * this value can be directly derived from the Binomial distribution. For the two-sided test,
         * the implementation works as follows: we start by looking at the most extreme cases
         * (0 success and n success where n is the number of trials from the sample) and determine their likelihood.
         * The lower value is added to the p-Value (if both values are equal, both are added). Then we continue with
         * the next extreme value, until we added the value for the actual observed sample.</p>
         * <p>
         * <strong>Preconditions</strong>:
         * <ul>
         * <li>Number of trials must be &ge; 0.</li>
         * <li>Number of successes must be &ge; 0.</li>
         * <li>Number of successes must be &le; number of trials.</li>
         * <li>Probability must be &ge; 0 and &le; 1.</li>
         * </ul></p>
         *
         * @param numberOfTrials number of trials performed
         * @param numberOfSuccesses number of successes observed
         * @param probability assumed probability of a single trial under the null hypothesis
         * @param alternativeHypothesis type of hypothesis being evaluated (one- or two-sided)
         * @return p-value
         * @throws org.apache.commons.math3.exception.NotPositiveException if {@code numberOfTrials} or {@code numberOfSuccesses} is negative
         * @throws org.apache.commons.math3.exception.OutOfRangeException if {@code probability} is not between 0 and 1
         * @throws org.apache.commons.math3.exception.MathIllegalArgumentException if {@code numberOfTrials} &lt; {@code numberOfSuccesses} or
         * if {@code alternateHypothesis} is null.
         * @see org.apache.jackrabbit.oak.jcr.LargeOperationIT.BinomialTest.AlternativeHypothesis
         */
        public double binomialTest(int numberOfTrials, int numberOfSuccesses, double probability,
                AlternativeHypothesis alternativeHypothesis) {
            if (numberOfTrials < 0) {
                throw new NotPositiveException(numberOfTrials);
            }
            if (numberOfSuccesses < 0) {
                throw new NotPositiveException(numberOfSuccesses);
            }
            if (probability < 0 || probability > 1) {
                throw new OutOfRangeException(probability, 0, 1);
            }
            if (numberOfTrials < numberOfSuccesses) {
                throw new MathIllegalArgumentException(
                        LocalizedFormats.BINOMIAL_INVALID_PARAMETERS_ORDER,
                        numberOfTrials, numberOfSuccesses);
            }
            if (alternativeHypothesis == null) {
                throw new NullArgumentException();
            }

            final BinomialDistribution distribution = new BinomialDistribution(numberOfTrials, probability);
            switch (alternativeHypothesis) {
                case GREATER_THAN:
                    return 1 - distribution.cumulativeProbability(numberOfSuccesses - 1);
                case LESS_THAN:
                    return distribution.cumulativeProbability(numberOfSuccesses);
                case TWO_SIDED:
                    int criticalValueLow = 0;
                    int criticalValueHigh = numberOfTrials;
                    double pTotal = 0;

                    while (true) {
                        double pLow = distribution.probability(criticalValueLow);
                        double pHigh = distribution.probability(criticalValueHigh);

                        if (pLow == pHigh) {
                            pTotal += 2 * pLow;
                            criticalValueLow++;
                            criticalValueHigh--;
                        } else if (pLow < pHigh) {
                            pTotal += pLow;
                            criticalValueLow++;
                        } else {
                            pTotal += pHigh;
                            criticalValueHigh--;
                        }

                        if (criticalValueLow > numberOfSuccesses || criticalValueHigh < numberOfSuccesses) {
                            break;
                        }
                    }
                    return pTotal;
                default:
                    throw new MathInternalError(LocalizedFormats. OUT_OF_RANGE_SIMPLE, alternativeHypothesis,
                            AlternativeHypothesis.TWO_SIDED, AlternativeHypothesis.LESS_THAN);
            }
        }
    }
}