package org.apache.jackrabbit.cluster.test;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.cluster.test.insistent.InsistentChangePack;
import org.apache.jackrabbit.cluster.test.insistent.InsistentSession;
import org.apache.jackrabbit.cluster.test.insistent.impl.InsistentSessionImpl;
import org.apache.jackrabbit.cluster.test.test.OakClusterRepository;
import org.apache.jackrabbit.cluster.test.test.OakTestUtil;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import java.util.List;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.size;
import static com.jayway.awaitility.Awaitility.await;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
public class InsistentShortRunningSessionOakClusterTest {

    public static final Logger log = Logger.getLogger(InsistentShortRunningSessionOakClusterTest.class);

    @Rule
    public OakClusterRepository oakClusterRepository = new OakClusterRepository();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void writeConcurrent1() throws Exception {
        writeConcurrent(1, 10000);
    }

    @Test
    public void writeConcurrent2() throws Exception {
        writeConcurrent(2, 10000);
    }

    @Test
    public void writeConcurrent4() throws Exception {
        writeConcurrent(4, 2000);
    }

    @Test
    public void writeConcurrent10() throws Exception {
        writeConcurrent(10, 1000);
    }

    @Test
    public void writeConcurrent50() throws Exception {
        writeConcurrent(50, 100);
    }

    @Test
    public void writeConcurrent75() throws Exception {
        writeConcurrent(75, 100);
    }

    @Test
    public void writeConcurrent100() throws Exception {
        writeConcurrent(100, 30);
    }

    private void writeConcurrent(final int concurrentWriters, final int nodesPerWriter) throws RepositoryException, InterruptedException, OakClusterRepository.OakClusterRepositoryException {
        log.info(format("start %d threads and write %d per thread", concurrentWriters, nodesPerWriter));

        // given
        final Repository repositoryOne = oakClusterRepository.repository();
        final Repository repositoryTwo = oakClusterRepository.repository();

        final InsistentSession sessionOne = new InsistentSessionImpl(OakTestUtil.session(repositoryOne));
        final InsistentSession sessionTwo = new InsistentSessionImpl(OakTestUtil.session(repositoryTwo));

        final String existing = "existing";
        sessionOne.save(new InsistentChangePack() {
            @Override
            public void write() throws RepositoryException {
                sessionOne.getRootNode().addNode(existing);
            }
        });
        sleep(1000);
        sessionTwo.refresh(false);

        // when
        final List<ChildWriter> writers = Lists.newArrayList();
        for (int i = 0; i < concurrentWriters; i++) {
            final boolean secondClusterNode = i % 2 == 0;
            ChildWriter writer = new ChildWriter(secondClusterNode ? repositoryOne : repositoryTwo, existing, format("child_%s_%d_", secondClusterNode ? "two" : "one", i), nodesPerWriter);
            writer.start();
            writers.add(writer);
        }
        await().atMost(20, MINUTES).until(new Runnable() {
            @Override
            public void run() {
                while (from(writers).anyMatch(IS_ALIVE)) {
                    try {
                        log.info(format("wait for writers: %d -- %s", size(from(writers).filter(IS_ALIVE)), on(", ").skipNulls().join(from(writers).transform(TO_STATUS))));
                        sleep(5000);
                    } catch (InterruptedException e) {
                        log.error(e);
                    }
                }
            }
        });

        // then
        await().atMost(60, SECONDS).until(new Runnable() {
            @Override
            public void run() {
                long size = 0;
                while (size < concurrentWriters * nodesPerWriter)
                    try {
                        sessionOne.refresh(false);
                        size = sessionOne.getRootNode().getNode(existing).getNodes().getSize();
                        log.info("children: " + size);
                        sleep(5000);
                    } catch (Exception e) {
                        log.error(e);
                    }
            }
        });
    }

    private static final Predicate<Thread> IS_ALIVE = new Predicate<Thread>() {
        @Override
        public boolean apply(Thread thread) {
            return thread.isAlive();
        }
    };

    private static final Function<ChildWriter, String> TO_STATUS = new Function<ChildWriter, String>() {
        @Override
        public String apply(ChildWriter childWriter) {
            return childWriter.status();
        }
    };

    private class ChildWriter extends Thread {

        private final Repository repository;
        private final String parent;
        private final String child;
        private final int nodes;
        int counter = 0;

        ChildWriter(Repository repository, String parent, String child, int nodes) {
            this.repository = repository;
            this.parent = parent;
            this.child = child;
            this.nodes = nodes;
        }

        public String status() {
            return isAlive() ? child + "=" + counter : null;
        }

        @Override
        public void run() {
            for (counter = 0; counter < nodes; counter++) {
                final String childName = format("%s%d", child, counter);
                try {
                    final InsistentSession insistentSession = new InsistentSessionImpl(OakTestUtil.session(repository));
                    //log.info(format("write: %s", childName));
                    InsistentChangePack writer = new InsistentChangePack() {
                        @Override
                        public void write() throws RepositoryException {
                            insistentSession.getRootNode().getNode(parent).addNode(childName);
                        }
                    };
                    insistentSession.save(writer);
                    insistentSession.logout();
                } catch (Exception e) {
                    log.error(currentThread().getName() + " / " + childName + " / ", e);
                }
            }
        }
    }

}