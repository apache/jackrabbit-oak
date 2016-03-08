package org.apache.jackrabbit.cluster.test;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.cluster.test.test.OakClusterRepository;
import org.apache.jackrabbit.cluster.test.test.OakTestUtil;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.util.List;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.size;
import static com.jayway.awaitility.Awaitility.await;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
public class ConcurrentOakClusterTest {

    public static final Logger log = Logger.getLogger(ConcurrentOakClusterTest.class);

    @Rule
    public OakClusterRepository oakClusterRepository = new OakClusterRepository();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Test
    public void twoCluster50Threads() throws Exception {
        // given
        final int concurrentWriters = 50;
        final int numberOfNodes = 100;

        Repository repositoryOne = oakClusterRepository.repository();
        Repository repositoryTwo = oakClusterRepository.repository();

        final Session sessionOne = OakTestUtil.session(repositoryOne);

        final String existing = "existing";
        sessionOne.getRootNode().addNode(existing);
        sessionOne.save();
        sleep(1000);

        final List<ChildWriter> writers = Lists.newArrayList();

        // when

        for (int i = 0; i < concurrentWriters; i++) {
            final boolean secondClusterNode = i % 2 == 0;
            ChildWriter writer = new ChildWriter(secondClusterNode ? repositoryOne : repositoryTwo, existing, format("child_%s_%d_", secondClusterNode ? "two" : "one", i), numberOfNodes);
            writer.start();
            writers.add(writer);
        }

        await().atMost(20, MINUTES).until(new Runnable() {
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
        await().atMost(2, MINUTES).until(new Runnable() {
            public void run() {
                long size = 0;
                while (size < concurrentWriters * numberOfNodes)
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

    private class ChildWriter extends Thread {

        private final Repository repository;
        private final String parent;
        private final String child;
        private final int numberOfNodes;
        private int counter = 0;

        ChildWriter(Repository repository, String parent, String child, int numberOfNodes) throws RepositoryException {
            this.repository = repository;
            this.parent = parent;
            this.child = child;
            this.numberOfNodes = numberOfNodes;
        }

        public String status() {
            return isAlive() ? child + "=" + counter : null;
        }

        @Override
        public void run() {
            for (counter = 0; counter < numberOfNodes; counter++) {
                try {
                    Session session = OakTestUtil.session(this.repository);
                    String childName = format("%s%d", child, counter);
                    session.getRootNode().getNode(parent).addNode(childName);
                    session.save();
                    session.logout();
                } catch (RepositoryException e) {
                    log.error(format("Exception during save operation %s, %s", e.getMessage(), e.getCause().getCause().getMessage()));
                }
            }
        }
    }


    private static final Predicate<Thread> IS_ALIVE = new Predicate<Thread>() {
        public boolean apply(Thread thread) {
            return thread.isAlive();
        }
    };

    private static final Function<ChildWriter, String> TO_STATUS = new Function<ChildWriter, String>() {
        public String apply(ChildWriter childWriter) {
            return childWriter.status();
        }
    };

}