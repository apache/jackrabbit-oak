package org.apache.jackrabbit.cluster.test;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.cluster.test.test.OakClusterRepository;
import org.apache.jackrabbit.cluster.test.test.OakTestUtil;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.jcr.Repository;
import javax.jcr.Session;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.FluentIterable.from;
import static com.jayway.awaitility.Awaitility.await;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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
    public void saveManyChildrenOnBothClusterNodes_success() throws Exception {
        // given

        Repository repositoryOne = oakClusterRepository.repository();
        Repository repositoryTwo = oakClusterRepository.repository();

        Session sessionOne = OakTestUtil.session(repositoryOne);
        Session sessionTwo = OakTestUtil.session(repositoryTwo);

        final String existing = "existing";
        sessionOne.getRootNode().addNode(existing);
        sessionOne.save();
        sleep(1000);
        sessionTwo.refresh(false);

        // when

        final List<ChildWriter> writers = Lists.newArrayList();
        final int concurrentWriters = 100;

        for (int i = 0; i < concurrentWriters; i++) {
            final boolean secondClusterNode = i % 2 == 0;
            ChildWriter writer = new ChildWriter(secondClusterNode ? sessionOne : sessionTwo, existing, format("child_%s_%d_", secondClusterNode ? "two" : "one", i));
            writer.start();
            writers.add(writer);
        }

        await().atMost(2, TimeUnit.MINUTES).until(new Runnable() {
            @Override
            public void run() {
                while (from(writers).anyMatch(IS_ALIVE)) {
                    try {
                        log.info("wait for writers");
                        sleep(5000);
                    } catch (InterruptedException e) {
                        log.error(e);
                    }
                }
            }
        });

        sleep(10000);

        // then
        sessionOne.refresh(false);
        final long size = sessionOne.getRootNode().getNode(existing).getNodes().getSize();
        log.info("children: " + size);
        assertThat(size, is(concurrentWriters * 10l));
    }

    private static final Predicate<Thread> IS_ALIVE = new Predicate<Thread>() {
        @Override
        public boolean apply(Thread thread) {
            return thread.isAlive();
        }
    };

    private class ChildWriter extends Thread {

        private final Session session;
        private final String parent;
        private final String child;

        ChildWriter(Session session, String parent, String child) {
            this.session = session;
            this.parent = parent;
            this.child = child;
        }

        @Override
        public void run() {
            for (int i = 0; i < 10; i++) {
                String childName = "";
                try {
                    childName = format("%s%d", child, i);
                    log.info(format("write: %s", childName));
                    session.getRootNode().getNode(parent).addNode(childName);
                    session.save();
                    //session.refresh(false);
                } catch (Exception e) {
                    log.error(currentThread().getName() + " / " + childName + " / " + e.getMessage());
                }
            }
        }
    }

}