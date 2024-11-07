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
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.guava.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory.WAIT_OTHER_COPY_SYSPROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ConcurrentCopyOnReadDirectoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Rule
    public TemporarySystemProperty tempSysProp = new TemporarySystemProperty();

    private ExecutorService executorService = null;

    private Directory remote;
    private IndexCopier copier;

    private Directory firstCoR = null;
    private List<Future<String>> leechingCoRFutures = new ArrayList<>();
    private List<Directory> leechingCoRs = Collections.synchronizedList(new ArrayList<>());

    private CountDownLatch firstCoRBlocker;
    private Future<String> firstCoRFutre;
    private  LuceneIndexDefinition defn;

    private static final String REMOTE_INPUT_PREFIX = "Remote - ";

    @Before
    public void setup() throws Exception {
        System.setProperty(WAIT_OTHER_COPY_SYSPROP_NAME, String.valueOf(TimeUnit.MILLISECONDS.toMillis(30)));

        // normal remote directory
        remote = new RAMDirectory() {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                IndexInput ret = spy(super.openInput(name, context));
                when(ret.toString())
                        .thenAnswer(invocationOnMock -> REMOTE_INPUT_PREFIX + invocationOnMock.callRealMethod());
                return ret;
            }
        };
        IndexOutput output = remote.createOutput("file", IOContext.DEFAULT);
        output.writeString("foo");
        output.close();

        IndexInput remoteInput = remote.openInput("file", IOContext.READ);
        assertTrue(remoteInput.length() > 1);

        copier = new IndexCopier(newDirectExecutorService(), temporaryFolder.newFolder(), true);

        NodeState root = InitialContentHelper.INITIAL_CONTENT;
        defn = new LuceneIndexDefinition(root, root, "/foo");
    }

    @After
    public void tearDown() {
        // This is no-op usually but would save us in case first CoR is stuck in wait
        firstCoRBlocker.countDown();

        if (executorService != null) {
            new ExecutorCloser(executorService, 1, TimeUnit.SECONDS).close();
        }
    }

    @Test
    public void concurrentPrefetch() throws Exception {
        // setup one primary CoR and 2 subsequent ones to read. Each would run concurrently.
        setupCopiers(2);
        // let of go of CoR1 to finish its work
        firstCoRBlocker.countDown();

        assertNull("First CoR must not throw exception", firstCoRFutre.get());

        waitForLeechingCoRsToFinish();

        for (Directory d : Iterables.concat(Collections.singleton(firstCoR), leechingCoRs)) {
            IndexInput input = d.openInput("file", IOContext.READ);
            assertFalse(d + " must not be reading from remote",
                    input.toString().startsWith(REMOTE_INPUT_PREFIX));
        }
    }

    @Test
    public void concurrentPrefetchWithTimeout() throws Exception {
        // setup one primary CoR and 2 subsequent ones to read. Each would run concurrently.
        setupCopiers(2);

        // don't unblock firstCor so that leeching CoRs time out
        waitForLeechingCoRsToFinish();

        // let it go now as leeching CoRs have finished
        firstCoRBlocker.countDown();

        assertNull("First CoR must not throw exception", firstCoRFutre.get());

        IndexInput input = firstCoR.openInput("file", IOContext.READ);
        assertFalse(firstCoR + " must not be reading from remote",
                input.toString().startsWith(REMOTE_INPUT_PREFIX));

        for (Directory d : leechingCoRs) {
            input = d.openInput("file", IOContext.READ);
            assertTrue(d + " must be reading from remote",
                    input.toString().startsWith(REMOTE_INPUT_PREFIX));
        }
    }

    private void setupCopiers(int numLeechers) throws Exception {
        // 1 thread each for leeching copier and another one for the first one
        executorService = Executors.newFixedThreadPool(numLeechers + 1);

        setupFirstCoR();
        setupLeechingCoRs(numLeechers);
    }

    private void setupFirstCoR() throws Exception {
        firstCoRBlocker = new CountDownLatch(1);
        CountDownLatch firstCoRWaiter = new CountDownLatch(1);

        // Create a blocking remote for CoR1 to signal how open input progresses
        Directory blockingRemote = spy(remote);
        doAnswer(invocationOnMock -> {
            IndexInput input;
            try {
                input = (IndexInput) invocationOnMock.callRealMethod();
            } finally {
                // signal that input has been opened
                firstCoRWaiter.countDown();
            }

            // wait while we're signalled that we can be done with opening input
            boolean wait = true;
            while (wait) {
                try {
                    // block until we are signalled to call super
                    firstCoRBlocker.await();
                    wait = false;
                } catch (InterruptedException e) {
                    // ignore
                }
            }

            return input;
        }).when(blockingRemote).openInput(any(), any());

        // create CoR instance to start pre-fetching in a separate thread as we want to block it mid-way
        firstCoRFutre = executorService.submit(() -> {
            try {
                String description = "firstCoR";
                Thread.currentThread().setName(description);
                firstCoR = openCoR(copier, blockingRemote, defn, description);
                return null;
            } catch (Throwable t) {
                return getThrowableAsString(t);
            }
        });

        // wait for CoR to start fetching which we're blocking its completion via cor1Blocker latch
        firstCoRWaiter.await();
    }

    private void setupLeechingCoRs(int numLeechers) throws Exception {
        CountDownLatch leechingCoRsWaiter = new CountDownLatch(numLeechers);
        // Create a blocking copier for leeching CoRs to signal when it starts to wait for it to wait for copy completion
        IndexCopier blockingCopier = spy(copier);
        doAnswer(invocationOnMock -> {
            leechingCoRsWaiter.countDown();
            return invocationOnMock.callRealMethod();
        }).when(blockingCopier).isCopyInProgress(any());

        for (int i = 0; i < numLeechers; i++) {
            final String leecherName = "CoR-" + (i + 1);
            leechingCoRFutures.add(executorService.submit(() -> createLeechingCoR(blockingCopier, defn, leecherName)));
        }

        // wait for leeching CoRs to start
        leechingCoRsWaiter.await();
    }

    private String createLeechingCoR(IndexCopier blockingCopier, LuceneIndexDefinition defn, String threadName) {
        Thread.currentThread().setName(threadName);

        // get another directory instance with normal remote while the previous is blocked by us
        try {
            CopyOnReadDirectory dir = (CopyOnReadDirectory) openCoR(blockingCopier, remote, defn, threadName);
            leechingCoRs.add(dir);

            return null;
        } catch (Throwable t) {
            return getThrowableAsString(t);
        }
    }

    private void waitForLeechingCoRsToFinish() throws Exception {
        for (Future<String> corFuture : leechingCoRFutures) {
            assertNull("Leeching CoR must not throw exception", corFuture.get());
        }
    }

    private static Directory openCoR(IndexCopier copier, Directory remote, LuceneIndexDefinition defn,
                                     String description) throws IOException {
        Directory d = spy(copier.wrapForRead("/oak:index/foo", defn, remote, INDEX_DATA_CHILD_NAME));
        when(d.toString())
                .thenAnswer(invocationOnMock -> description);
        return d;
    }

    private static String getThrowableAsString(Throwable t) {
        StringBuilder sb = new StringBuilder(t.getMessage() + "\n");
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        sb.append(sw.getBuffer());
        return sb.toString();
    }
}
