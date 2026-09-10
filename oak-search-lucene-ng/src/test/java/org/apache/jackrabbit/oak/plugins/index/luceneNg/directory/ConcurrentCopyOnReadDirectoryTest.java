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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.internal.concurrent.ExecutorUtils;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.CopyOnReadDirectory.WAIT_OTHER_COPY_SYSPROP_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Port of {@code oak-lucene}'s {@code ConcurrentCopyOnReadDirectoryTest} for Lucene 9's
 * {@link Directory} API.
 * <p>
 * Two adaptations beyond the mechanical package/type renames:
 * <p>
 * 1. {@code remote} is a real {@link OakDirectory} (this module's only remote implementation
 * - see {@link LuceneNgIndexCopierTest}'s construction pattern) instead of legacy's
 * {@code RAMDirectory} stand-in, since {@code CopyOnReadDirectory}'s constructor now
 * requires {@code remote} typed {@link OakDirectory}.
 * <p>
 * 2. Legacy blocked the first CoR's copy by intercepting {@code remote}'s {@code openInput}
 * (which - on Lucene 4.7.2, what {@code oak-lucene} actually runs against - is called
 * *after* the destination's {@code createOutput} inside {@code Directory.copy}, so the
 * local placeholder file already exists, and leeching CoRs correctly take the
 * "wait for in-progress copy" branch). Lucene 9's {@code Directory.copyFrom} reverses that
 * order (confirmed via {@code javap} on the two jars' compiled {@code Directory.copy}/
 * {@code copyFrom} bytecode: destination {@code createOutput} now runs *after* source
 * {@code openInput} returns). Blocking on {@code openInput} under the new order would delay
 * {@code createOutput} itself, so the local file would never come into existence during the
 * block window - leeching CoRs would see "no local file yet" and race to copy the file
 * themselves instead of exercising {@code waitForCopyCompletion}/{@code isCopyInProgress},
 * deadlocking the test (verified empirically: porting the interception unchanged hangs both
 * tests). This port instead blocks the *local* side's {@code createOutput} - via a
 * {@code LuceneNgIndexCopier} subclass overriding {@code createLocalDirForIndexReader} to
 * wrap the very first directory it creates in a delaying {@link FilterDirectory} - preserving
 * the same intended window (local file physically exists, byte copy still pending) under the
 * new ordering.
 * <p>
 * There is also no {@code TemporarySystemProperty} JUnit rule available on this module's test
 * classpath (it lives only in oak-commons' test-jar, which isn't a dependency here), so the
 * {@code WAIT_OTHER_COPY_SYSPROP_NAME} system property is saved/restored manually.
 */
public class ConcurrentCopyOnReadDirectoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private String previousWaitCopyMillis;

    private ExecutorService executorService = null;

    private OakDirectory remote;
    private LuceneNgIndexCopier copier;

    private Directory firstCoR = null;
    private List<Future<String>> leechingCoRFutures = new ArrayList<>();
    private List<Directory> leechingCoRs = Collections.synchronizedList(new ArrayList<>());

    // Referenced (not just created) from the copier's createLocalDirForIndexReader override
    // below, so they must be fields: the override is wired up once in setup(), while these
    // latches are (re)created per-test in setupFirstCoR().
    private CountDownLatch firstCoRBlocker;
    private CountDownLatch firstCoRWaiter;
    private final AtomicBoolean firstLocalDirDelayed = new AtomicBoolean(false);

    private Future<String> firstCoRFutre;
    private LuceneNgIndexDefinition defn;

    private static final String REMOTE_INPUT_PREFIX = "Remote - ";

    @Before
    public void setup() throws Exception {
        previousWaitCopyMillis = System.getProperty(WAIT_OTHER_COPY_SYSPROP_NAME);
        System.setProperty(WAIT_OTHER_COPY_SYSPROP_NAME, String.valueOf(TimeUnit.MILLISECONDS.toMillis(30)));

        // normal remote directory - openInput is tagged so a returned IndexInput's toString()
        // reveals whether a read actually went to remote or was served from the local copy.
        NodeBuilder storageBuilder = InitialContentHelper.INITIAL_CONTENT.builder();
        remote = new OakDirectory(storageBuilder, "testIndex", false) {
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

        // Single copier instance shared by firstCoR and all leeching CoRs (directly, or via a
        // spy wrapping it - see setupLeechingCoRs) so LuceneNgIndexCopier's in-progress-copy
        // bookkeeping is genuinely shared state, not per-instance. createLocalDirForIndexReader
        // is overridden to delay only the very first local directory it ever creates (which is
        // always firstCoR's, since firstCoR is always set up before the leeching CoRs) right
        // after that directory's createOutput call returns - see class javadoc for why this
        // replaces legacy's remote-side openInput interception.
        copier = new LuceneNgIndexCopier(ExecutorUtils.directExecutor(), temporaryFolder.newFolder(), true) {
            @Override
            protected Directory createLocalDirForIndexReader(String indexPath, IndexDefinition definition,
                                                              String dirName, File localDir) throws IOException {
                Directory result = super.createLocalDirForIndexReader(indexPath, definition, dirName, localDir);
                if (!firstLocalDirDelayed.compareAndSet(false, true)) {
                    return result;
                }
                return new FilterDirectory(result) {
                    @Override
                    public IndexOutput createOutput(String name, IOContext context) throws IOException {
                        IndexOutput out;
                        try {
                            out = super.createOutput(name, context);
                        } finally {
                            // signal that the local placeholder file now exists
                            firstCoRWaiter.countDown();
                        }

                        boolean wait = true;
                        while (wait) {
                            try {
                                // block until we are signalled to let the copy proceed
                                firstCoRBlocker.await();
                                wait = false;
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }

                        return out;
                    }
                };
            }
        };

        defn = testDefinition("uid-1");
    }

    @After
    public void tearDown() {
        if (previousWaitCopyMillis == null) {
            System.clearProperty(WAIT_OTHER_COPY_SYSPROP_NAME);
        } else {
            System.setProperty(WAIT_OTHER_COPY_SYSPROP_NAME, previousWaitCopyMillis);
        }

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

        for (Directory d : IterableUtils.chainedIterable(Collections.singleton(firstCoR), leechingCoRs)) {
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
        firstCoRWaiter = new CountDownLatch(1);

        // create CoR instance to start pre-fetching in a separate thread as we want to block it mid-way
        firstCoRFutre = executorService.submit(() -> {
            try {
                String description = "firstCoR";
                Thread.currentThread().setName(description);
                firstCoR = openCoR(copier, remote, defn, description);
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
        LuceneNgIndexCopier blockingCopier = spy(copier);
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

    private String createLeechingCoR(LuceneNgIndexCopier blockingCopier, LuceneNgIndexDefinition defn, String threadName) {
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

    private static Directory openCoR(LuceneNgIndexCopier copier, OakDirectory remote, LuceneNgIndexDefinition defn,
                                     String description) throws IOException {
        Directory d = spy(copier.wrapForRead("/oak:index/foo", defn, remote, "lucene9"));
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

    /**
     * Follows LuceneNgIndexCopierTest's construction pattern: an INITIAL_CONTENT-backed
     * NodeBuilder with the lucene9 type property set, fed through
     * LuceneNgIndexDefinition.Builder (which exposes .uid(...) via the shared
     * IndexDefinition.Builder) so getUniqueId() returns the requested value.
     */
    private LuceneNgIndexDefinition testDefinition(String uniqueId) {
        NodeState root = InitialContentHelper.INITIAL_CONTENT;
        NodeBuilder builder = root.builder();
        builder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        NodeState defnState = builder.getNodeState();

        return new LuceneNgIndexDefinition.Builder()
                .root(root)
                .defn(defnState)
                .indexPath("/oak:index/foo")
                .uid(uniqueId)
                .build();
    }
}
