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

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory.WAIT_OTHER_COPY_SYSPROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Tests for concurrent CopyOnReadDirectory behavior.
 *
 * These tests verify that when multiple CopyOnReadDirectory instances try to copy
 * the same file concurrently, they properly coordinate:
 * 1. One CoR starts copying and others wait for it to complete
 * 2. If the wait times out, the waiting CoRs read from remote instead
 *
 * In Lucene 5.x, copyFrom() is atomic - it either completes or doesn't create the file.
 * To test concurrent copy behavior, we use a TestableIndexCopier that:
 * 1. Creates a partial local file during startCopy() to simulate an in-progress copy
 * 2. Blocks until signalled to complete the copy
 * This allows leeching CoRs to see the local file and wait for the copy to complete.
 */
public class ConcurrentCopyOnReadDirectoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Rule
    public TemporarySystemProperty tempSysProp = new TemporarySystemProperty();

    private ExecutorService executorService = null;

    private Directory remote;
    private TestableIndexCopier copier;

    private Directory firstCoR = null;
    private List<Future<String>> leechingCoRFutures = new ArrayList<>();
    private List<Directory> leechingCoRs = Collections.synchronizedList(new ArrayList<>());

    private CountDownLatch firstCoRBlocker;
    private Future<String> firstCoRFutre;
    private LuceneIndexDefinition defn;

    private static final String REMOTE_INPUT_PREFIX = "Remote - ";

    @Before
    public void setup() throws Exception {
        // Set a short timeout for waiting on other copies (30ms)
        System.setProperty(WAIT_OTHER_COPY_SYSPROP_NAME, String.valueOf(30));

        // Create remote directory with a test file
        remote = new RAMDirectory();
        IndexOutput output = remote.createOutput("file", IOContext.DEFAULT);
        output.writeString("foo");
        output.close();

        IndexInput remoteInput = remote.openInput("file", IOContext.READ);
        assertTrue(remoteInput.length() > 1);
        remoteInput.close();

        // Use a testable IndexCopier that allows us to control copy blocking
        copier = new TestableIndexCopier(temporaryFolder.newFolder(), true);

        NodeState root = InitialContentHelper.INITIAL_CONTENT;
        defn = new LuceneIndexDefinition(root, root, "/foo");
    }

    @After
    public void tearDown() {
        // Unblock any blocked copies
        if (firstCoRBlocker != null) {
            firstCoRBlocker.countDown();
        }
        if (copier != null) {
            copier.unblockAllCopies();
        }

        if (executorService != null) {
            new ExecutorCloser(executorService, 1, TimeUnit.SECONDS).close();
        }
    }

    @Test(timeout = 30000)
    public void concurrentPrefetch() throws Exception {
        // Setup: one primary CoR and 2 subsequent ones to read concurrently
        setupCopiers(2);

        // Let the first CoR finish its work
        firstCoRBlocker.countDown();

        assertNull("First CoR must not throw exception", firstCoRFutre.get());

        waitForLeechingCoRsToFinish();

        // All directories should be reading from local (not remote)
        for (Directory d : IterableUtils.chainedIterable(Collections.singleton(firstCoR), leechingCoRs)) {
            IndexInput input = d.openInput("file", IOContext.READ);
            assertFalse(d + " must not be reading from remote",
                    input.toString().startsWith(REMOTE_INPUT_PREFIX));
            input.close();
        }
    }

    @Test(timeout = 30000)
    public void concurrentPrefetchWithTimeout() throws Exception {
        // Setup: one primary CoR and 2 subsequent ones to read concurrently
        setupCopiers(2);

        // Don't unblock firstCoR so that leeching CoRs time out waiting
        waitForLeechingCoRsToFinish();

        // Now let the first CoR finish
        firstCoRBlocker.countDown();

        assertNull("First CoR must not throw exception", firstCoRFutre.get());

        // First CoR should read from local
        IndexInput input = firstCoR.openInput("file", IOContext.READ);
        assertFalse(firstCoR + " must not be reading from remote",
                input.toString().startsWith(REMOTE_INPUT_PREFIX));
        input.close();

        // Leeching CoRs timed out waiting for copy, so they read from remote
        for (Directory d : leechingCoRs) {
            input = d.openInput("file", IOContext.READ);
            assertTrue(d + " must be reading from remote",
                    input.toString().startsWith(REMOTE_INPUT_PREFIX));
            input.close();
        }
    }

    private void setupCopiers(int numLeechers) throws Exception {
        executorService = Executors.newFixedThreadPool(numLeechers + 1);

        setupFirstCoR();
        setupLeechingCoRs(numLeechers);
    }

    private void setupFirstCoR() throws Exception {
        firstCoRBlocker = new CountDownLatch(1);
        CountDownLatch firstCoRWaiter = new CountDownLatch(1);

        // Configure the copier to block during copy and signal when copy starts
        copier.setBlockOnCopy(true, firstCoRBlocker, firstCoRWaiter);

        // Create CoR instance in a separate thread (it will block during prefetch)
        firstCoRFutre = executorService.submit(() -> {
            try {
                String description = "firstCoR";
                Thread.currentThread().setName(description);
                firstCoR = copier.wrapForRead("/oak:index/foo", defn, remote, INDEX_DATA_CHILD_NAME);
                return null;
            } catch (Throwable t) {
                return getThrowableAsString(t);
            }
        });

        // Wait for the copy to start (it will be blocked)
        firstCoRWaiter.await();
    }

    private void setupLeechingCoRs(int numLeechers) throws Exception {
        CountDownLatch leechingCoRsWaiter = new CountDownLatch(numLeechers);

        // Create a remote directory that marks its inputs with a prefix
        Directory remoteWithPrefix = createRemoteWithPrefix();

        // Create a spy copier that signals when isCopyInProgress is called
        IndexCopier blockingCopier = spy(copier);
        doAnswer(invocationOnMock -> {
            leechingCoRsWaiter.countDown();
            return invocationOnMock.callRealMethod();
        }).when(blockingCopier).isCopyInProgress(any());

        // Disable blocking for leeching CoRs - they should wait for the first copy
        copier.setBlockOnCopy(false, null, null);

        for (int i = 0; i < numLeechers; i++) {
            final String leecherName = "CoR-" + (i + 1);
            leechingCoRFutures.add(executorService.submit(() -> {
                Thread.currentThread().setName(leecherName);
                try {
                    CopyOnReadDirectory dir = (CopyOnReadDirectory) blockingCopier.wrapForRead(
                            "/oak:index/foo", defn, remoteWithPrefix, INDEX_DATA_CHILD_NAME);
                    leechingCoRs.add(dir);
                    return null;
                } catch (Throwable t) {
                    return getThrowableAsString(t);
                }
            }));
        }

        // Wait for leeching CoRs to start checking for copy in progress
        leechingCoRsWaiter.await();
    }

    private Directory createRemoteWithPrefix() {
        return new FilterDirectory(remote) {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                final IndexInput delegate = in.openInput(name, context);
                return new IndexInput(REMOTE_INPUT_PREFIX + delegate.toString()) {
                    @Override public void close() throws IOException { delegate.close(); }
                    @Override public long getFilePointer() { return delegate.getFilePointer(); }
                    @Override public void seek(long pos) throws IOException { delegate.seek(pos); }
                    @Override public long length() { return delegate.length(); }
                    @Override public IndexInput slice(String desc, long off, long len) throws IOException {
                        return delegate.slice(desc, off, len);
                    }
                    @Override public byte readByte() throws IOException { return delegate.readByte(); }
                    @Override public void readBytes(byte[] b, int off, int len) throws IOException {
                        delegate.readBytes(b, off, len);
                    }
                };
            }
        };
    }

    private void waitForLeechingCoRsToFinish() throws Exception {
        for (Future<String> corFuture : leechingCoRFutures) {
            assertNull("Leeching CoR must not throw exception", corFuture.get());
        }
    }

    private static String getThrowableAsString(Throwable t) {
        StringBuilder sb = new StringBuilder(t.getMessage() + "\n");
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        sb.append(sw.getBuffer());
        return sb.toString();
    }

    /**
     * A testable IndexCopier that can block during copy operations and create
     * a partial local file to simulate an in-progress copy.
     *
     * This creates a partial local file during startCopy() so that leeching CoRs
     * will see the file exists and wait for the copy to complete.
     */
    private static class TestableIndexCopier extends IndexCopier {
        private volatile boolean blockOnCopy = false;
        private volatile CountDownLatch copyBlocker;
        private volatile CountDownLatch copyStarted;
        private final AtomicBoolean hasBlocked = new AtomicBoolean(false);

        public TestableIndexCopier(File indexRootDir, boolean prefetchEnabled) throws IOException {
            super(Executors.newSingleThreadExecutor(), indexRootDir, prefetchEnabled);
        }

        public void setBlockOnCopy(boolean block, CountDownLatch blocker, CountDownLatch started) {
            this.blockOnCopy = block;
            this.copyBlocker = blocker;
            this.copyStarted = started;
            this.hasBlocked.set(false);
        }

        public void unblockAllCopies() {
            if (copyBlocker != null) {
                copyBlocker.countDown();
            }
        }

        @Override
        public long startCopy(LocalIndexFile file) {
            long result = super.startCopy(file);

            // Block only once (for the first copy)
            if (blockOnCopy && hasBlocked.compareAndSet(false, true)) {
                // Create a partial local file so leeching CoRs will see it exists
                // This simulates an in-progress copy
                try {
                    File localFile = new File(file.getKey());
                    localFile.getParentFile().mkdirs();
                    // Create a file with size 1 (different from remote size)
                    java.io.FileOutputStream fos = new java.io.FileOutputStream(localFile);
                    fos.write(0);
                    fos.close();
                } catch (IOException e) {
                    // Ignore - test will fail if file creation fails
                }

                // Signal that copy has started (file is now marked as "copy in progress")
                if (copyStarted != null) {
                    copyStarted.countDown();
                }

                // Block until signalled to proceed
                if (copyBlocker != null) {
                    while (true) {
                        try {
                            copyBlocker.await();
                            break;
                        } catch (InterruptedException e) {
                            // ignore and retry
                        }
                    }
                }
            }

            return result;
        }
    }
}
