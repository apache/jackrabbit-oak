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

package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import org.apache.jackrabbit.oak.commons.internal.concurrent.ExecutorUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class CopyOnReadDirectoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void multipleCloseCalls() throws Exception {
        AtomicInteger executionCount = new AtomicInteger();
        Executor e = r -> {executionCount.incrementAndGet(); r.run();};
        LuceneNgIndexCopier c = new LuceneNgIndexCopier(ExecutorUtils.directExecutor(), temporaryFolder.newFolder(), true);

        // remote must be a real OakDirectory (this module's only remote implementation,
        // see LuceneNgIndexCopierTest's construction pattern) rather than a generic
        // in-memory Directory stand-in - CopyOnReadDirectory's constructor now requires it.
        NodeBuilder storageBuilder = INITIAL_CONTENT.builder();
        OakDirectory remote = new OakDirectory(storageBuilder, "testIndex", false);

        // Not opened via try-with-resources: CopyOnReadDirectory.close() (below) already
        // closes `local` as part of its own close path, so a second close here would
        // double-close it.
        File localDir = temporaryFolder.newFolder();
        Directory local = FSDirectory.open(localDir.toPath());
        Directory dir = new CopyOnReadDirectory(c, remote, local, localDir, false, "foo", e);

        dir.close();
        dir.close();
        assertEquals(1, executionCount.get());
    }

    @Test
    public void copiesAreExclusiveBeforeLocalOutputExists() throws Exception {
        CountDownLatch inputOpened = new CountDownLatch(1);
        CountDownLatch allowCopy = new CountDownLatch(1);
        CountDownLatch waiterStarted = new CountDownLatch(1);
        AtomicInteger remoteOpens = new AtomicInteger();
        AtomicInteger claims = new AtomicInteger();
        OakDirectory remote = remoteWithFile();
        OakDirectory blocked = spy(remote);
        doAnswer(invocation -> {
            if (remoteOpens.incrementAndGet() == 1) {
                inputOpened.countDown();
                assertTrue(allowCopy.await(10, TimeUnit.SECONDS));
            }
            return invocation.callRealMethod();
        }).when(blocked).openInput(eq("file"), any(IOContext.class));
        File localDir = temporaryFolder.newFolder();
        LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, temporaryFolder.newFolder(), false) {
            @Override
            long startCopy(LocalIndexFile file) {
                long start = super.startCopy(file);
                if (claims.incrementAndGet() == 2) {
                    waiterStarted.countDown();
                }
                return start;
            }
        };
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try (Directory first = new CopyOnReadDirectory(copier, blocked, FSDirectory.open(localDir.toPath()),
                localDir, false, "/oak:index/test", Runnable::run);
             Directory second = new CopyOnReadDirectory(copier, remote, FSDirectory.open(localDir.toPath()),
                     localDir, false, "/oak:index/test", Runnable::run)) {
            Future<String> winner = threads.submit(() -> read(first));
            assertTrue(inputOpened.await(10, TimeUnit.SECONDS));
            assertFalse(new File(localDir, "file").exists());
            Future<String> waiter = threads.submit(() -> read(second));
            assertTrue(waiterStarted.await(10, TimeUnit.SECONDS));
            allowCopy.countDown();
            assertEquals("content", winner.get(10, TimeUnit.SECONDS));
            assertEquals("content", waiter.get(10, TimeUnit.SECONDS));
            assertEquals("content", read(first));
            assertEquals("content", read(second));
            assertTrue(new File(localDir, "file").isFile());
            assertEquals(1, copier.getDownloadCount());
        } finally {
            allowCopy.countDown();
            threads.shutdown();
            assertTrue(threads.awaitTermination(10, TimeUnit.SECONDS));
            copier.close();
        }
    }

    @Test
    public void failedSyncDoesNotPublishOrLeaveCopyInProgress() throws Exception {
        File localDir = temporaryFolder.newFolder();
        OakDirectory remote = remoteWithFile();
        LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, temporaryFolder.newFolder(), false);
        Directory failing = new FilterDirectory(FSDirectory.open(localDir.toPath())) {
            @Override
            public void sync(Collection<String> names) throws IOException {
                throw new IOException("sync failed");
            }
        };
        LocalIndexFile file = new LocalIndexFile(failing, "file", remote.fileLength("file"), true);
        try (Directory first = new CopyOnReadDirectory(copier, remote, failing, localDir, false,
                "/oak:index/test", Runnable::run)) {
            assertEquals("content", read(first));
            assertEquals(0, copier.getReaderLocalReadCount());
            assertFalse(copier.isCopyInProgress(file));
            assertEquals(0, copier.getDownloadCount());
        }
        try (Directory retry = new CopyOnReadDirectory(copier, remote, FSDirectory.open(localDir.toPath()),
                localDir, false, "/oak:index/test", Runnable::run)) {
            assertEquals("content", read(retry));
            assertEquals(1, copier.getDownloadCount());
        } finally {
            copier.close();
        }
    }

    @Test
    public void failedCopyReleasesOwnershipAndCanBeRetriedByAnotherGeneration() throws Exception {
        OakDirectory remote = spy(remoteWithFile());
        AtomicInteger opens = new AtomicInteger();
        doAnswer(invocation -> {
            if (opens.incrementAndGet() == 1) {
                throw new IOException("copy input failed");
            }
            return invocation.callRealMethod();
        }).when(remote).openInput(eq("file"), any(IOContext.class));
        File localDir = temporaryFolder.newFolder();
        try (LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, temporaryFolder.newFolder(), false)) {
            LocalIndexFile file;
            try (Directory local = FSDirectory.open(localDir.toPath())) {
                file = new LocalIndexFile(local, "file", remote.fileLength("file"), true);
            }
            try (Directory first = new CopyOnReadDirectory(copier, remote, FSDirectory.open(localDir.toPath()),
                    localDir, false, "/oak:index/test", Runnable::run)) {
                assertEquals("content", read(first));
                assertFalse(copier.isCopyInProgress(file));
                assertEquals(0, copier.getDownloadCount());
            }
            try (Directory second = new CopyOnReadDirectory(copier, remote, FSDirectory.open(localDir.toPath()),
                    localDir, false, "/oak:index/test", Runnable::run)) {
                assertEquals("content", read(second));
                assertEquals(1, copier.getDownloadCount());
            }
        }
    }

    private OakDirectory remoteWithFile() throws IOException {
        OakDirectory remote = new OakDirectory(INITIAL_CONTENT.builder(), "testIndex", false);
        try (IndexOutput output = remote.createOutput("file", IOContext.DEFAULT)) {
            output.writeString("content");
        }
        return remote;
    }

    private String read(Directory directory) throws IOException {
        try (IndexInput input = directory.openInput("file", IOContext.READ)) {
            return input.readString();
        }
    }
}
