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

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback.IndexProgress.COMMIT_FAILED;
import static org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback.IndexProgress.COMMIT_SUCCEDED;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.CIHelper;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollector;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollectorImpl;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.BlobDeletionCallback;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.collections.Sets;

public class ActiveDeletedBlobCollectorTest {
    @Rule
    public TemporaryFolder blobCollectionRoot = new TemporaryFolder(new File("target"));

    private Clock clock;
    private ChunkDeletionTrackingBlobStore blobStore;
    private ActiveDeletedBlobCollector adbc;

    @Before
    public void setup() throws Exception {
        clock = new Clock.Virtual();
        blobStore = new ChunkDeletionTrackingBlobStore();
        createBlobCollector();
    }

    private void createBlobCollector() {
        adbc = new ActiveDeletedBlobCollectorImpl(clock,
                new File(blobCollectionRoot.getRoot(), "/a"), sameThreadExecutor());
    }

    @Test
    public void simpleCase() throws Exception {
        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();

        bdc.deleted("blobId", Collections.singleton("/a"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);

        verifyBlobsDeleted("blobId");
    }

    @Test
    public void noopDoesNothing() throws Exception {
        adbc = ActiveDeletedBlobCollectorFactory.NOOP;
        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();

        bdc.deleted("blobId", Collections.singleton("/a"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);

        verifyBlobsDeleted();
    }

    @Test
    public void blobTimestampMustBeBiggerThanFileTimestamp() throws Exception {
        BlobDeletionCallback bdc1 = adbc.getBlobDeletionCallback();
        bdc1.deleted("blobId1", Collections.singleton("/a"));
        bdc1.commitProgress(COMMIT_SUCCEDED);

        BlobDeletionCallback bdc2 = adbc.getBlobDeletionCallback();
        bdc2.deleted("blobId2", Collections.singleton("/b"));

        BlobDeletionCallback bdc3 = adbc.getBlobDeletionCallback();
        bdc3.deleted("blobId3", Collections.singleton("/c"));
        bdc3.commitProgress(COMMIT_SUCCEDED);

        long time = clock.getTimeIncreasing();
        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(1));

        bdc2.commitProgress(COMMIT_SUCCEDED);

        adbc.purgeBlobsDeleted(time, blobStore);

        //blobId2 is committed later
        verifyBlobsDeleted("blobId1", "blobId3");
    }

    @Test
    public void uncommittedDeletionsMustNotBePurged() throws Exception {
        BlobDeletionCallback bdc1 = adbc.getBlobDeletionCallback();
        bdc1.deleted("blobId1", Collections.singleton("/a"));
        bdc1.commitProgress(COMMIT_FAILED);

        BlobDeletionCallback bdc2 = adbc.getBlobDeletionCallback();
        bdc2.deleted("blobId2", Collections.singleton("/b"));
        bdc2.commitProgress(COMMIT_SUCCEDED);

        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);

        //blobId2 is committed later
        verifyBlobsDeleted("blobId2");
    }

    @Test
    public void deleteBlobsDespiteFileExplicitlyPurgedBeforeRestart() throws Exception {
        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();
        bdc.deleted("blobId1", Collections.singleton("/a"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(1));
        createBlobCollector();
        bdc = adbc.getBlobDeletionCallback();
        bdc.deleted("blobId2", Collections.singleton("/b"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(1));
        createBlobCollector();
        bdc = adbc.getBlobDeletionCallback();
        bdc.deleted("blobId3", Collections.singleton("/c"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);

        verifyBlobsDeleted("blobId1", "blobId2", "blobId3");
    }

    @Test
    public void multiThreadedCommits() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        File rootDirectory = new File(blobCollectionRoot.getRoot(), "b");
        FileUtils.forceMkdir(rootDirectory);
        adbc = new ActiveDeletedBlobCollectorImpl(clock, rootDirectory, executorService);

        int numThreads = 4;
        int numBlobsPerThread = 500;

        List<Thread> threads = new ArrayList<>(numThreads);
        final AtomicInteger threadIndex = new AtomicInteger(0);
        for (; threadIndex.get() < numThreads; threadIndex.incrementAndGet()) {
            threads.add(new Thread(new Runnable() {
                private int thisThreadNum = threadIndex.get();
                @Override
                public void run() {
                    int blobCnt = 0;
                    while (blobCnt < numBlobsPerThread) {
                        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();
                        for (; blobCnt < numBlobsPerThread;) {
                            String id = "Thread" + thisThreadNum + "Blob" + blobCnt;
                            bdc.deleted(id, Collections.singleton(id));
                            blobCnt++;
                            if (Math.random() > 0.5) {
                                break;
                            }
                        }
                        bdc.commitProgress(COMMIT_SUCCEDED);
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }));
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        boolean timeout = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        assertFalse(timeout);

        List<String> deletedChunks = new ArrayList<>(numThreads*numBlobsPerThread*2);
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            for (int blobCnt = 0; blobCnt < numBlobsPerThread; blobCnt++) {
                String id = "Thread" + threadNum + "Blob" + blobCnt;
                Iterators.addAll(deletedChunks, blobStore.resolveChunks(id));
            }
        }

        // Blocking queue doesn't supply all the items immediately.
        // So, we'd push "MARKER*" blob ids and purge until some marker blob
        // gets purged. BUT, we'd time-out this activity in 3 seconds
        long until = Clock.SIMPLE.getTime() + TimeUnit.SECONDS.toMillis(3);
        List<String> markerChunks = Lists.newArrayList();
        int i = 0;
        while (Clock.SIMPLE.getTime() < until) {
            // Push commit with a marker blob-id and wait for it to be purged
            BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();
            String markerBlobId = "MARKER-" + (i++);
            bdc.deleted(markerBlobId, Lists.newArrayList(markerBlobId));
            bdc.commitProgress(COMMIT_SUCCEDED);

            Iterators.addAll(markerChunks, blobStore.resolveChunks(markerBlobId));
            clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(5));
            adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);

            if (blobStore.markerChunkDeleted) {
                break;
            }
        }

        assertTrue("Timed out while waiting for marker chunk to be purged", blobStore.markerChunkDeleted);

        // don't care how many marker blobs are purged
        blobStore.deletedChunkIds.removeAll(markerChunks);

        HashSet<String> list = new HashSet<>(deletedChunks);
        list.removeAll(blobStore.deletedChunkIds);
        assertTrue("size: " + list.size() + "; list: " + list.toString(), list.isEmpty());

        assertThat(blobStore.deletedChunkIds, containsInAnyOrder(deletedChunks.toArray()));
    }

    @Test
    public void inaccessibleWorkDirGivesNoop() throws Exception {
        assumeFalse(CIHelper.windows());

        File rootDir = blobCollectionRoot.getRoot();
        File unwritableExistingRootFolder = new File(rootDir, "existingRoot");
        FileUtils.forceMkdir(unwritableExistingRootFolder);
        File unwritableNonExistingRootFolder = new File(unwritableExistingRootFolder, "existingRoot");

        Path unwritableExistingPath = FileSystems.getDefault().getPath(unwritableExistingRootFolder.getPath());
        Files.setPosixFilePermissions(unwritableExistingPath,
                Sets.newSet(PosixFilePermission.OWNER_READ,
                        PosixFilePermission.GROUP_READ,
                        PosixFilePermission.OTHERS_READ));

        adbc = ActiveDeletedBlobCollectorFactory.newInstance(unwritableExistingRootFolder, sameThreadExecutor());
        assertEquals("Unwritable existing root folder must have NOOP active blob collector",
                ActiveDeletedBlobCollectorFactory.NOOP, adbc);

        adbc = ActiveDeletedBlobCollectorFactory.newInstance(unwritableNonExistingRootFolder, sameThreadExecutor());
        assertEquals("Unwritable non-existing root folder must have NOOP active blob collector",
                ActiveDeletedBlobCollectorFactory.NOOP, adbc);
    }

    @Test
    public void cancellablePurge() throws Exception {
        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();
        for (int i = 0; i < 10; i++) {
            String id = "Blob" + i;
            bdc.deleted(id, Collections.singleton(id));
        }
        bdc.commitProgress(COMMIT_SUCCEDED);

        Semaphore purgeBlocker = new Semaphore(0);
        blobStore.callback = () -> purgeBlocker.acquireUninterruptibly();
        Thread purgeThread = new Thread(() -> {
            try {
                adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        purgeThread.setDaemon(true);
        purgeBlocker.release(10);//allow 5 deletes
        purgeThread.start();

        boolean deleted5 = waitFor(5000, () -> blobStore.deletedChunkIds.size() >= 10);
        assertTrue("Deleted " + blobStore.deletedChunkIds.size() + " chunks", deleted5);

        adbc.cancelBlobCollection();
        purgeBlocker.release(20);//release all that's there... this is more than needed, btw.

        boolean deleted6 = waitFor(5000, () -> blobStore.deletedChunkIds.size() >= 12);
        assertTrue("Haven't deleted another blob which was locked earlier.", deleted6);

        boolean cancelWorked = waitFor(5000, () -> !purgeThread.isAlive());
        assertTrue("Cancel didn't let go of purge thread in 2 seconds", cancelWorked);

        assertTrue("Cancelling purge must return asap", blobStore.deletedChunkIds.size() == 12);
    }

    @Test
    public void resumeCancelledPurge() throws Exception {
        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();
        for (int i = 0; i < 10; i++) {
            String id = "Blob" + i;
            bdc.deleted(id, Collections.singleton(id));
        }
        bdc.commitProgress(COMMIT_SUCCEDED);

        Semaphore purgeBlocker = new Semaphore(0);
        blobStore.callback = () -> purgeBlocker.acquireUninterruptibly();
        Thread purgeThread = new Thread(() -> {
            try {
                adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        purgeThread.setDaemon(true);
        purgeBlocker.release(10);//allow 5 deletes
        purgeThread.start();

        waitFor(5000, () -> blobStore.deletedChunkIds.size() >= 10);

        adbc.cancelBlobCollection();
        purgeBlocker.release(22);//release all that's there... this is more than needed, btw.

        waitFor(5000, () -> blobStore.deletedChunkIds.size() >= 12);

        waitFor(5000, () -> !purgeThread.isAlive());

        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);

        // Resume can re-attempt to delete already deleted blobs. Hence, the need for for ">="
        assertEquals("All blobs must get deleted", 20, blobStore.deletedChunkIds.size());
    }

    @Test
    public void dontWarnWhileErrorsWhileDeletingBlobs() throws Exception {
        LogCustomizer warnLogCustomizer =
                LogCustomizer.forLogger(ActiveDeletedBlobCollectorFactory.class.getName()).enable(Level.WARN)
                        .contains("Exception occurred while ")
                        .create();

        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();
        bdc.deleted("blobId1", Collections.singleton("/a"));
        bdc.deleted("blobId2", Collections.singleton("/b"));
        bdc.deleted("blobId3", Collections.singleton("/c"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        List<String> externallyDeletedChunks = Lists.newArrayList(blobStore.resolveChunks("blobId2"));
        blobStore.countDeleteChunks(externallyDeletedChunks, 0);

        warnLogCustomizer.starting();
        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);
        blobStore.deletedChunkIds.removeAll(externallyDeletedChunks);

        verifyBlobsDeleted("blobId1", "blobId3");

        assertEquals("No warn logs must show up: " + warnLogCustomizer.getLogs(), 0, warnLogCustomizer.getLogs().size());
        warnLogCustomizer.finished();

        bdc = adbc.getBlobDeletionCallback();
        bdc.deleted("blobId4", Collections.singleton("/d"));
        bdc.deleted("blobId5", Collections.singleton("/e"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        blobStore.resetLists();
        blobStore.failWithDSEForChunkIds.addAll(Lists.newArrayList(blobStore.resolveChunks("blobId4")));

        warnLogCustomizer.starting();
        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);

        verifyBlobsDeleted("blobId3", "blobId5");

        assertEquals("No warn logs must show up", 0, warnLogCustomizer.getLogs().size());
        warnLogCustomizer.finished();

        bdc = adbc.getBlobDeletionCallback();
        bdc.deleted("blobId6", Collections.singleton("/f"));
        bdc.deleted("blobId7", Collections.singleton("/g"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        blobStore.resetLists();
        blobStore.failWithExceptionForChunkIds.addAll(Lists.newArrayList(blobStore.resolveChunks("blobId6")));

        warnLogCustomizer.starting();
        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);

        verifyBlobsDeleted("blobId5", "blobId7");

        assertEquals("General exception must log a warn", 1, warnLogCustomizer.getLogs().size());
        warnLogCustomizer.finished();
    }

    @Test
    public void doDebugLogWhileErrorsWhileDeletingBlobs() throws Exception {
        LogCustomizer warnLogCustomizer =
                LogCustomizer.forLogger(ActiveDeletedBlobCollectorFactory.class.getName()).enable(Level.DEBUG)
                        .contains("Exception occurred while ")
                        .create();

        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();
        bdc.deleted("blobId1", Collections.singleton("/a"));
        bdc.deleted("blobId2", Collections.singleton("/b"));
        bdc.deleted("blobId3", Collections.singleton("/c"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        List<String> externallyDeletedChunks = Lists.newArrayList(blobStore.resolveChunks("blobId2"));
        blobStore.countDeleteChunks(externallyDeletedChunks, 0);

        warnLogCustomizer.starting();
        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);
        blobStore.deletedChunkIds.removeAll(externallyDeletedChunks);

        verifyBlobsDeleted("blobId1", "blobId3");

        assertEquals("Should log on debug", 1, warnLogCustomizer.getLogs().size());
        warnLogCustomizer.finished();

        bdc = adbc.getBlobDeletionCallback();
        bdc.deleted("blobId4", Collections.singleton("/d"));
        bdc.deleted("blobId5", Collections.singleton("/e"));
        bdc.commitProgress(COMMIT_SUCCEDED);

        blobStore.resetLists();
        blobStore.failWithDSEForChunkIds.addAll(Lists.newArrayList(blobStore.resolveChunks("blobId4")));

        warnLogCustomizer.starting();
        adbc.purgeBlobsDeleted(clock.getTimeIncreasing(), blobStore);

        verifyBlobsDeleted("blobId3", "blobId5");

        assertEquals("Should log on debug", 1, warnLogCustomizer.getLogs().size());
        warnLogCustomizer.finished();
    }

    // OAK-6950
    @Test
    public void pauseMarkingDeletedBlobs() {
        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();
        assertFalse("Active deletion should be safe by default", bdc.isMarkingForActiveDeletionUnsafe());

        adbc.flagActiveDeletionUnsafe(true);
        bdc = adbc.getBlobDeletionCallback();
        assertTrue("Active deletion should be unsafe", bdc.isMarkingForActiveDeletionUnsafe());

        adbc.flagActiveDeletionUnsafe(false);
        bdc = adbc.getBlobDeletionCallback();
        assertFalse("Active deletion should be safe after unpausing", bdc.isMarkingForActiveDeletionUnsafe());
    }

    // OAK-6950
    @Test
    public void pauseMarkingDeletedBlobsNOOP() {
        adbc = ActiveDeletedBlobCollectorFactory.NOOP;
        BlobDeletionCallback bdc = adbc.getBlobDeletionCallback();
        assertFalse("Active deletion should be safe by default", bdc.isMarkingForActiveDeletionUnsafe());

        adbc.flagActiveDeletionUnsafe(true);
        bdc = adbc.getBlobDeletionCallback();
        assertTrue("Active deletion should be unsafe", bdc.isMarkingForActiveDeletionUnsafe());

        adbc.flagActiveDeletionUnsafe(false);
        bdc = adbc.getBlobDeletionCallback();
        assertFalse("Active deletion should be safe after unpausing", bdc.isMarkingForActiveDeletionUnsafe());
    }

    private void verifyBlobsDeleted(String ... blobIds) throws IOException {
        List<String> chunkIds = new ArrayList<>();
        for (String blobId : blobIds) {
            chunkIds.addAll(Lists.newArrayList(blobStore.resolveChunks(blobId)));
        }

        assertThat(blobStore.deletedChunkIds, containsInAnyOrder(chunkIds.toArray()));
    }

    class ChunkDeletionTrackingBlobStore implements GarbageCollectableBlobStore {
        Set<String> deletedChunkIds = com.google.common.collect.Sets.newLinkedHashSet();
        Set<String> failWithDSEForChunkIds = com.google.common.collect.Sets.newLinkedHashSet();
        Set<String> failWithExceptionForChunkIds = com.google.common.collect.Sets.newLinkedHashSet();
        Runnable callback = null;
        volatile boolean markerChunkDeleted = false;

        @Override
        public String writeBlob(InputStream in) throws IOException {
            return null;
        }

        @Override
        public String writeBlob(InputStream in, BlobOptions options) throws IOException {
            return null;
        }

        @Override
        public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
            return 0;
        }

        @Override
        public long getBlobLength(String blobId) throws IOException {
            return 0;
        }

        @Override
        public InputStream getInputStream(String blobId) throws IOException {
            return null;
        }

        @Override
        public String getBlobId(@Nonnull String reference) {
            return null;
        }

        @Override
        public String getReference(@Nonnull String blobId) {
            return null;
        }

        @Override
        public void setBlockSize(int x) {

        }

        @Override
        public String writeBlob(String tempFileName) throws IOException {
            return null;
        }

        @Override
        public int sweep() throws IOException {
            return 0;
        }

        @Override
        public void startMark() throws IOException {

        }

        @Override
        public void clearInUse() {

        }

        @Override
        public void clearCache() {

        }

        @Override
        public long getBlockSizeMin() {
            return 0;
        }

        @Override
        public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
            return null;
        }

        @Override
        public boolean deleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
            setMarkerChunkDeletedFlag(chunkIds);
            deletedChunkIds.addAll(chunkIds);
            return true;
        }

        @Override
        public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
            setMarkerChunkDeletedFlag(chunkIds);

            long count = 0;
            for (String chunkId : chunkIds) {
                if (deletedChunkIds.contains(chunkId)) {
                    throw new DataStoreException("Already deleted chunk: " + chunkId);
                } else if (failWithDSEForChunkIds.contains(chunkId)) {
                    throw new DataStoreException("Synthetically failing with DSE for chunk: " + chunkId);
                } else if (failWithExceptionForChunkIds.contains(chunkId)) {
                    throw new Exception("Synthetically failing with Exception for chunk: " + chunkId);
                }
                deletedChunkIds.add(chunkId);
                count++;
            }
            return count;
        }

        private void setMarkerChunkDeletedFlag(List<String> deletedChunkIds) {
            if (!markerChunkDeleted) {
                for (String chunkId : deletedChunkIds) {
                    if (chunkId.startsWith("MARKER")) {
                        markerChunkDeleted = true;
                        break;
                    }

                    if (callback != null) {
                        callback.run();
                    }
                }
            }
        }

        @Override
        public Iterator<String> resolveChunks(String blobId) throws IOException {
            return Iterators.forArray(blobId + "-1", blobId + "-2");
        }

        private void resetLists() {
            deletedChunkIds.clear();
            failWithDSEForChunkIds.clear();
            failWithExceptionForChunkIds.clear();
        }
    }

    private interface Condition {
        boolean evaluate();
    }

    private boolean waitFor(long timeout, Condition c)
            throws InterruptedException {
        long end = System.currentTimeMillis() + timeout;
        long remaining = end - System.currentTimeMillis();
        while (remaining > 0) {
            if (c.evaluate()) {
                return true;
            }

            Thread.sleep(100);//The constant is exaggerated
            remaining = end - System.currentTimeMillis();
        }
        return c.evaluate();
    }
}
