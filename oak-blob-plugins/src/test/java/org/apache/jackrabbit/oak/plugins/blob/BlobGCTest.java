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

package org.apache.jackrabbit.oak.plugins.blob;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Generic class for BlobGC tests which uses custom MemoryNodeStore as well as a memory NodeStore.
 */
public class BlobGCTest {
    protected static final Logger log = LoggerFactory.getLogger(BlobGCTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected GarbageCollectableBlobStore blobStore;
    protected NodeStore nodeStore;
    protected Whiteboard wb;
    protected long startReferenceTime;

    protected BlobReferenceRetriever referenceRetriever;
    protected CheckpointMBean checkpointMBean;

    protected Clock clock;

    @Before
    public void before() {
        clock = getClock();
        blobStore = new TimeLapsedBlobStore();
        nodeStore = new MemoryBlobStoreNodeStore(blobStore);
        // add whiteboard
        final AtomicReference<Map<?, ?>> props = new AtomicReference<Map<?, ?>>();
        wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                props.set(properties);
                return super.register(type, service, properties);
            }
        };
        referenceRetriever = ((MemoryBlobStoreNodeStore) nodeStore).getBlobReferenceRetriever();
        startReferenceTime = ((TimeLapsedBlobStore) blobStore).startTime;
    }

    protected Clock getClock() {
        return new Clock.Virtual();
    }

    @Test
    public void gc() throws Exception {
        log.info("Staring gc()");

        BlobStoreState state = setUp(10, 5, 100);

        log.info("{} blobs added : {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} blobs remaining : {}", state.blobsPresent.size(), state.blobsPresent);

        Set<String> existingAfterGC = gcInternal(0);
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
    }

    @Test
    public void noGc() throws Exception {
        log.info("Staring noGc()");
        startReferenceTime = clock.getTime();

        BlobStoreState state = setUp(10, 5, 100);
        long afterSetupTime = clock.getTime();

        log.info("{} blobs added : {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} blobs remaining : {}", state.blobsPresent.size(), state.blobsPresent);

        Set<String> existingAfterGC = gcInternal(afterSetupTime - startReferenceTime + 2);
        assertTrue(Sets.symmetricDifference(state.blobsAdded, existingAfterGC).isEmpty());
    }

    @Test
    public void gcCheckDeletedSize() throws Exception {
        log.info("Staring gcCheckDeletedSize()");

        BlobStoreState state = setUp(10, 5, 100);

        log.info("{} blobs added : {}", state.blobsAdded.size(), state.blobsAdded);
        log.info("{} blobs remaining : {}", state.blobsPresent.size(), state.blobsPresent);

        // Capture logs for the second round of gc
        LogCustomizer customLogs = LogCustomizer
            .forLogger(MarkSweepGarbageCollector.class.getName())
            .enable(Level.INFO)
            .filter(Level.INFO)
            .contains("Estimated size recovered for")
            .create();
        customLogs.starting();

        Set<String> existingAfterGC = gcInternal(0);
        assertEquals(1, customLogs.getLogs().size());
        long deletedSize = (state.blobsAdded.size() - state.blobsPresent.size()) * 100;
        assertTrue(customLogs.getLogs().get(0).contains(String.valueOf(deletedSize)));

        customLogs.finished();
        assertTrue(Sets.symmetricDifference(state.blobsPresent, existingAfterGC).isEmpty());
    }

    protected Set<String> gcInternal(long maxBlobGcInSecs) throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        MarkSweepGarbageCollector gc = initGC(maxBlobGcInSecs, executor);
        gc.collectGarbage(false);

        assertEquals(0, executor.getTaskCount());
        Set<String> existingAfterGC = iterate();
        log.info("{} blobs existing after gc : {}", existingAfterGC.size(), existingAfterGC);
        return existingAfterGC;
    }

    private MarkSweepGarbageCollector initGC(long blobGcMaxAgeInSecs, ThreadPoolExecutor executor)
        throws Exception {
        return initGC(blobGcMaxAgeInSecs, executor, folder.newFolder().getAbsolutePath());
    }

    private MarkSweepGarbageCollector initGC(long blobGcMaxAgeInSecs, ThreadPoolExecutor executor,
        String root) throws Exception {
        String repoId = null;
        if (SharedDataStoreUtils.isShared(blobStore)) {
            repoId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
            ((SharedDataStore) blobStore).addMetadataRecord(
                new ByteArrayInputStream(new byte[0]),
                REPOSITORY.getNameFromId(repoId));
        }

        MarkSweepGarbageCollector gc =
            new MarkSweepGarbageCollector(referenceRetriever,
                blobStore, executor,
                root, 2048, blobGcMaxAgeInSecs, repoId, wb);
        return gc;
    }

    protected Set<String> iterate() throws Exception {
        Iterator<String> cur = blobStore.getAllChunkIds(0);

        Set<String> existing = Sets.newHashSet();
        while (cur.hasNext()) {
            existing.add(cur.next());
        }
        return existing;
    }

    public BlobStoreState setUp(
        int count,
        int deletions,
        int blobSize) throws Exception {

        preSetup();

        NodeBuilder a = nodeStore.getRoot().builder();
        /* Create and delete nodes with blobs stored in DS*/
        int maxDeleted  = deletions;
        int numBlobs = count;
        List<Integer> toBeDeleted = Lists.newArrayList();
        Random rand = new Random();
        for (int i = 0; i < maxDeleted; i++) {
            int n = rand.nextInt(numBlobs);
            if (!toBeDeleted.contains(n)) {
                toBeDeleted.add(n);
            }
        }

        BlobStoreState state = new BlobStoreState();
        for (int i = 0; i < numBlobs; i++) {
            Blob b = nodeStore.createBlob(randomStream(i, blobSize));
            Iterator<String> idIter = blobStore.resolveChunks(b.getContentIdentity());
            while (idIter.hasNext()) {
                String chunk = idIter.next();
                state.blobsAdded.add(chunk);
                if (!toBeDeleted.contains(i)) {
                    state.blobsPresent.add(chunk);
                }
            }
            a.child("c" + i).setProperty("x", b);
        }

        nodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        log.info("Created blobs : {}", state.blobsAdded.size());

        for (int id : toBeDeleted) {
            delete("c" + id, nodeStore);
        }
        log.info("Deleted nodes : {}", toBeDeleted.size());

        // Sleep a little to make eligible for cleanup
        clock.waitUntil(5);

        postSetup(state);

        return state;
    }

    protected Set<String> createBlobs(int count, int size) throws Exception {
        HashSet<String> blobSet = new HashSet<String>();
        for  (int i = 0; i < count; i++) {
            String id = blobStore.writeBlob(randomStream(100 + i, size));
            Iterator<String> idIter = blobStore.resolveChunks(id);
            while (idIter.hasNext()) {
                String chunk = idIter.next();
                blobSet.add(chunk);
            }
        }
        log.info("{} Additional created {}", blobSet.size(), blobSet);
        return blobSet;
    }

    protected void preSetup() {}

    protected void postSetup(BlobStoreState state) {
        ((MemoryBlobStoreNodeStore) nodeStore).setReferencedBlobs(state.blobsPresent);
    }

    protected void delete(String nodeId, NodeStore nodeStore) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child(nodeId).remove();

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    /**
     * Represents state of the blobs after setup
     */
    class BlobStoreState {
        Set<String> blobsAdded = Sets.newHashSet();
        Set<String> blobsPresent = Sets.newHashSet();
    }

    /**
     * MemoryNodeStore extension which created blobs in the in-memory blob store
     */
    static class MemoryBlobStoreNodeStore extends MemoryNodeStore {
        private final BlobStore blobStore;
        Set<String> referencedBlobs;

        public MemoryBlobStoreNodeStore(BlobStore blobStore) {
            this.blobStore = blobStore;
        }

        public void setReferencedBlobs(Set<String> referencedBlobs) {
            this.referencedBlobs = referencedBlobs;
        }

        @Override
        public ArrayBasedBlob createBlob(InputStream in) {
            try {
                String id = blobStore.writeBlob(in);
                return new TestBlob(id, blobStore);
            } catch(Exception e) {
                log.error("Error in createBlobs", e);
            }
            return null;
        }

        public BlobReferenceRetriever getBlobReferenceRetriever() {
            return collector -> {
                for(String id : referencedBlobs) {
                    collector.addReference(id, null);
                }
            };
        }

        static class TestBlob extends ArrayBasedBlob {
            private String id;
            private BlobStore blobStore;

            public TestBlob(String id, BlobStore blobStore) {
                super(new byte[0]);
                this.id = id;
                this.blobStore = blobStore;
            }

            @Override
            public String getContentIdentity() {
                return id;
            }
            @Nonnull
            @Override
            public InputStream getNewStream() {
                try {
                    return blobStore.getInputStream(id);
                } catch (IOException e) {
                    log.error("Error in getNewStream", e);
                }
                return null;
            }

            @Override
            public long length() {
                try {
                    return blobStore.getBlobLength(id);
                } catch (IOException e) {
                    log.error("Error in length", e);
                }
                return 0;
            }
        }
    }

    /**
     * Test in memory DS to store the contents with an increasing time
     */
    class TimeLapsedBlobStore implements GarbageCollectableBlobStore, SharedDataStore {
        private final long startTime;
        Map<String, DataRecord> store;
        Map<String, DataRecord> metadata;

        public TimeLapsedBlobStore() {
            this(System.currentTimeMillis());
        }

        public TimeLapsedBlobStore(long startTime) {
            this.startTime = clock.getTime();
            store = Maps.newHashMap();
            metadata = Maps.newHashMap();
        }

        @Override public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
            return store.keySet().iterator();
        }

        @Override public boolean deleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
            return (chunkIds.size() == countDeleteChunks(chunkIds, maxLastModifiedTime));
        }

        @Override public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
            int count = 0;

            for(String id : chunkIds) {
                log.info("maxLastModifiedTime {}", maxLastModifiedTime);
                log.info("store.get(id).getLastModified() {}", store.get(id).getLastModified());
                if (maxLastModifiedTime <= 0 || store.get(id).getLastModified() < maxLastModifiedTime) {
                    store.remove(id);
                    count++;
                }
            }
            return count;
        }

        @Override public Iterator<String> resolveChunks(String blobId) throws IOException {
            return Iterators.singletonIterator(blobId);
        }

        @Override public String writeBlob(InputStream in) throws IOException {
            return writeBlob(in, new BlobOptions());
        }

        @Override public String writeBlob(InputStream in, BlobOptions options) throws IOException {
            try {
                byte[] data = IOUtils.toByteArray(in);
                String id = getIdForInputStream(new ByteArrayInputStream(data));
                id += "#" + data.length;
                TestRecord rec = new TestRecord(id, data, clock.getTime());
                store.put(id, rec);
                log.info("Blob created {} with timestamp {}", rec.id, rec.lastModified);
                return id;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        private String getIdForInputStream(final InputStream in)
            throws Exception {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            OutputStream output = new DigestOutputStream(new NullOutputStream(), digest);
            try {
                IOUtils.copyLarge(in, output);
            } finally {
                IOUtils.closeQuietly(output);
                IOUtils.closeQuietly(in);
            }
            return encodeHexString(digest.digest());
        }

        @Override public long getBlobLength(String blobId) throws IOException {
            return ((TestRecord) store.get(blobId)).data.length;
        }

        @Override public InputStream getInputStream(String blobId) throws IOException {
            try {
                return store.get(blobId).getStream();
            } catch (DataStoreException e) {
                e.printStackTrace();
            }
            return null;
        }

        @CheckForNull @Override public String getBlobId(@Nonnull String reference) {
            return reference;
        }

        @CheckForNull @Override public String getReference(@Nonnull String blobId) {
            return blobId;
        }

        @Override public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
            try {
                byte[] data = IOUtils.toByteArray(stream);
                TestRecord rec = new TestRecord(name, data, clock.getTime());
                metadata.put(name, rec);
                log.info("Metadata created {} with timestamp {}", rec.id, rec.lastModified);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override public void addMetadataRecord(File f, String name) throws DataStoreException {
            FileInputStream fstream = null;
            try {
                fstream = new FileInputStream(f);
                addMetadataRecord(fstream, name);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeQuietly(fstream);
            }
        }

        @Override public DataRecord getMetadataRecord(String name) {
            return metadata.get(name);
        }

        @Override public List<DataRecord> getAllMetadataRecords(String prefix) {
            List<DataRecord> recs = Lists.newArrayList();
            Iterator<Map.Entry<String, DataRecord>> iter = metadata.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, DataRecord> entry = iter.next();
                if (entry.getKey().startsWith(prefix)) {
                    recs.add(entry.getValue());
                }
            }
            return recs;
        }

        @Override public boolean deleteMetadataRecord(String name) {
            metadata.remove(name);
            if (!metadata.containsKey(name)) {
                return true;
            }
            return false;
        }

        @Override public void deleteAllMetadataRecords(String prefix) {
            List<String> recs = Lists.newArrayList();
            Iterator<Map.Entry<String, DataRecord>> iter = metadata.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, DataRecord> entry = iter.next();
                if (entry.getKey().startsWith(prefix)) {
                    recs.add(entry.getKey());
                }
            }

            for(String key: recs) {
                metadata.remove(key);
            }
        }

        @Override public Iterator<DataRecord> getAllRecords() throws DataStoreException {
            return store.values().iterator();
        }

        @Override public DataRecord getRecordForId(DataIdentifier id) throws DataStoreException {
            return store.get(id.toString());
        }

        @Override public Type getType() {
            return Type.SHARED;
        }

        class TestRecord implements DataRecord {
            String id;
            byte[] data;
            long lastModified;

            public TestRecord(String id, byte[] data, long lastModified) {
                this.id = id;
                this.data = data;
                this.lastModified = lastModified;
            }

            @Override public DataIdentifier getIdentifier() {
                return new DataIdentifier(id);
            }

            @Override public String getReference() {
                return id;
            }

            @Override public long getLength() throws DataStoreException {
                return data.length;
            }

            @Override public InputStream getStream() throws DataStoreException {
                return new ByteArrayInputStream(data);
            }

            @Override public long getLastModified() {
                return lastModified;
            }
        }

        /** No-op **/
        @Override public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
            throw new UnsupportedOperationException("readBlob not supported");
        }

        @Override public void setBlockSize(int x) {
        }

        @Override public String writeBlob(String tempFileName) throws IOException {
            throw new UnsupportedOperationException("getBlockSizeMin not supported");
        }

        @Override public int sweep() throws IOException {
            throw new UnsupportedOperationException("sweep not supported");
        }

        @Override public void startMark() throws IOException {
        }

        @Override public void clearInUse() {
        }

        @Override public void clearCache() {
        }

        @Override public long getBlockSizeMin() {
            throw new UnsupportedOperationException("getBlockSizeMin not supported");
        }
    }
}
