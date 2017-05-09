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
package org.apache.jackrabbit.oak.spi.blob;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests a BlobStore implementation.
 */
public abstract class AbstractBlobStoreTest {

    protected GarbageCollectableBlobStore store;

    /**
     * Should be overridden by subclasses to set the {@link #store} variable.
     */
    @Before
    public abstract void setUp() throws Exception;

    @After
    public void tearDown() throws Exception {
        store = null;
    }

    protected int getArtifactSize() {
        return 2080;
    }

    @Test
    public void testWriteFile() throws Exception {
        store.setBlockSize(1024 * 1024);
        byte[] data = new byte[4 * 1024 * 1024];
        Random r = new Random(0);
        r.nextBytes(data);
        String tempFileName = "target/temp/test";
        File tempFile = new File(tempFileName);
        tempFile.getParentFile().mkdirs();
        OutputStream out = new FileOutputStream(tempFile, false);
        out.write(data);
        out.close();
        String s = store.writeBlob(tempFileName);
        assertEquals(data.length, store.getBlobLength(s));
        byte[] buff = new byte[1];
        for (int i = 0; i < data.length; i += 1024) {
            store.readBlob(s, i, buff, 0, 1);
            assertEquals(data[i], buff[0]);
        }
        try {
            store.writeBlob(tempFileName + "_wrong");
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testCombinedIdentifier() throws Exception {
        String id = store.writeBlob(new ByteArrayInputStream(new byte[2]));
        assertEquals(2, store.getBlobLength(id));
        String combinedId = id + id;
        assertEquals(4, store.getBlobLength(combinedId));
        doTestRead(new byte[4], 4, combinedId);
    }

    @Test
    public void testEmptyIdentifier() throws Exception {
        byte[] data = new byte[1];
        assertEquals(-1, store.readBlob("", 0, data, 0, 1));
        assertEquals(0, store.getBlobLength(""));
    }

    @Test
    public void testCloseStream() throws Exception {
        final AtomicBoolean closed = new AtomicBoolean();
        InputStream in = new InputStream() {
            @Override
            public void close() {
                closed.set(true);
            }
            @Override
            public int read() throws IOException {
                return -1;
            }
        };
        store.writeBlob(in);
        assertTrue(closed.get());
    }

    @Test
    public void testExceptionWhileReading() throws Exception {
        final AtomicBoolean closed = new AtomicBoolean();
        InputStream in = new InputStream() {
            @Override
            public void close() {
                closed.set(true);
            }
            @Override
            public int read() throws IOException {
                throw new RuntimeException("abc");
            }
        };
        try {
            store.writeBlob(in);
        } catch (Exception e) {
            String msg = e.getMessage();
            assertTrue(msg, msg.indexOf("abc") >= 0);
        }
        assertTrue(closed.get());
    }

    @Test
    public void testIllegalIdentifier() throws Exception {
        byte[] data = new byte[1];
        try {
            store.readBlob("ff", 0, data, 0, 1);
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testIllegalIdentifier2() throws Exception {
        try {
            store.getBlobLength("ff");
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testIllegalIdentifier3() throws Exception {
        if (store instanceof AbstractBlobStore) {
            try {
                ((AbstractBlobStore) store).mark("ff");
                fail();
            } catch (Exception e) {
                // expected
            }
        }
    }

    @Test
    public void testSmall() throws Exception {
        doTest(10, 300);
    }

    @Test
    public void testMedium() throws Exception {
        doTest(100, 100);
    }

    @Test
    public void testLarge() throws Exception {
        doTest(1000, 10);
    }

    @Test
    public void testGarbageCollection() throws Exception {
        HashMap<String, byte[]> map = new HashMap<String, byte[]>();
        ArrayList<String> mem = new ArrayList<String>();
        int count;
        for (int i = 1; i <= 1000; i *= 10) {
            byte[] data = new byte[i];
            String id;
            id = store.writeBlob(new ByteArrayInputStream(data));
            // copy the id so the string is not in the weak hash map
            map.put(new String(id), data);
            mem.add(id);
            data = new byte[i];
            Arrays.fill(data, (byte) 1);
            id = store.writeBlob(new ByteArrayInputStream(data));
            // copy the id so the string is not in the weak hash map
            map.put(new String(id), data);
            mem.add(id);
        }
        store.startMark();
        store.sweep();
        for (String id : map.keySet()) {
            byte[] test = readFully(id);
            assertTrue(Arrays.equals(map.get(id), test));
        }

        mem.clear();

        store.clearInUse();
        store.startMark();
        for (String id : map.keySet()) {
            byte[] d = map.get(id);
            if (d[0] != 0) {
                continue;
            }
            if (store instanceof AbstractBlobStore) {
                ((AbstractBlobStore) store).mark(id);
            } else {
                // this should mark the id
                store.getBlobLength(id);
            }
        }
        count = store.sweep();

        store.clearInUse();
        store.clearCache();

        // https://issues.apache.org/jira/browse/OAK-60
        // endure there is at least one old entry (with age 1 ms)
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            // ignore
        }

        store.startMark();
        count = store.sweep();
        assertTrue("count: " + count, count > 0);
        int failedCount = 0;
        for (String id : map.keySet()) {
            long length = store.getBlobLength(id);
            try {
                readFully(id);
            } catch (Exception e) {
                assertTrue(id + ":" + length, length > store.getBlockSizeMin());
                failedCount++;
            }
        }
        assertTrue("failedCount: " + failedCount, failedCount > 0);
    }

    @Test
    public void testReference() throws Exception {
        assumeThat(store, instanceOf(AbstractBlobStore.class));
        AbstractBlobStore abs = (AbstractBlobStore) store;
        Random r = new Random();
        byte[] key = new byte[256];
        r.nextBytes(key);
        abs.setReferenceKey(key);

        byte[] data = new byte[1000];
        r.nextBytes(data);
        String blobId = store.writeBlob(new ByteArrayInputStream(data));
        String reference = store.getReference(blobId);
        String blobId2 = store.getBlobId(reference);
        assertEquals(blobId, blobId2);
    }

    private void doTest(int maxLength, int count) throws Exception {
        String[] s = new String[count * 2];
        Random r = new Random(0);
        for (int i = 0; i < s.length;) {
            byte[] data = new byte[r.nextInt(maxLength)];
            r.nextBytes(data);
            s[i++] = store.writeBlob(new ByteArrayInputStream(data));
            s[i++] = store.writeBlob(new ByteArrayInputStream(data));
        }
        r.setSeed(0);
        for (int i = 0; i < s.length;) {
            int expectedLen = r.nextInt(maxLength);
            byte[] expectedData = new byte[expectedLen];
            r.nextBytes(expectedData);
            assertEquals(expectedLen, store.getBlobLength(s[i++]));

            String id = s[i++];
            doTestRead(expectedData, expectedLen, id);
        }
    }

    private void doTestRead(byte[] expectedData, int expectedLen, String id) throws Exception {
        byte[] got = readFully(id);
        assertEquals(expectedLen, got.length);
        assertEquals(expectedData.length, got.length);
        for (int i = 0; i < got.length; i++) {
            assertEquals(expectedData[i], got[i]);
        }
    }

    public byte[] readFully(String id) throws Exception {
        int len = (int) store.getBlobLength(id);
        byte[] data;
        if (len < 100) {
            data = new byte[len];
            for (int i = 0; i < len; i++) {
                store.readBlob(id, i, data, i, 1);
            }
        } else {
            data = BlobStoreInputStream.readFully(store, id);
        }
        assertEquals(len, data.length);
        return data;
    }

    public static void main(String... args) throws Exception {
        FileBlobStore store = new FileBlobStore("target/temp");
        String id = addFiles(store, "~/temp/ds");
        extractFiles(store, id, "target/test");
    }

    public static void extractFiles(BlobStore store, String listingId, String target) throws IOException {
        String listing = new String(BlobStoreInputStream.readFully(store, listingId), "UTF-8");
        JsopTokenizer t = new JsopTokenizer(listing);
        File targetDir = new File(target);
        targetDir.mkdirs();
        t.read('{');
        if (!t.matches('}')) {
            do {
                String file = t.readString();
                t.read(':');
                String id = t.readString();
                byte[] data = BlobStoreInputStream.readFully(store, id);
                File outFile = new File(targetDir, file);
                outFile.getParentFile().mkdirs();
                FileOutputStream out = new FileOutputStream(outFile);
                try {
                    out.write(data);
                } finally {
                    out.close();
                }
            } while (t.matches(','));
        }
        t.read('}');
    }

    public static String addFiles(BlobStore store, String dir) throws Exception {
        ArrayList<String> list = new ArrayList<String>();
        String root = new File(dir).getAbsolutePath();
        String parent = new File(dir).getParentFile().getAbsolutePath();
        addFiles(list, new File(root));
        JsopBuilder listing = new JsopBuilder();
        listing.object();
        for (String f : list) {
            FileInputStream in = new FileInputStream(f);
            String id = store.writeBlob(in);
            in.close();
            String name = f.substring(parent.length());
            listing.key(name).value(id);
            listing.newline();
        }
        listing.endObject();
        String l = listing.toString();
        String id = store.writeBlob(new ByteArrayInputStream(l.getBytes("UTF-8")));
        return id;
    }

    private static void addFiles(ArrayList<String> list, File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                addFiles(list, f);
            }
            return;
        }
        if (!file.isFile()) {
            return;
        }
        list.add(file.getAbsolutePath());
    }

    @Test
    public void list() throws Exception {
        Set<String> ids = createArtifacts();

        Iterator<String> iter = store.getAllChunkIds(0);
        while (iter.hasNext()) {
            ids.remove(iter.next());
        }

        assertTrue("unexpected ids in store: " + ids, ids.isEmpty());
    }

    @Test
    public void delete() throws Exception {
        Set<String> ids = createArtifacts();

        store.deleteChunks(Lists.newArrayList(ids), 0);

        Iterator<String> iter = store.getAllChunkIds(0);
        Set<String> ret = Sets.newHashSet();
        while (iter.hasNext()) {
            ret.add(iter.next());
        }

        assertTrue(ret.toString(), ret.isEmpty());
    }
    
    @Test
    public void deleteCount() throws Exception {
        Set<String> ids = createArtifacts();

        long count = store.countDeleteChunks(Lists.newArrayList(ids), 0);

        Iterator<String> iter = store.getAllChunkIds(0);
        Set<String> ret = Sets.newHashSet();
        while (iter.hasNext()) {
            ret.add(iter.next());
        }

        assertTrue(ret.toString(), ret.isEmpty());
        assertEquals(ids.size(), count);
    }

    @Test
    public void uploadCallback() throws Exception {
        assumeTrue(supportsStatsCollection());
        TestCollector collector = new TestCollector();
        setupCollector(collector);
        int size = 10 * 1024;
        store.writeBlob(randomStream(42, size));
        //For chunked storage the actual stored size is greater than the file size
        assertCollectedSize(collector.size, size);
        assertEquals(1, collector.uploadCount);
    }

    @Test
    public void downloadCallback() throws Exception {
        assumeTrue(supportsStatsCollection());
        TestCollector collector = new TestCollector();
        setupCollector(collector);
        int size = 10 * 1024;
        String id = store.writeBlob(randomStream(42, size));

        store.clearCache();
        collector.reset();

        InputStream is = store.getInputStream(id);
        CountingOutputStream cos = new CountingOutputStream(new NullOutputStream());
        IOUtils.copy(is, cos);
        is.close();

        assertEquals(size, cos.getCount());

        //For chunked storage the actual stored size is greater than the file size
        assertCollectedSize(collector.size, size);
        assertEquals(1, collector.downloadCount);
    }

    protected void setupCollector(BlobStatsCollector statsCollector) {
        if (store instanceof AbstractBlobStore){
            ((AbstractBlobStore) store).setStatsCollector(statsCollector);
        }
    }

    protected boolean supportsStatsCollection(){
        return false;
    }

    private Set<String> createArtifacts() throws Exception {
        Set<String> ids = Sets.newHashSet();
        int number = 10;
        for (int i = 0; i < number; i++) {
            String id = store.writeBlob(randomStream(i, getArtifactSize()));
            Iterator<String> iter = store.resolveChunks(id.toString());
            while (iter.hasNext()) {
                ids.add(iter.next());
            }
        }
        return ids;
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private static void assertCollectedSize(long collectedSize, long expectedSize){
        if (collectedSize < expectedSize) {
            fail(String.format("Collected size %d is less that expected size %d", collectedSize, expectedSize));
        }
    }

    private static class TestCollector implements BlobStatsCollector {
        long size;
        int uploadCount;
        int downloadCount;

        @Override
        public void uploaded(long timeTaken, TimeUnit unit, long size) {
            this.size += size;
        }

        @Override
        public void downloaded(String blobId, long timeTaken, TimeUnit unit, long size) {
            this.size += size;
        }

        @Override
        public void uploadCompleted(String blobId) {
            uploadCount++;
        }

        @Override
        public void downloadCompleted(String blobId) {
            downloadCount++;
        }

        void reset(){
            size = 0;
            downloadCount = 0;
            uploadCount = 0;
        }
    }
}
