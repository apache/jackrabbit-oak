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
package org.apache.jackrabbit.mk.blobs;

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
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.util.IOUtilsTest;

/**
 * Tests a BlobStore implementation.
 */
public abstract class AbstractBlobStoreTest extends TestCase {

    protected AbstractBlobStore store;

    /**
     * Should be overridden by subclasses to set the {@link #store} variable.
     */
    public abstract void setUp() throws Exception;

    public void tearDown() throws Exception {
        store = null;
    }

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

    public void testCombinedIdentifier() throws Exception {
        String id = store.writeBlob(new ByteArrayInputStream(new byte[2]));
        assertEquals(2, store.getBlobLength(id));
        String combinedId = id + id;
        assertEquals(4, store.getBlobLength(combinedId));
        doTestRead(new byte[4], 4, combinedId);
    }

    public void testEmptyIdentifier() throws Exception {
        byte[] data = new byte[1];
        assertEquals(-1, store.readBlob("", 0, data, 0, 1));
        assertEquals(0, store.getBlobLength(""));
    }

    public void testCloseStream() throws Exception {
        final AtomicBoolean closed = new AtomicBoolean();
        InputStream in = new InputStream() {
            public void close() {
                closed.set(true);
            }
            public int read() throws IOException {
                return -1;
            }
        };
        store.writeBlob(in);
        assertTrue(closed.get());
    }

    public void testExceptionWhileReading() throws Exception {
        final AtomicBoolean closed = new AtomicBoolean();
        InputStream in = new InputStream() {
            public void close() {
                closed.set(true);
            }
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


    public void testIllegalIdentifier() throws Exception {
        byte[] data = new byte[1];
        try {
            store.readBlob("ff", 0, data, 0, 1);
            fail();
        } catch (Exception e) {
            // expected
        }
        try {
            store.getBlobLength("ff");
            fail();
        } catch (Exception e) {
            // expected
        }
        try {
            store.mark("ff");
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    public void testSmall() throws Exception {
        doTest(10, 300);
    }

    public void testMedium() throws Exception {
        doTest(100, 100);
    }

    public void testLarge() throws Exception {
        doTest(1000, 10);
    }

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
            store.mark(id);
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
        IOUtilsTest.assertEquals(expectedData, got);
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

//        DbBlobStore store = new DbBlobStore();
//        store.setConnectionPool(JdbcConnectionPool.create("jdbc:h2:target/test;log=0;undo_log=0", "", ""));

        String id = addFiles(store, "~/temp/ds");
        extractFiles(store, id, "target/test");

    }

    public static void extractFiles(AbstractBlobStore store, String listingId, String target) throws IOException {
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

    public static String addFiles(AbstractBlobStore store, String dir) throws Exception {
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

    // FIXME: OAK-430: Incorrect read logic in AbstractBlobStore
//    public void testRead() throws Exception {
//        int blobLength = 81;
//        store.setBlockSize(80);
//        store.setBlockSizeMin(48);
//
//        byte[] blob = createBlob(blobLength);
//        String blobId = store.writeBlob(new ByteArrayInputStream(blob));
//        assertNotNull(blobId);
//
//        byte[] readBlob = new byte[blobLength];
//        store.readBlob(blobId, 0, readBlob, 0, readBlob.length);
//        assertTrue(Arrays.equals(blob, readBlob));
//    }

    private static byte[] createBlob(int blobLength) {
        byte[] blob = new byte[blobLength];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte)i;
        }
        return blob;
    }


}
