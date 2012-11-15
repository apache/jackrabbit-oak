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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.mongomk.AbstractMongoConnectionTest;
import org.apache.jackrabbit.mongomk.impl.blob.MongoBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the {@link MongoBlobStore} implementation. It should really extend from
 * AbstractBlobStore but it cannot due to classpath issues, so instead AbstractBlobStore
 * tests are copied here as well.
 */
public class MongoBlobStoreTest extends AbstractMongoConnectionTest {

    protected AbstractBlobStore store;

    @Before
    public void setUp() throws Exception {
        MongoBlobStore blobStore = new MongoBlobStore(mongoConnection.getDB());
        blobStore.setBlockSize(128);
        blobStore.setBlockSizeMin(48);
        this.store = blobStore;
    }

    @After
    public void tearDown() throws Exception {
        store = null;
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

    @Test
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

    @Test
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

    private void doTestRead(byte[] expected, int expectedLen, String id) throws Exception {
        byte[] got = readFully(id);
        assertEquals(expectedLen, got.length);
        assertEquals(expected.length, got.length);
        for (int i = 0; i < got.length; i++) {
            assertEquals(expected[i], got[i]);
        }
    }

    private byte[] readFully(String id) throws Exception {
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

}