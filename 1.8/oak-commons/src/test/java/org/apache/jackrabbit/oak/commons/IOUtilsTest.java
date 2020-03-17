/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import junit.framework.TestCase;

/**
 * Test the utility classes.
 */
public class IOUtilsTest extends TestCase {

    public void testReadFully() throws IOException {
        final Random r = new Random(1);
        byte[] data = new byte[1000];
        final AtomicInteger readCount = new AtomicInteger();
        r.nextBytes(data);
        FilterInputStream in = new FilterInputStream(new ByteArrayInputStream(data)) {
            @Override
            public int read(byte[] buffer, int off, int max) throws IOException {
                readCount.incrementAndGet();
                if (r.nextInt(10) == 0) {
                    return 0;
                }
                return in.read(buffer, off, Math.min(10, max));
            }
        };
        in.mark(10000);
        byte[] test = new byte[1000];

        // readFully is not supposed to call read when reading 0 bytes
        assertEquals(0, IOUtils.readFully(in, test, 0, 0));
        assertEquals(0, readCount.get());

        assertEquals(1000, IOUtils.readFully(in, test, 0, 1000));
        IOUtilsTest.assertEquals(data, test);
        test = new byte[1001];
        in.reset();
        in.mark(10000);
        assertEquals(1000, IOUtils.readFully(in, test, 0, 1001));
        assertEquals(0, IOUtils.readFully(in, test, 0, 0));
    }

    public void testSkipFully() throws IOException {
        final Random r = new Random(1);
        byte[] data = new byte[1000];
        r.nextBytes(data);
        FilterInputStream in = new FilterInputStream(new ByteArrayInputStream(data)) {
            @Override
            public int read(byte[] buffer, int off, int max) throws IOException {
                return in.read(buffer, off, Math.min(10, max));
            }
        };
        in.mark(10000);
        IOUtils.skipFully(in, 1000);
        assertEquals(-1, in.read());
        in.reset();
        try {
            IOUtils.skipFully(in, 1001);
            fail();
        } catch (EOFException e) {
            // expected
        }
    }

    public void testStringReadWrite() throws IOException {
        final Random r = new Random(1);
        for (int i = 0; i < 100000; i += i / 10 + 1) {
            String s = "";
            for (int j = 0; j < 10; j++) {
                String p = new String(new char[i]).replace((char) 0, 'a');
                s += p;
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.writeString(out, s);
            byte[] data = out.toByteArray();
            ByteArrayInputStream in = new ByteArrayInputStream(data) {
                @Override
                public int read(byte[] b, int off, int len) {
                    if (r.nextBoolean()) {
                        len = r.nextInt(len);
                    }
                    return super.read(b, off, len);
                }
            };
            String t = IOUtils.readString(in);
            assertEquals(s, t);
            assertEquals(-1, in.read());
        }
        try {
            InputStream in = new ByteArrayInputStream(new byte[]{1});
            IOUtils.readString(in);
            fail();
        } catch (EOFException e) {
            // expected
        }
    }

    public void testBytesReadWrite() throws IOException {
        final Random r = new Random();
        int iterations = 1000;
        while (iterations-- > 0) {
            int n = Math.abs(r.nextInt()) % 0x40000;
            byte[] buf = new byte[n];
            r.nextBytes(buf);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.writeBytes(out, buf);
            byte[] buf1 = IOUtils.readBytes(new ByteArrayInputStream(out.toByteArray()));
            assertEquals(buf, buf1);
        }
    }

    public void testVarInt() throws IOException {
        testVarInt(0, 1);
        testVarInt(0x7f, 1);
        testVarInt(0x80, 2);
        testVarInt(0x3fff, 2);
        testVarInt(0x4000, 3);
        testVarInt(0x1fffff, 3);
        testVarInt(0x200000, 4);
        testVarInt(0xfffffff, 4);
        testVarInt(0x10000000, 5);
        testVarInt(-1, 5);
        for (int x = 0; x < 0x20000; x++) {
            testVarInt(x, 0);
            testVarInt(Integer.MIN_VALUE + x, 0);
            testVarInt(Integer.MAX_VALUE - x, 5);
            testVarInt(0x200000 + x - 100, 0);
            testVarInt(0x10000000 + x - 100, 0);
        }
        Random r = new Random(1);
        for (int i = 0; i < 100000; i++) {
            testVarInt(r.nextInt(), 0);
            testVarInt(r.nextInt(10000000), 0);
        }

        // trailing 0s are never written, but are an alternative way to encode a value
        InputStream in = new ByteArrayInputStream(new byte[]{(byte) 0x80, 0});
        assertEquals(0, IOUtils.readVarInt(in));
        assertEquals(-1, in.read());
    }

    public void testVarLong() throws IOException {
        testVarLong(0, 1);
        testVarLong(0x7f, 1);
        testVarLong(0x80, 2);
        testVarLong(0x3fff, 2);
        testVarLong(0x4000, 3);
        testVarLong(0x1fffff, 3);
        testVarLong(0x200000, 4);
        testVarLong(0xfffffff, 4);
        testVarLong(0x10000000, 5);
        testVarLong(0x1fffffffL, 5);
        testVarLong(0x2000000000L, 6);
        testVarLong(0x3ffffffffffL, 6);
        testVarLong(0x40000000000L, 7);
        testVarLong(0x1ffffffffffffL, 7);
        testVarLong(0x2000000000000L, 8);
        testVarLong(0xffffffffffffffL, 8);
        testVarLong(0x100000000000000L, 9);
        testVarLong(-1, 10);
        for (int x = 0; x < 0x20000; x++) {
            testVarLong(x, 0);
            testVarLong(Long.MIN_VALUE + x, 0);
            testVarLong(Long.MAX_VALUE - x, 9);
            testVarLong(0x200000 + x - 100, 0);
            testVarLong(0x10000000 + x - 100, 0);
        }
        Random r = new Random(1);
        for (int i = 0; i < 100000; i++) {
            testVarLong(r.nextLong(), 0);
            testVarLong(r.nextInt(Integer.MAX_VALUE), 0);
        }

        // trailing 0s are never written, but are an alternative way to encode a value
        InputStream in = new ByteArrayInputStream(new byte[]{(byte) 0x80, 0});
        assertEquals(0, IOUtils.readVarLong(in));
        assertEquals(-1, in.read());
    }

    public void testVarEOF() throws IOException {
        try {
            IOUtils.readVarInt(new ByteArrayInputStream(new byte[0]));
            fail();
        } catch (EOFException e) {
            // expected
        }
        try {
            IOUtils.readVarLong(new ByteArrayInputStream(new byte[0]));
            fail();
        } catch (EOFException e) {
            // expected
        }
    }

    public void testLong() throws IOException {
        testLong(Long.MIN_VALUE);
        testLong(Long.MAX_VALUE);
        testLong(0x0L);
        for (long l : Lists.newArrayList(0x01L, 0x08L)) {
            for (long x = l; x != 0; x = (x << 4)) {
                testLong(x);
            }
        }
        long loopMax = (Long.MAX_VALUE >> 4) + 1;
        for (long l : Lists.newArrayList(0x07L, 0x0FL)) {
            for (long x = l; x <= loopMax; x = ((x << 4) | 0x0FL)) {
                testLong(x);
            }
        }
    }

    public void testInt() throws IOException {
        testInt(Integer.MIN_VALUE);
        testInt(Integer.MAX_VALUE);
        testInt(0x0);
        for (int i : Lists.newArrayList(0x01, 0x08)) {
            for (int x = i; x != 0; x = (x << 4)) {
                testInt(x);
            }
        }
        int loopMax = (Integer.MAX_VALUE >> 4) + 1;
        for (int i : Lists.newArrayList(0x07, 0x0F)) {
            for (int x = i; x <= loopMax; x = ((x << 4) | 0x0F)) {
                testInt(x);
            }
        }
    }

    public void testNextPowerOf2() {
        assertEquals(1L, IOUtils.nextPowerOf2(0));
        assertEquals(1L, IOUtils.nextPowerOf2(1));
        assertEquals(2L, IOUtils.nextPowerOf2(2));
        assertEquals(4L, IOUtils.nextPowerOf2(3));
        assertEquals(4L, IOUtils.nextPowerOf2(4));
        assertEquals(8L, IOUtils.nextPowerOf2(5));
        assertEquals(8L, IOUtils.nextPowerOf2(7));
        assertEquals(8L, IOUtils.nextPowerOf2(8));
        assertEquals(16L, IOUtils.nextPowerOf2(12)); // it is powers of 2, not evens
        assertEquals(32L, IOUtils.nextPowerOf2(21)); // random mid-range number

        // Test values around the upper limit of powers of 2 in the signed int range
        assertEquals(0x01L << (Integer.SIZE-2), IOUtils.nextPowerOf2(Integer.MAX_VALUE >> 1));
        assertEquals(0x01L << (Integer.SIZE-2), IOUtils.nextPowerOf2((Integer.MAX_VALUE >> 1) + 1));
        assertEquals( 0x01L << ((long)Integer.SIZE - 1L), IOUtils.nextPowerOf2((Integer.MAX_VALUE >> 1) + 2));
        assertEquals(0x01L << ((long)Integer.SIZE - 1L), IOUtils.nextPowerOf2(Integer.MAX_VALUE));

        // Negative values
        assertEquals(1L, IOUtils.nextPowerOf2(-1));
        assertEquals(1L, IOUtils.nextPowerOf2(-2));
        assertEquals(1L, IOUtils.nextPowerOf2(Integer.MIN_VALUE));
    }

    public void testCopyStream() throws IOException {
        final Random r = new Random(1);
        for (int length : Lists.newArrayList(0, 1, 1000, 4096, 4097, 1024*1024)) {
            byte[] inData = new byte[length];
            r.nextBytes(inData);
            ByteArrayInputStream in = new ByteArrayInputStream(inData);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copy(in, out);
            assertEquals(inData, out.toByteArray());
        }
    }

    public void testHumanReadableByteCount() {
        assertEquals("0 B", IOUtils.humanReadableByteCount(0L));
        assertEquals("1 B", IOUtils.humanReadableByteCount(1L));
        assertEquals("999 B", IOUtils.humanReadableByteCount(999L));
        assertEquals("1.0 kB", IOUtils.humanReadableByteCount(1000L));
        assertEquals("1.0 kB", IOUtils.humanReadableByteCount(1001L));
        assertEquals("1.1 kB", IOUtils.humanReadableByteCount(1100L));
        assertEquals("2.0 kB", IOUtils.humanReadableByteCount(2000L));
        assertEquals("1000.0 kB", IOUtils.humanReadableByteCount(999999L));
        assertEquals("1.0 MB", IOUtils.humanReadableByteCount(1000000L));
        assertEquals("1.0 MB", IOUtils.humanReadableByteCount(1010000L));
        assertEquals("1.1 MB", IOUtils.humanReadableByteCount(1100000L));
        assertEquals("2.0 MB", IOUtils.humanReadableByteCount(2000000L));
        assertEquals("2.1 GB", IOUtils.humanReadableByteCount(Integer.MAX_VALUE));
        assertEquals("54.3 GB", IOUtils.humanReadableByteCount(54320000000L));
        assertEquals("20.6 TB", IOUtils.humanReadableByteCount(20560000000000L));
        assertEquals("377.5 PB", IOUtils.humanReadableByteCount(377500000000000000L));
        assertEquals("1.0 EB", IOUtils.humanReadableByteCount(1000000000000000000L));
        assertEquals("9.2 EB", IOUtils.humanReadableByteCount(Long.MAX_VALUE));
        assertEquals("0 B", IOUtils.humanReadableByteCount(-0L));
        assertEquals("0", IOUtils.humanReadableByteCount(-1L));
        assertEquals("0", IOUtils.humanReadableByteCount(Integer.MIN_VALUE));
        assertEquals("0", IOUtils.humanReadableByteCount(Long.MIN_VALUE));
    }

    private static void testVarInt(int x, int expectedLen) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.writeVarInt(out, x);
        byte[] data = out.toByteArray();
        assertTrue(data.length <= 5);
        if (expectedLen > 0) {
            assertEquals(expectedLen, data.length);
        }
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        int x2 = IOUtils.readVarInt(in);
        assertEquals(x, x2);
        assertEquals(-1, in.read());
    }

    private static void testVarLong(long x, int expectedLen) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.writeVarLong(out, x);
        byte[] data = out.toByteArray();
        assertTrue(data.length <= 10);
        if (expectedLen > 0) {
            assertEquals(expectedLen, data.length);
        }
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        long x2 = IOUtils.readVarLong(in);
        assertEquals(x, x2);
        assertEquals(-1, in.read());
    }

    private static void testLong(long x) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.writeLong(out, x);
        byte[] data = out.toByteArray();
        assertTrue(data.length == Long.BYTES);
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        long x2 = IOUtils.readLong(in);
        assertEquals(x, x2);
        assertEquals(-1, in.read());
    }

    private static void testInt(int x) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.writeInt(out, x);
        byte[] data = out.toByteArray();
        assertTrue(data.length == Integer.BYTES);
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        int x2 = IOUtils.readInt(in);
        assertEquals(x, x2);
        assertEquals(-1, in.read());
    }

    public static void assertEquals(byte[] expected, byte[] got) {
        assertEquals(expected.length, got.length);
        for (int i = 0; i < got.length; i++) {
            assertEquals(expected[i], got[i]);
        }
    }

}
