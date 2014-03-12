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

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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

    public static void assertEquals(byte[] expected, byte[] got) {
        assertEquals(expected.length, got.length);
        for (int i = 0; i < got.length; i++) {
            assertEquals(expected[i], got[i]);
        }
    }

}
