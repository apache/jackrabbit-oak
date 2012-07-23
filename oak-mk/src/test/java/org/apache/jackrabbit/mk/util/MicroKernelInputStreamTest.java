/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.mk.util;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@code MicroKernelInputStream}.
 */
public class MicroKernelInputStreamTest {

    MicroKernel mk = new MicroKernelImpl();

    @Test
    public void small() throws IOException {
        doTest(10, 10);
    }

    @Test
    public void medium() throws IOException {
        doTest(1000, 10);
    }

    @Test
    public void large() throws IOException {
        doTest(100000, 1);
    }

    private void doTest(int maxLength, int count) throws IOException {
        String[] s = new String[count * 2];
        Random r = new Random(0);
        for (int i = 0; i < s.length;) {
            int len = count == 1 ? maxLength : r.nextInt(maxLength);
            byte[] data = new byte[len];
            r.nextBytes(data);
            s[i++] = mk.write(new ByteArrayInputStream(data));
            s[i++] = mk.write(new ByteArrayInputStream(data));
        }
        r.setSeed(0);
        for (int i = 0; i < s.length;) {
            int len = count == 1 ? maxLength : r.nextInt(maxLength);
            byte[] expectedData = new byte[len];
            r.nextBytes(expectedData);
            assertEquals(len, mk.getLength(s[i++]));

            String id = s[i++];
            doTestReadFully(expectedData, len, id);
            doTestRead(expectedData, len, id);
        }
    }

    private void doTestReadFully(byte[] expectedData, int expectedLen, String id)
            throws IOException {
        byte[] got = MicroKernelInputStream.readFully(mk, id);
        assertByteArrayEquals(expectedData, expectedLen, got);
    }

    private static void assertByteArrayEquals(byte[] expected, int expectedLen, byte[] got) {
        assertEquals(expectedLen, got.length);
        for (int j = 0; j < expectedLen; j++) {
            if (expected[j] != got[j]) {
                assertEquals("j:" + j, expected[j], got[j]);
            }
        }
    }

    private void doTestRead(byte[] expectedData, int expectedLen, String id) throws IOException {
        InputStream in = new MicroKernelInputStream(mk, id);
        Random r = new Random(1);
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        int minLen = 0;
        if (expectedLen > 1000000) {
            minLen = 4000;
        }
        int pos = 0;
        while (true) {
            int op = r.nextInt(5);
            if (op == 0) {
                // read one byte
                int x = in.read();
                if (x < 0) {
                    break;
                }
                buff.write(x);
                pos++;
            } else if (op == 1) {
                // skip a large number of bytes
                long n = minLen + r.nextInt(5000);
                long skipped = in.skip(n);
                assertTrue(skipped >= 0);
                buff.write(expectedData, pos, (int) skipped);
                pos += skipped;
            } else if (op == 2) {
                // skip a small number of bytes (possibly negative)
                long n = r.nextInt(10) - 3;
                long skipped = in.skip(n);
                assertTrue(skipped >= 0);
                buff.write(expectedData, pos, (int) skipped);
                pos += skipped;
            } else if (op == 3) {
                // read a large number of bytes
                byte[] x = new byte[minLen + r.nextInt(5000)];
                int l = in.read(x);
                if (l < 0) {
                    break;
                }
                buff.write(x, 0, l);
                pos += l;
            } else {
                // read a small number of bytes
                int offset = r.nextInt(10);
                int len = minLen + r.nextInt(1000);
                byte[] x = new byte[offset + len];
                int l = in.read(x, offset, len);
                if (l < 0) {
                    break;
                }
                buff.write(x, offset, l);
                pos += l;
            }
        }
        byte[] got = buff.toByteArray();
        assertByteArrayEquals(expectedData, expectedLen, got);
    }

}
