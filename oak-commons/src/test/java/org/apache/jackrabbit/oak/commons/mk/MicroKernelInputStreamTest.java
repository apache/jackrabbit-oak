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
package org.apache.jackrabbit.oak.commons.mk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.junit.Test;

/**
 * Tests the {@code MicroKernelInputStream}.
 */
public class MicroKernelInputStreamTest {

    MicroKernel mk = new MockMicroKernel();

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

    //------------------------------------------------------------< MockMicroKernel >---

    private static class MockMicroKernel implements MicroKernel {
        private final Map<String, byte[]> streams = Maps.newHashMap();

        @Override
        public String getHeadRevision() throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public String checkpoint(long lifetime) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getRevisionHistory(long since, int maxEntries, String path) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String waitForCommit(String oldHeadRevisionId, long timeout) throws MicroKernelException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getJournal(String fromRevisionId, String toRevisionId, String path) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String diff(String fromRevisionId, String toRevisionId, String path, int depth) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nodeExists(String path, String revisionId) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getChildNodeCount(String path, String revisionId) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getNodes(String path, String revisionId, int depth, long offset, int maxChildNodes, String filter) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String commit(String path, String jsonDiff, String revisionId, String message) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String branch(String trunkRevisionId) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String merge(String branchRevisionId, String message) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public String rebase(@Nonnull String branchRevisionId, String newBaseRevisionId) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public String reset(@Nonnull String branchRevisionId, @Nonnull String ancestorRevisionId) throws MicroKernelException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLength(String blobId) throws MicroKernelException {
            byte[] data = streams.get(blobId);
            if (data == null) {
                throw new MicroKernelException("No such blob:" + blobId);
            } else {
                return data.length;
            }
        }

        @Override
        public int read(String blobId, long pos, byte[] buff, int off, int length) throws MicroKernelException {
            byte[] data = streams.get(blobId);
            if (data == null) {
                throw new MicroKernelException("No such blob:" + blobId);
            } else {
                try {
                    InputStream stream = new ByteArrayInputStream(data);
                    try {
                        ByteStreams.skipFully(stream, pos);
                        return stream.read(buff, off, length);
                    } finally {
                        stream.close();
                    }
                } catch (IOException e) {
                    throw new MicroKernelException("Failed to read a blob", e);
                }
            }
        }

        @Override
        public String write(InputStream in) throws MicroKernelException {
            try {
                byte[] data = ByteStreams.toByteArray(in);
                String id = String.valueOf(Arrays.hashCode(data));
                streams.put(id, data);
                return id;
            } catch (IOException e) {
                throw new MicroKernelException(e);
            } finally {
                IOUtils.closeQuietly(in);
            }
        }
    }

}
