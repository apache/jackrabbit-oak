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

package org.apache.jackrabbit.oak.plugins.value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.jcr.Binary;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.Blob;
import org.junit.Test;

public class BinaryBasedBlobTest {

    @Test
    public void getStream() throws RepositoryException, IOException {
        byte[] bytes = "just a test".getBytes();
        Binary binary = new TestBinary(bytes);
        Blob blob = new BinaryBasedBlob(binary);

        assertEquals(bytes.length, blob.length());

        InputStream expected = binary.getStream();
        InputStream actual = blob.getNewStream();
        try {
            for (int e = expected.read(); e != -1; e = expected.read()) {
                assertEquals(e, actual.read());
            }
            assertEquals(-1, actual.read());

        } finally {
            expected.close();
            actual.close();
        }
    }

    @Test
    public void getStreamWithError() throws IOException {
        Blob blob = new BinaryBasedBlob(new FailingBinary());

        assertEquals(-1, blob.length());
        InputStream ins = blob.getNewStream();
        try {
            ins.read();
            fail();
        } catch (IOException ignored) {
        } finally {
            ins.close();
        }
    }

    private static class TestBinary implements Binary {
        private final byte[] data;

        public TestBinary(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream getStream() throws RepositoryException {
            return new ByteArrayInputStream(data);
        }

        @Override
        public int read(byte[] b, long position) throws IOException, RepositoryException {
            return getStream().read(b, (int) position, b.length);
        }

        @Override
        public long getSize() throws RepositoryException {
            return data.length;
        }

        @Override
        public void dispose() { }
    }

    private static class FailingBinary implements Binary {
        @Override
        public InputStream getStream() throws RepositoryException {
            throw new RepositoryException("no stream");
        }

        @Override
        public int read(byte[] b, long position) throws IOException, RepositoryException {
            throw new RepositoryException("no stream");
        }

        @Override
        public long getSize() throws RepositoryException {
            return -1;
        }

        @Override
        public void dispose() { }
    }

}
