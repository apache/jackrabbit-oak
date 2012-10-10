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
package org.apache.jackrabbit.oak.plugins.memory;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

public class ValueBasedBlob extends AbstractBlob {
    private final Value value;

    public ValueBasedBlob(Value value) {
        this.value = value;
    }

    @Nonnull
    @Override
    public InputStream getNewStream() {
        return new ValueBasedInputStream(value);
    }

    @Override
    public long length() {
        try {
            Binary binary = value.getBinary();
            try {
                return binary.getSize();
            }
            finally {
                binary.dispose();
            }
        }
        catch (RepositoryException e) {
            return -1;
        }
    }

    private static class ValueBasedInputStream extends InputStream {
        private final Value value;

        private Binary binary;
        private InputStream stream;

        public ValueBasedInputStream(Value value) {
            this.value = value;
        }

        @Override
        public int read() throws IOException {
            return stream().read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return stream().read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return stream().read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return stream().skip(n);
        }

        @Override
        public int available() throws IOException {
            return stream().available();
        }

        @Override
        public void close() throws IOException {
            if (stream != null) {
                stream.close();
                stream = null;
            }
            if (binary != null) {
                binary.dispose();
                binary = null;
            }
        }

        @Override
        public synchronized void mark(int readlimit) {
        }

        @Override
        public synchronized void reset() throws IOException {
            throw new IOException("mark not supported");
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        private InputStream stream() throws IOException {
            try {
                if (binary == null) {
                    binary = value.getBinary();
                    stream = binary.getStream();
                }
                return stream;
            }
            catch (RepositoryException e) {
                throw new IOException(e);
            }
        }

    }
}
