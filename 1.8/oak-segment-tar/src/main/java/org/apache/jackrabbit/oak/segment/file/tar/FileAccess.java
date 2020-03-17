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
package org.apache.jackrabbit.oak.segment.file.tar;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * A wrapper around either memory mapped files or random access files, to allow
 * reading from a file.
 */
abstract class FileAccess {

    abstract boolean isMemoryMapped();

    abstract int length() throws IOException;

    abstract ByteBuffer read(int position, int length) throws IOException;

    abstract void close() throws IOException;

    //-----------------------------------------------------------< private >--

    /**
     * The implementation that uses memory mapped files.
     */
    static class Mapped extends FileAccess {

        private final RandomAccessFile file;

        private MappedByteBuffer buffer;

        Mapped(RandomAccessFile file) throws IOException {
            this.file = file;
            this.buffer = file.getChannel().map(READ_ONLY, 0, file.length());
        }

        @Override
        boolean isMemoryMapped() {
            return true;
        }

        @Override
        public int length() {
            return buffer.remaining();
        }

        @Override
        public ByteBuffer read(int position, int length) {
            ByteBuffer entry = buffer.asReadOnlyBuffer();
            entry.position(entry.position() + position);
            entry.limit(entry.position() + length);
            return entry.slice();
        }

        @Override
        public void close() throws IOException {
            buffer = null;
            file.close();
        }

    }
    
    /**
     * The implementation that uses random access file (reads are synchronized).
     */    
    static class Random extends FileAccess {

        private final RandomAccessFile file;

        Random(RandomAccessFile file) {
            this.file = file;
        }

        @Override
        boolean isMemoryMapped() {
            return false;
        }

        @Override
        public synchronized int length() throws IOException {
            long length = file.length();
            checkState(length < Integer.MAX_VALUE);
            return (int) length;
        }

        @Override
        public synchronized ByteBuffer read(int position, int length)
                throws IOException {
            ByteBuffer entry = ByteBuffer.allocate(length);
            file.seek(position);
            file.readFully(entry.array());
            return entry;
        }

        @Override
        public synchronized void close() throws IOException {
            file.close();
        }

    }

}
