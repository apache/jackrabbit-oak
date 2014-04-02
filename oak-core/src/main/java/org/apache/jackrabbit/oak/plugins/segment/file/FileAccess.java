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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.zip.CRC32;

abstract class FileAccess {

    static FileAccess open(File file, boolean memoryMapping)
            throws IOException {
        RandomAccessFile access = new RandomAccessFile(file, "r");
        if (memoryMapping) {
            return new Mapped(access);
        } else {
            return new Random(access);
        }
    }

    abstract boolean isMemoryMapped();

    abstract int length() throws IOException;

    abstract long crc32(int position, int size) throws IOException;

    abstract ByteBuffer read(int position, int length) throws IOException;

    abstract void close() throws IOException;

    //-----------------------------------------------------------< private >--

    private static class Mapped extends FileAccess {

        private final MappedByteBuffer buffer;

        Mapped(RandomAccessFile file) throws IOException {
            try {
                buffer = file.getChannel().map(READ_ONLY, 0, file.length());
            } finally {
                file.close();
            }
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
        public long crc32(int position, int length) {
            ByteBuffer entry = buffer.asReadOnlyBuffer();
            entry.position(entry.position() + position);

            byte[] data = new byte[length];
            entry.get(data);

            CRC32 checksum = new CRC32();
            checksum.update(data);
            return checksum.getValue();
        }

        @Override
        public ByteBuffer read(int position, int length) {
            ByteBuffer entry = buffer.asReadOnlyBuffer();
            entry.position(entry.position() + position);
            entry.limit(entry.position() + length);
            return entry.slice();
        }

        @Override
        public void close() {
        }

    }

    private static class Random extends FileAccess {

        private final RandomAccessFile file;

        Random(RandomAccessFile file) {
            this.file = file;
        }

        @Override
        boolean isMemoryMapped() {
            return false;
        }

        @Override
        public int length() throws IOException {
            long length = file.length();
            checkState(length < Integer.MAX_VALUE);
            return (int) length;
        }

        @Override
        public long crc32(int position, int length) throws IOException {
            CRC32 checksum = new CRC32();
            checksum.update(read(position, length).array());
            return checksum.getValue();
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