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

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

class MappedAccess implements FileAccess {

    private final MappedByteBuffer buffer;

    private boolean updated = false;

    MappedAccess(RandomAccessFile file, int length) throws IOException {
        try {
            long l = file.length();
            if (l == 0) { // it's a new file
                l = length;
                updated = true;
            }
            buffer = file.getChannel().map(READ_WRITE, 0, l);
        } finally {
            file.close();
        }
    }

    @Override
    public int length() {
        return buffer.limit();
    }

    @Override
    public ByteBuffer read(int position, int length) {
        ByteBuffer entry = buffer.asReadOnlyBuffer();
        entry.position(position);
        entry.limit(position + length);
        return entry.slice();
    }

    @Override
    public synchronized void write(
            int position, byte[] b, int offset, int length)
            throws IOException {
        ByteBuffer entry = buffer.duplicate();
        entry.position(position);
        entry.put(b, offset, length);
        updated = true;
    }

    @Override
    public synchronized void flush() throws IOException {
        if (updated) {
            buffer.force();
            updated = false;
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

}
