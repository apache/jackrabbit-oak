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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

class RandomAccess implements FileAccess {

    private final RandomAccessFile file;

    RandomAccess(@Nonnull RandomAccessFile file) throws IOException {
        this.file = checkNotNull(file);
    }

    @Override
    public int length() throws IOException {
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
    public synchronized void write(
            int position, byte[] buffer, int offset, int length)
            throws IOException {
        file.seek(position);
        file.write(buffer, offset, length);
    }

    @Override
    public void flush() throws IOException {
        file.getFD().sync();
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

}
