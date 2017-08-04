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

package org.apache.jackrabbit.oak.segment.file.tar.index;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.ByteBuffer.wrap;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class IndexLoader {

    public static IndexLoader newIndexLoader(int blockSize) {
        checkArgument(blockSize > 0, "Invalid block size");
        return new IndexLoader(blockSize);
    }

    private final int blockSize;

    private final IndexLoaderV1 v1;

    private final IndexLoaderV2 v2;

    private IndexLoader(int blockSize) {
        this.blockSize = blockSize;
        this.v1 = new IndexLoaderV1(blockSize);
        this.v2 = new IndexLoaderV2(blockSize);
    }

    public Index loadIndex(RandomAccessFile file) throws IOException, InvalidIndexException {
        long length = file.length();

        if (length % blockSize != 0 || length < 6 * blockSize || length > Integer.MAX_VALUE) {
            throw new InvalidIndexException(String.format("Unexpected size %d", length));
        }

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        file.seek(length - 2 * blockSize - Integer.BYTES);
        file.readFully(buffer.array());
        int magic = buffer.getInt();

        if (magic == IndexLoaderV1.MAGIC) {
            return v1.loadIndex(file);
        }
        if (magic == IndexLoaderV2.MAGIC) {
            return v2.loadIndex(file);
        }

        throw new InvalidIndexException("Unrecognized magic number");
    }

}
