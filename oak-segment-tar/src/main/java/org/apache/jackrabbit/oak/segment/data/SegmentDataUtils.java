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

package org.apache.jackrabbit.oak.segment.data;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.io.HexDump;

class SegmentDataUtils {

    private SegmentDataUtils() {
        // Prevent instantiation
    }

    private static final int MAX_SEGMENT_SIZE = 1 << 18;

    static void hexDump(ByteBuffer buffer, OutputStream stream) throws IOException {
        byte[] data = new byte[buffer.remaining()];
        buffer.duplicate().get(data);
        HexDump.dump(data, 0, stream, 0);
    }

    static void binDump(ByteBuffer buffer, OutputStream stream) throws IOException {
        ByteBuffer data = buffer.duplicate();
        try (WritableByteChannel channel = Channels.newChannel(stream)) {
            while (data.hasRemaining()) {
                channel.write(data);
            }
        }
    }

    static int estimateMemoryUsage(ByteBuffer buffer) {
        return buffer.isDirect() ? 0 : buffer.remaining();
    }

    static ByteBuffer readBytes(ByteBuffer buffer, int index, int size) {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(index);
        duplicate.limit(index + size);
        return duplicate.slice();
    }

    static int index(ByteBuffer buffer, int recordReferenceOffset) {
        return buffer.limit() - (MAX_SEGMENT_SIZE - recordReferenceOffset);
    }


}
