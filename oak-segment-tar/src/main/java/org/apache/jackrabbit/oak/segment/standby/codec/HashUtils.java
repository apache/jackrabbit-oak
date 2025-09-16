/**
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
package org.apache.jackrabbit.oak.segment.standby.codec;

import org.apache.commons.codec.digest.MurmurHash3;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HashUtils {
    private HashUtils() {
        // no instances for you
    }

    /**
     * Computes a MurmurHash3 hash value for the provided data components.
     * <p>
     * This method combines a byte mask, a long length value, and a byte array into a single
     * byte sequence using little-endian byte ordering, then computes a 32-bit MurmurHash3
     * hash of this sequence, returned as an unsigned long.
     *
     * @param mask    A byte value to include in the hash computation
     * @param length  A long value to include in the hash computation (stored in little-endian order)
     * @param data    The byte array data to include in the hash computation
     * @return        The computed MurmurHash3 value as an unsigned 32-bit integer converted to long
     */
    public static long hashMurmur32(byte mask, long length, byte[] data) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(1 + 8 + data.length)
                .order(ByteOrder.LITTLE_ENDIAN)  // To align with Guava that uses Little Endianess
                .put(mask)
                .putLong(length)
                .put(data);

        byteBuffer.flip();  // Reset position to start to read data from beginning

        // create a byte array with exact size to avoid any un-initialized values to interfere with hash calculation
        final byte[] bytes = new byte[byteBuffer.limit()];
        byteBuffer.get(bytes);

        return Integer.toUnsignedLong(MurmurHash3.hash32x86(bytes));
    }
}
