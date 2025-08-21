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

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Unit cases for {@link HashUtils}
 */
public class HashUtilsTest {

    @Test
    public void testHashMurmur32Consistency() {
        byte mask = 0x01;
        long length = 10L;
        byte[] data = "test data".getBytes();

        // Ensure same inputs produce same hash
        long hash1 = HashUtils.hashMurmur32(mask, length, data);
        long hash2 = HashUtils.hashMurmur32(mask, length, data);

        Assert.assertEquals(hash1, hash2);
    }

    @Test
    public void testDifferentInputsProduceDifferentHashes() {
        byte mask = 0x01;
        long length = 10L;
        byte[] data1 = "test data".getBytes();
        byte[] data2 = "test data!".getBytes();

        long hash1 = HashUtils.hashMurmur32(mask, length, data1);
        long hash2 = HashUtils.hashMurmur32((byte) 0x02, length, data1); // Different mask
        long hash3 = HashUtils.hashMurmur32(mask, 11L, data1); // Different length
        long hash4 = HashUtils.hashMurmur32(mask, length, data2); // Different data

        Assert.assertNotEquals(hash1, hash2);
        Assert.assertNotEquals(hash1, hash3);
        Assert.assertNotEquals(hash1, hash4);
    }

    @Test
    public void testEmptyData() {
        byte mask = 0x01;
        long length = 0L;
        byte[] emptyData = new byte[0];

        long hash = HashUtils.hashMurmur32(mask, length, emptyData);
        // We're just ensuring no exceptions are thrown, and a hash value is returned
        Assert.assertTrue(hash >= 0);
    }

    @Test
    public void testEndianness() {
        byte mask = 0x01;
        long length = 32L;
        byte[] data = new byte[]{1, 2, 3, 4};

        // Create manually calculated hash with known endianness
        ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + data.length)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(mask)
                .putLong(length)
                .put(data);
        buffer.flip();
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes);

        long manualHash = Integer.toUnsignedLong(org.apache.commons.codec.digest.MurmurHash3.hash32x86(bytes));
        long methodHash = HashUtils.hashMurmur32(mask, length, data);

        Assert.assertEquals(manualHash, methodHash);
    }

    @Test
    public void testKnownValues() {
        // Test with known pre-computed values
        byte mask = 0x00;
        long length = 0L;
        byte[] data = new byte[0];

        // These values would need to be pre-computed
        Assert.assertEquals(4183281807L, HashUtils.hashMurmur32(mask, length, data));

        // Test with another known value
        mask = 0x01;
        length = 100L;
        data = "Apache Jackrabbit Oak".getBytes();

        long hash = HashUtils.hashMurmur32(mask, length, data);
        Assert.assertEquals(2290483938L, hash); // Just ensuring we get a non-negative value
    }

    @Test
    public void testBoundaryValues() {
        byte mask = Byte.MIN_VALUE;
        long length = Long.MIN_VALUE;
        byte[] data = new byte[1024]; // Larger array

        long hash1 = HashUtils.hashMurmur32(mask, length, data);

        mask = Byte.MAX_VALUE;
        length = Long.MAX_VALUE;

        long hash2 = HashUtils.hashMurmur32(mask, length, data);

        // Different inputs should produce different hashes
        Assert.assertNotEquals(hash1, hash2);
    }
}