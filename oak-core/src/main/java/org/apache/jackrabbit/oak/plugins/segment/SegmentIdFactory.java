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
package org.apache.jackrabbit.oak.plugins.segment;

import java.security.SecureRandom;
import java.util.UUID;

public class SegmentIdFactory {

    private static final SecureRandom random = new SecureRandom();

    private static final long MSB_MASK = ~(0xfL << 12);

    private static final long VERSION = (0x4L << 12);

    private static final long LSB_MASK = ~(0xfL << 60);

    private static final long DATA = 0xAL << 60;

    private static final long BULK = 0xBL << 60;

    private static UUID newSegmentId(long type) {
        long msb = (random.nextLong() & MSB_MASK) | VERSION;
        long lsb = (random.nextLong() & LSB_MASK) | type;
        return new UUID(msb, lsb);
    }

    static UUID newDataSegmentId() {
        return newSegmentId(DATA);
    }

    static UUID newBulkSegmentId() {
        return newSegmentId(BULK);
    }

    public static boolean isDataSegmentId(UUID id) {
        return (id.getLeastSignificantBits() & ~LSB_MASK) == DATA; 
    }

    public static boolean isBulkSegmentId(UUID id) {
        return (id.getLeastSignificantBits() & ~LSB_MASK) == BULK; 
    }

}
