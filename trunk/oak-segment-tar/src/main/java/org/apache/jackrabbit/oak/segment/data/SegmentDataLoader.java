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

import java.nio.ByteBuffer;

class SegmentDataLoader {

    private static final int VERSION_OFFSET = 3;

    private static final byte SEGMENT_DATA_V12 = 12;

    private static final byte SEGMENT_DATA_V13 = 13;

    static SegmentData newSegmentData(ByteBuffer buffer) {
        switch (buffer.get(VERSION_OFFSET)) {
            case SEGMENT_DATA_V12:
                return new SegmentDataV12(buffer);
            case SEGMENT_DATA_V13:
                return new SegmentDataV13(buffer);
            default:
                throw new IllegalArgumentException("invalid segment buffer");
        }
    }

    static SegmentData newRawSegmentData(ByteBuffer buffer) {
        return new SegmentDataRaw(buffer);
    }

    private SegmentDataLoader() {
        // Prevent instantiation
    }
}
