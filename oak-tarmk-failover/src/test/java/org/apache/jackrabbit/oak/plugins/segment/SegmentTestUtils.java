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
package org.apache.jackrabbit.oak.plugins.segment;

import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;

import java.util.Random;

public class SegmentTestUtils {

    private SegmentTestUtils() {

    }

    public static int newValidOffset(Random random) {
        return random.nextInt(MAX_SEGMENT_SIZE >> RECORD_ALIGN_BITS) << RECORD_ALIGN_BITS;
    }

    public static RecordId newRecordId(SegmentTracker factory, Random random) {
        SegmentId id = factory.newDataSegmentId();
        RecordId r = new RecordId(id, newValidOffset(random));
        return r;
    }
}
