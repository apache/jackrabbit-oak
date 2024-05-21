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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.junit.Test;

public class BoundedHistogramTest {

    @Test
    public void testOverflow() {
        int maxSize = 100;
        BoundedHistogram histogram = new BoundedHistogram("test", maxSize);

        assertFalse(histogram.isOverflowed());

        // Add enough entries to overflow the histogram
        for (int i = 0; i < maxSize; i++) {
            histogram.addEntry("/a/b/c/path" + i);
        }
        assertFalse(histogram.isOverflowed());
        for (int i = maxSize; i < maxSize * 2; i++) {
            histogram.addEntry("/a/b/c/path" + i);
        }
        assertTrue(histogram.isOverflowed());

        // Another pass. The entries that are already in the histogram should be incremented
        for (int i = 0; i < maxSize * 2; i++) {
            histogram.addEntry("/a/b/c/path" + i);
        }
        // Check that the size was capped at maxSize
        ConcurrentHashMap<String, LongAdder> map = histogram.getMap();
        assertEquals(maxSize, map.size());
        map.values().forEach((v) -> assertEquals(2, v.intValue()));
    }
}
