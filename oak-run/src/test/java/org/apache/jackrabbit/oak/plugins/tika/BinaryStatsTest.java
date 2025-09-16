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
package org.apache.jackrabbit.oak.plugins.tika;

import org.apache.commons.collections4.FluentIterable;
import org.apache.jackrabbit.oak.plugins.tika.BinaryStats.MimeTypeStats;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinaryStatsTest {

    @Test
    public void testMimeTypeStatsComparison() throws IOException {
        // Create BinaryStats instance with a minimal implementation of BinaryResourceProvider
        BinaryStats stats = new BinaryStats(null, path -> FluentIterable.of());

        // Create test MimeTypeStats instances
        MimeTypeStats indexedLargeSize = stats.createStat("application/pdf");
        indexedLargeSize.setIndexed(true);
        indexedLargeSize.addSize(1000);

        MimeTypeStats indexedSmallSize = stats.createStat("text/plain");
        indexedSmallSize.setIndexed(true);
        indexedSmallSize.addSize(100);

        MimeTypeStats notIndexedLargeSize = stats.createStat("image/png");
        notIndexedLargeSize.setIndexed(false);
        notIndexedLargeSize.addSize(2000);

        MimeTypeStats notIndexedSmallSize = stats.createStat("audio/mp3");
        notIndexedSmallSize.setIndexed(false);
        notIndexedSmallSize.addSize(500);

        // Test case 1: Compare by indexed status (false first, then true)
        Assert.assertTrue(notIndexedLargeSize.compareTo(indexedLargeSize) < 0);
        Assert.assertTrue(indexedLargeSize.compareTo(notIndexedLargeSize) > 0);

        // Test case 2: Same indexed status, compare by size (larger comes first)
        Assert.assertTrue(indexedLargeSize.compareTo(indexedSmallSize) > 0);
        Assert.assertTrue(notIndexedLargeSize.compareTo(notIndexedSmallSize) > 0);

        // Test case 3: Sort a list and verify ordering
        List<MimeTypeStats> statsList = new ArrayList<>();
        statsList.add(indexedLargeSize);
        statsList.add(notIndexedSmallSize);
        statsList.add(indexedSmallSize);
        statsList.add(notIndexedLargeSize);

        Collections.sort(statsList);

        // Verify sort order: not indexed first (larger to smaller), then indexed (larger to smaller)
        Assert.assertSame(notIndexedSmallSize, statsList.get(0));
        Assert.assertSame(notIndexedLargeSize, statsList.get(1));
        Assert.assertSame(indexedSmallSize, statsList.get(2));
        Assert.assertSame(indexedLargeSize, statsList.get(3));
    }
}