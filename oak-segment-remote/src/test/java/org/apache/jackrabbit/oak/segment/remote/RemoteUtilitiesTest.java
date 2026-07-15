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
package org.apache.jackrabbit.oak.segment.remote;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class RemoteUtilitiesTest {
    @Test
    public void testValidEntryIndex() {
        UUID uuid = UUID.randomUUID();
        String name = RemoteUtilities.getSegmentFileName(
                RemoteUtilities.MAX_ENTRY_COUNT - 1,
                uuid.getMostSignificantBits(),
                uuid.getLeastSignificantBits()
        );
        assertEquals(uuid, RemoteUtilities.getSegmentUUID(name));
    }

    @Test
    public void testInvalidEntryIndex() {
        UUID uuid = UUID.randomUUID();
        String name = RemoteUtilities.getSegmentFileName(
            RemoteUtilities.MAX_ENTRY_COUNT,
            uuid.getMostSignificantBits(),
            uuid.getLeastSignificantBits()
        );
        assertNotEquals(uuid, RemoteUtilities.getSegmentUUID(name));
    }

    private void expectArchiveSortOrder(List<String> expectedOrder) {
        List<String> archives = new ArrayList<>(expectedOrder);
        Collections.shuffle(archives);
        archives.sort(RemoteUtilities.ARCHIVE_INDEX_COMPARATOR);
        assertEquals(expectedOrder, archives);
    }

    @Test
    public void testSortArchives() {
        expectArchiveSortOrder(Arrays.asList("data00001a.tar", "data00002a.tar", "data00003a.tar"));
    }

    @Test
    public void testSortArchivesLargeIndices() {
        expectArchiveSortOrder(Arrays.asList("data00003a.tar", "data20000a.tar", "data100000a.tar"));
    }

    @Test
    public void testIsSegmentName_ValidName() {
        UUID uuid = UUID.randomUUID();
        String validName = RemoteUtilities.getSegmentFileName(0, uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        assertTrue(RemoteUtilities.isSegmentName(validName));

        String validMaxName = RemoteUtilities.getSegmentFileName(
            RemoteUtilities.MAX_ENTRY_COUNT - 1,
            uuid.getMostSignificantBits(),
            uuid.getLeastSignificantBits()
        );
        assertTrue(RemoteUtilities.isSegmentName(validMaxName));
    }

    @Test
    public void testIsSegmentName_InvalidNames() {
        // closed marker
        assertFalse(RemoteUtilities.isSegmentName("closed"));

        // metadata files
        assertFalse(RemoteUtilities.isSegmentName("data00000a.tar.brf"));
        assertFalse(RemoteUtilities.isSegmentName("data00000a.tar.gph"));
        assertFalse(RemoteUtilities.isSegmentName("data00000a.tar.idx"));

        // empty value
        assertFalse(RemoteUtilities.isSegmentName(""));
        assertFalse(RemoteUtilities.isSegmentName(null));
    }
}
