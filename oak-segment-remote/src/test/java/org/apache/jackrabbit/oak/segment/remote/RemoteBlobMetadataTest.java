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

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.*;

import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;

public class RemoteBlobMetadataTest {

    @Test
    public void toSegmentMetadata() {
        RemoteSegmentArchiveEntry entry = new RemoteSegmentArchiveEntry(-7554506325726244935L, -5874985927363300041L,
                3, 5, 50, 60, true);
        HashMap<String, String> map = RemoteBlobMetadata.toSegmentMetadata(entry);

        assertEquals("segment", map.get(RemoteBlobMetadata.METADATA_TYPE));
        assertEquals("97290085-b1a5-4fb9-ae77-db6d13177537", map.get(RemoteBlobMetadata.METADATA_SEGMENT_UUID));
        assertEquals("3", map.get(RemoteBlobMetadata.METADATA_SEGMENT_POSITION));
        assertEquals("50", map.get(RemoteBlobMetadata.METADATA_SEGMENT_GENERATION));
        assertEquals("60", map.get(RemoteBlobMetadata.METADATA_SEGMENT_FULL_GENERATION));
        assertEquals("true", map.get(RemoteBlobMetadata.METADATA_SEGMENT_COMPACTED));
    }


    @Test
    public void toIndexEntry() {
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put(RemoteBlobMetadata.METADATA_SEGMENT_UUID, "97290085-b1a5-4fb9-ae77-db6d13177537");
        metadata.put(RemoteBlobMetadata.METADATA_SEGMENT_POSITION, "3");
        metadata.put(RemoteBlobMetadata.METADATA_SEGMENT_GENERATION, "50");
        metadata.put(RemoteBlobMetadata.METADATA_SEGMENT_FULL_GENERATION, "60");
        metadata.put(RemoteBlobMetadata.METADATA_SEGMENT_COMPACTED, "true");
        RemoteSegmentArchiveEntry azureSegmentArchiveEntry = RemoteBlobMetadata.toIndexEntry(metadata, 5);
        System.out.println(azureSegmentArchiveEntry);


        assertEquals(-7554506325726244935L, azureSegmentArchiveEntry.getMsb());
        assertEquals(-5874985927363300041L, azureSegmentArchiveEntry.getLsb());
        assertEquals(3, azureSegmentArchiveEntry.getPosition());
        assertEquals(5, azureSegmentArchiveEntry.getLength());
        assertEquals(50, azureSegmentArchiveEntry.getGeneration());
        assertEquals(60, azureSegmentArchiveEntry.getFullGeneration());
        assertTrue(azureSegmentArchiveEntry.isCompacted());
    }

    @Test
    public void isSegment() {
        assertTrue(RemoteBlobMetadata.isSegment(Collections.singletonMap("type", "segment")));

        assertFalse(RemoteBlobMetadata.isSegment(Collections.singletonMap("type", "index")));
    }
}
