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
package org.apache.jackrabbit.oak.segment.azure;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.*;

public class AzureBlobMetadataTest {

    @Test
    public void toSegmentMetadata() {
        AzureSegmentArchiveEntry entry = new AzureSegmentArchiveEntry(-7554506325726244935L, -5874985927363300041L,
                3, 5, 50, 60, true);
        HashMap<String, String> map = AzureBlobMetadata.toSegmentMetadata(entry);

        assertEquals("segment", map.get(AzureBlobMetadata.METADATA_TYPE));
        assertEquals("97290085-b1a5-4fb9-ae77-db6d13177537", map.get(AzureBlobMetadata.METADATA_SEGMENT_UUID));
        assertEquals("3", map.get(AzureBlobMetadata.METADATA_SEGMENT_POSITION));
        assertEquals("50", map.get(AzureBlobMetadata.METADATA_SEGMENT_GENERATION));
        assertEquals("60", map.get(AzureBlobMetadata.METADATA_SEGMENT_FULL_GENERATION));
        assertEquals("true", map.get(AzureBlobMetadata.METADATA_SEGMENT_COMPACTED));
    }


    @Test
    public void toIndexEntry() {
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_UUID, "97290085-b1a5-4fb9-ae77-db6d13177537");
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_POSITION, "3");
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_GENERATION, "50");
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_FULL_GENERATION, "60");
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_COMPACTED, "true");
        AzureSegmentArchiveEntry azureSegmentArchiveEntry = AzureBlobMetadata.toIndexEntry(metadata, 5);
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
    public void toIndexEntry_caseInsensitive() {
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_UUID.toUpperCase(), "97290085-b1a5-4fb9-ae77-db6d13177537");
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_POSITION.toUpperCase(), "3");
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_GENERATION.toUpperCase(), "50");
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_FULL_GENERATION.toUpperCase(), "60");
        metadata.put(AzureBlobMetadata.METADATA_SEGMENT_COMPACTED.toUpperCase(), "true");
        AzureSegmentArchiveEntry azureSegmentArchiveEntry = AzureBlobMetadata.toIndexEntry(metadata, 5);

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
        assertTrue(AzureBlobMetadata.isSegment(Collections.singletonMap("type", "segment")));

        assertFalse(AzureBlobMetadata.isSegment(Collections.singletonMap("type", "index")));
    }


    @Test
    public void isSegment_caseInsensitive() {
        assertTrue(AzureBlobMetadata.isSegment(Collections.singletonMap("Type", "segment")));
        assertTrue(AzureBlobMetadata.isSegment(Collections.singletonMap("TYPE", "segment")));
        assertTrue(AzureBlobMetadata.isSegment(Collections.singletonMap("tYPE", "segment")));
    }
}
