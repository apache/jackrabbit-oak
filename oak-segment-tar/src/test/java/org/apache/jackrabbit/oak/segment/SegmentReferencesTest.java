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

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.util.Arrays;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentReferencesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore newFileStore() throws Exception {
        return FileStoreBuilder.fileStoreBuilder(folder.getRoot()).build();
    }

    @Test
    public void segmentShouldNotReferenceItself() throws Exception {
        try (FileStore store = newFileStore()) {

            // Write two records, one referencing the other.

            SegmentWriter writer = defaultSegmentWriterBuilder("test").build(store);
            RecordId stringId = writer.writeString("test");
            RecordId listId = writer.writeList(Arrays.asList(stringId, stringId));
            writer.flush();

            // The two records should be living in the same segment.

            assertEquals(listId.getSegmentId(), stringId.getSegmentId());

            // This inter-segment reference shouldn't generate a reference from
            // this segment to itself.

            assertEquals(0, listId.getSegment().getReferencedSegmentIdCount());
        }
    }

    @Test
    public void segmentShouldExposeReferencedSegments() throws Exception {
        try (FileStore store = newFileStore()) {

            // Write two records, one referencing the other.

            SegmentWriter writer = defaultSegmentWriterBuilder("test").build(store);

            RecordId stringId = writer.writeString("test");
            writer.flush();

            RecordId listId = writer.writeList(Arrays.asList(stringId, stringId));
            writer.flush();

            // The two records should be living in two different segments.

            assertNotEquals(listId.getSegmentId(), stringId.getSegmentId());

            // This intra-segment reference should generate a reference from the
            // segment containing the list to the segment containing the string.

            assertEquals(1, listId.getSegment().getReferencedSegmentIdCount());
            assertEquals(stringId.getSegmentId().asUUID(), listId.getSegment().getReferencedSegmentId(0));
        }
    }

}
