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

package org.apache.jackrabbit.oak.segment.standby.server;

import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Iterator;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DefaultStandbyReferenceReaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore newFileStore() throws Exception {
        return FileStoreBuilder.fileStoreBuilder(folder.getRoot()).build();
    }

    @Test
    public void shouldReturnNullWhenSegmentDoesNotExist() throws Exception {
        try (FileStore store = newFileStore()) {
            DefaultStandbyReferencesReader reader = new DefaultStandbyReferencesReader(store);
            assertNull(reader.readReferences(randomUUID().toString()));
        }

    }

    @Test
    public void shouldReturnEmptyReferences() throws Exception {
        try (FileStore store = newFileStore()) {
            SegmentWriter writer = defaultSegmentWriterBuilder("test").build(store);

            RecordId id = writer.writeString("test");
            writer.flush();

            DefaultStandbyReferencesReader reader = new DefaultStandbyReferencesReader(store);
            Iterable<String> references = reader.readReferences(id.getSegmentId().asUUID().toString());
            assertFalse(references.iterator().hasNext());
        }
    }

    @Test
    public void shouldReturnReferences() throws Exception {
        try (FileStore store = newFileStore()) {
            SegmentWriter writer = defaultSegmentWriterBuilder("test").build(store);

            RecordId a = writer.writeString("test");
            writer.flush();

            RecordId b = writer.writeList(asList(a, a));
            writer.flush();

            DefaultStandbyReferencesReader reader = new DefaultStandbyReferencesReader(store);
            Iterator<String> i = reader.readReferences(b.getSegmentId().asUUID().toString()).iterator();
            assertTrue(i.hasNext());
            assertEquals(a.getSegmentId().asUUID().toString(), i.next());
            assertFalse(i.hasNext());
        }
    }

}
