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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.util.List;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentBufferWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore openFileStore() throws Exception {
        return fileStoreBuilder(folder.getRoot()).build();
    }

    private ReadOnlyFileStore openReadOnlyFileStore() throws Exception {
        return fileStoreBuilder(folder.getRoot()).buildReadOnly();
    }

    @Test
    public void nonDirtyBuffersShouldNotBeFlushed() throws Exception {
        List<SegmentId> before;

        try (FileStore store = openFileStore()) {
            // init
        }
        try (ReadOnlyFileStore store = openReadOnlyFileStore()) {
            before = newArrayList(store.getSegmentIds());
        }

        try (FileStore store = openFileStore()) {
            defaultSegmentWriterBuilder("t").build(store).flush();
        }

        List<SegmentId> after;

        try (ReadOnlyFileStore store = openReadOnlyFileStore()) {
            after = newArrayList(store.getSegmentIds());
        }

        assertEquals(before, after);
    }

    @Test
    public void dirtyBuffersShouldBeFlushed() throws Exception {
        List<SegmentId> before;

        try (FileStore store = openFileStore()) {
            // init
        }
        try (ReadOnlyFileStore store = openReadOnlyFileStore()) {
            before = newArrayList(store.getSegmentIds());
        }

        try (FileStore store = openFileStore()) {
            SegmentWriter writer = defaultSegmentWriterBuilder("t").build(store);
            writer.writeString("test");
            writer.flush();
        }

        List<SegmentId> after;

        try (ReadOnlyFileStore store = openReadOnlyFileStore()) {
            after = newArrayList(store.getSegmentIds());
        }

        assertNotEquals(before, after);
    }

}
