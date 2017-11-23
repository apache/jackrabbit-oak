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
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.segment.Segment.RecordConsumer;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BlobIdRecordTest {

    private static abstract class IdMappingBlobStore implements BlobStore {

        private final MemoryBlobStore bs = new MemoryBlobStore();

        private final Map<String, String> ids = new HashMap<>();

        @Override
        public String writeBlob(InputStream inputStream) throws IOException {
            String in = bs.writeBlob(inputStream);
            String out = generateId();
            ids.put(out, in);
            return out;
        }

        @Override
        public String writeBlob(InputStream inputStream, BlobOptions options) throws IOException {
            return writeBlob(inputStream);
        }

        @Override
        public int readBlob(String s, long l, byte[] bytes, int i, int i1) throws IOException {
            return bs.readBlob(mapId(s), l, bytes, i, i1);
        }

        @Override
        public long getBlobLength(String s) throws IOException {
            return bs.getBlobLength(mapId(s));
        }

        @Override
        public InputStream getInputStream(String s) throws IOException {
            return bs.getInputStream(mapId(s));
        }

        @Override
        public String getBlobId(@Nonnull String s) {
            return bs.getBlobId(s);
        }

        @Override
        public String getReference(@Nonnull String s) {
            return bs.getBlobId(mapId(s));
        }

        private String mapId(String in) {
            String out = ids.get(in);
            if (out == null) {
                throw new IllegalArgumentException("in");
            }
            return out;
        }

        protected abstract String generateId();

    }

    private static class ShortIdMappingBlobStore extends IdMappingBlobStore {

        private static int next;

        @Override
        protected String generateId() {
            return Integer.toString(next++);
        }

    }

    private static class LongIdMappingBlobStore extends IdMappingBlobStore {

        private static int next;

        @Override
        protected String generateId() {
            return Strings.repeat("0", Segment.BLOB_ID_SMALL_LIMIT) + Integer.toString(next++);
        }

    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void shortReferencesShouldHaveBlobIdType() throws Exception {
        try (FileStore ss = newFileStore(new ShortIdMappingBlobStore())) {
            SegmentWriter sw = defaultSegmentWriterBuilder("test").build(ss);
            byte[] content = new byte[Segment.MEDIUM_LIMIT + 1];
            SegmentBlob sb = new SegmentBlob(ss.getBlobStore(), sw.writeBlob(new ArrayBasedBlob(content)));
            assertRecordTypeEquals(sb, RecordType.BLOB_ID);
        }
    }

    @Test
    public void longReferencesShouldHaveBlobIdType() throws Exception {
        try (FileStore ss = newFileStore(new LongIdMappingBlobStore())) {
            SegmentWriter sw = defaultSegmentWriterBuilder("test").build(ss);
            byte[] content = new byte[Segment.MEDIUM_LIMIT + 1];
            SegmentBlob sb = new SegmentBlob(ss.getBlobStore(), sw.writeBlob(new ArrayBasedBlob(content)));
            assertRecordTypeEquals(sb, RecordType.BLOB_ID);
        }
    }

    private FileStore newFileStore(BlobStore blobStore) throws Exception {
        return fileStoreBuilder(folder.newFolder("ss")).withBlobStore(blobStore).build();
    }

    private void assertRecordTypeEquals(final Record record, final RecordType expected) {
        record.getSegment().forEachRecord(new RecordConsumer() {

            @Override
            public void consume(int number, RecordType type, int offset) {
                if (number == record.getRecordNumber()) {
                    assertEquals(expected, type);
                }
            }

        });
    }

}
