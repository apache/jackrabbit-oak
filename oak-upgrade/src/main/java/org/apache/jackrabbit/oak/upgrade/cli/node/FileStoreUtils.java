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
package org.apache.jackrabbit.oak.upgrade.cli.node;

import java.io.Closeable;
import java.io.IOException;

import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeStore;

public class FileStoreUtils {
    private FileStoreUtils() {

    }

    public static Closeable asCloseable(final ReadOnlyFileStore fs) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                fs.close();
            }
        };
    }

    public static Closeable asCloseable(final FileStore fs) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                fs.close();
            }
        };
    }

    public static boolean hasExternalBlobReferences(ReadOnlyFileStore fs) {
        try {
            for (SegmentId id : fs.getSegmentIds()) {
                if (!id.isDataSegmentId()) {
                    continue;
                }
                id.getSegment().forEachRecord(new Segment.RecordConsumer() {
                    @Override
                    public void consume(int number, RecordType type, int offset) {
                        // FIXME the consumer should allow to stop processing
                        // see java.nio.file.FileVisitor
                        if (type == RecordType.BLOB_ID) {
                            throw new ExternalBlobFound();
                        }
                    }
                });
            }
            return false;
        } catch (ExternalBlobFound e) {
            return true;
        } finally {
            fs.close();
        }
    }

    private static class ExternalBlobFound extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    public static class NodeStoreWithFileStore extends ProxyNodeStore {

        private final SegmentNodeStore segmentNodeStore;

        private final FileStore fileStore;

        public NodeStoreWithFileStore(SegmentNodeStore segmentNodeStore, FileStore fileStore) {
            this.segmentNodeStore = segmentNodeStore;
            this.fileStore = fileStore;
        }

        public FileStore getFileStore() {
            return fileStore;
        }

        @Override
        public SegmentNodeStore getNodeStore() {
            return segmentNodeStore;
        }
    }
}
