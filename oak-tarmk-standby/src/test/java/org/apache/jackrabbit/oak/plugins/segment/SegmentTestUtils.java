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
package org.apache.jackrabbit.oak.plugins.segment;

import static java.io.File.createTempFile;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public final class SegmentTestUtils {

    private SegmentTestUtils() { }

    public static int newValidOffset(Random random) {
        return random.nextInt(MAX_SEGMENT_SIZE >> RECORD_ALIGN_BITS) << RECORD_ALIGN_BITS;
    }

    public static RecordId newRecordId(SegmentTracker factory, Random random) {
        SegmentId id = factory.newDataSegmentId();
        RecordId r = new RecordId(id, newValidOffset(random));
        return r;
    }

    public static void assertEqualStores(File d1, File d2) throws Exception {
        FileStore f1 = FileStore.builder(d1).withMaxFileSize(1).withMemoryMapping(false).build();
        FileStore f2 = FileStore.builder(d2).withMaxFileSize(1).withMemoryMapping(false).build();
        try {
            assertEquals(f1.getHead(), f2.getHead());
        } finally {
            f1.close();
            f2.close();
        }
    }

    public static void addTestContent(NodeStore store, String child)
            throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child(child).setProperty("ts", System.currentTimeMillis());
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    public static File createTmpTargetDir(String name) throws IOException {
        File f = createTempFile(name, "dir", new File("target"));
        f.delete();
        f.mkdir();
        return f;
    }

}
