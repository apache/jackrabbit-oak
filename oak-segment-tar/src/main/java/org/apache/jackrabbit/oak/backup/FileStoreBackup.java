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

package org.apache.jackrabbit.oak.backup;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.segment.Compactor;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentBufferWriter;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.WriterCacheManager;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tooling.BasicReadOnlyBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreBackup {

    private static final Logger log = LoggerFactory
            .getLogger(FileStoreBackup.class);

    public static boolean USE_FAKE_BLOBSTORE = Boolean
            .getBoolean("oak.backup.UseFakeBlobStore");

    public static void backup(@Nonnull SegmentReader reader,
                              @Nonnull Revisions revisions,
                              @Nonnull File destination)
            throws IOException, InvalidFileStoreVersionException {
        Stopwatch watch = Stopwatch.createStarted();
        SegmentGCOptions gcOptions = SegmentGCOptions.defaultGCOptions()
                .setOffline();

        FileStoreBuilder builder = fileStoreBuilder(destination)
                .withDefaultMemoryMapping();
        if (USE_FAKE_BLOBSTORE) {
            builder.withBlobStore(new BasicReadOnlyBlobStore());
        }
        builder.withGCOptions(gcOptions);
        FileStore backup = builder.build();
        SegmentNodeState current = reader.readHeadState(revisions);
        try {
            int gen = 0;
            gen = current.getRecordId().getSegmentId().getGcGeneration();
            SegmentBufferWriter bufferWriter = new SegmentBufferWriter(
                    backup,
                    backup.getTracker(),
                    backup.getReader(),
                    "b",
                    gen
            );
            SegmentWriter writer = new SegmentWriter(
                    backup,
                    backup.getReader(),
                    backup.getBlobStore(),
                    new WriterCacheManager.Default(),
                    bufferWriter,
                    backup.getBinaryReferenceConsumer()
            );
            Compactor compactor = new Compactor(backup.getReader(), writer,
                    backup.getBlobStore(), Suppliers.ofInstance(false),
                    gcOptions);
            compactor.setContentEqualityCheck(true);
            SegmentNodeState head = backup.getHead();
            SegmentNodeState after = compactor.compact(head, current, head);
            if (after != null) {
                backup.getRevisions().setHead(head.getRecordId(),
                        after.getRecordId());
            }
        } finally {
            backup.close();
            backup = null;
        }

        FileStoreBuilder builder2 = fileStoreBuilder(destination)
                .withDefaultMemoryMapping();
        builder2.withGCOptions(gcOptions);
        backup = builder2.build();
        try {
            cleanup(backup);
        } finally {
            backup.close();
        }
        watch.stop();
        log.info("Backup finished in {}.", watch);
    }

    static boolean cleanup(FileStore f) throws IOException {
        boolean ok = true;
        for (File file : f.cleanup()) {
            ok = ok && file.delete();
        }
        return true;
    }
}
