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

package org.apache.jackrabbit.oak.backup.impl;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.backup.FileStoreBackup;
import org.apache.jackrabbit.oak.segment.Compactor;
import org.apache.jackrabbit.oak.segment.DefaultSegmentWriter;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentBufferWriter;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.WriterCacheManager;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tooling.BasicReadOnlyBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreBackupImpl implements FileStoreBackup {

    private static final Logger log = LoggerFactory.getLogger(FileStoreBackupImpl.class);

    public static final boolean USE_FAKE_BLOBSTORE = Boolean.getBoolean("oak.backup.UseFakeBlobStore");

    @Override
    public void backup(@Nonnull SegmentReader reader, @Nonnull Revisions revisions, @Nonnull File destination) throws IOException, InvalidFileStoreVersionException {
        Stopwatch watch = Stopwatch.createStarted();
        SegmentGCOptions gcOptions = SegmentGCOptions.defaultGCOptions().setOffline();

        FileStoreBuilder builder = fileStoreBuilder(destination)
            .withStrictVersionCheck(true)
            .withDefaultMemoryMapping();

        if (USE_FAKE_BLOBSTORE) {
            builder.withBlobStore(new BasicReadOnlyBlobStore());
        }

        builder.withGCOptions(gcOptions);
        FileStore backup = builder.build();
        SegmentNodeState current = reader.readHeadState(revisions);

        try {
            GCGeneration gen = current.getRecordId().getSegmentId().getGcGeneration();
            SegmentBufferWriter bufferWriter = new SegmentBufferWriter(
                    backup.getSegmentIdProvider(),
                    backup.getReader(),
                    "b",
                    gen
            );
            SegmentWriter writer = new DefaultSegmentWriter(
                    backup,
                    backup.getReader(),
                    backup.getSegmentIdProvider(),
                    backup.getBlobStore(),
                    new WriterCacheManager.Default(),
                    bufferWriter
            );
            Compactor compactor = new Compactor(
                    backup.getReader(),
                    writer,
                    backup.getBlobStore(),
                    Suppliers.ofInstance(false),
                    GCNodeWriteMonitor.EMPTY
            );
            SegmentNodeState head = backup.getHead();
            SegmentNodeState after = compactor.compact(head, current, head);
            writer.flush();

            if (after != null) {
                backup.getRevisions().setHead(head.getRecordId(), after.getRecordId());
            }
        } finally {
            backup.close();
        }

        backup = fileStoreBuilder(destination)
                .withDefaultMemoryMapping()
                .withGCOptions(gcOptions)
                .withStrictVersionCheck(true)
                .build();

        try {
            cleanup(backup);
        } finally {
            backup.close();
        }

        watch.stop();
        log.info("Backup finished in {}.", watch);
    }

    @Override
    public boolean cleanup(FileStore f) throws IOException {
        f.cleanup();
        return true;
    }

}
