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

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.backup.FileStoreRestore;
import org.apache.jackrabbit.oak.segment.DefaultSegmentWriter;
import org.apache.jackrabbit.oak.segment.Compactor;
import org.apache.jackrabbit.oak.segment.SegmentBufferWriter;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.WriterCacheManager;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreRestoreImpl implements FileStoreRestore {

    private static final Logger log = LoggerFactory.getLogger(FileStoreRestoreImpl.class);

    private static final String JOURNAL_FILE_NAME = "journal.log";

    @Override
    public void restore(File source, File destination) throws IOException, InvalidFileStoreVersionException {
        if (!validFileStore(source)) {
            throw new IOException("Folder " + source + " is not a valid FileStore directory");
        }

        ReadOnlyFileStore restore = fileStoreBuilder(source).buildReadOnly();
        Stopwatch watch = Stopwatch.createStarted();

        FileStore store = fileStoreBuilder(destination)
            .withStrictVersionCheck(true)
            .build();
        SegmentNodeState current = store.getHead();

        try {
            SegmentNodeState head = restore.getHead();
            GCGeneration gen = head.getRecordId().getSegmentId().getGcGeneration();
            SegmentBufferWriter bufferWriter = new SegmentBufferWriter(
                    store.getSegmentIdProvider(),
                    store.getReader(),
                    "r",
                    gen
            );
            SegmentWriter writer = new DefaultSegmentWriter(
                    store,
                    store.getReader(),
                    store.getSegmentIdProvider(),
                    store.getBlobStore(),
                    new WriterCacheManager.Default(),
                    bufferWriter
            );
            SegmentGCOptions gcOptions = defaultGCOptions().setOffline();
            Compactor compactor = new Compactor(
                    store.getReader(),
                    writer,
                    store.getBlobStore(),
                    Suppliers.ofInstance(false),
                    GCNodeWriteMonitor.EMPTY
            );
            SegmentNodeState after = compactor.compact(current, head, current);
            writer.flush();
            store.getRevisions().setHead(current.getRecordId(), after.getRecordId());
        } finally {
            restore.close();
            store.close();
        }

        watch.stop();
        log.info("Restore finished in {}.", watch);
    }

    @Override
    public void restore(File source) {
        log.warn("Restore not available as an online operation.");
    }

    private static boolean validFileStore(File source) {
        if (source == null || !source.isDirectory()) {
            return false;
        }

        String[] children = source.list();

        if (children == null) {
            return false;
        }

        for (String f : children) {
            if (JOURNAL_FILE_NAME.equals(f)) {
                return true;
            }
        }

        return false;
    }

}
