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

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.segment.Compactor;
import org.apache.jackrabbit.oak.segment.SegmentBufferWriter;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.WriterCacheManager;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;

public class FileStoreRestore {

    private static final Logger log = LoggerFactory
            .getLogger(FileStoreRestore.class);

    static int MAX_FILE_SIZE = 256;

    private static final String JOURNAL_FILE_NAME = "journal.log";

    public static void restore(File source, File destination)
            throws IOException {
        if (!validFileStore(source)) {
            throw new IOException("Folder " + source
                    + " is not a valid FileStore directory");
        }

        FileStore restore = fileStoreBuilder(source).buildReadOnly();
        Stopwatch watch = Stopwatch.createStarted();

        FileStore store = fileStoreBuilder(destination).build();
        SegmentNodeState current = store.getReader().readHeadState();
        try {
            SegmentNodeState head = restore.getReader().readHeadState();
            int gen = head.getRecordId().getSegment().getGcGeneration();
            SegmentBufferWriter bufferWriter = new SegmentBufferWriter(store,
                    store.getTracker(), store.getReader(), "r", gen);
            SegmentWriter writer = new SegmentWriter(store, store.getReader(),
                    store.getBlobStore(), new WriterCacheManager.Default(),
                    bufferWriter);
            SegmentGCOptions gcOptions = defaultGCOptions().setOffline();
            Compactor compactor = new Compactor(store.getReader(), writer,
                    store.getBlobStore(), Suppliers.ofInstance(false),
                    gcOptions);
            compactor.setContentEqualityCheck(true);
            SegmentNodeState after = compactor.compact(current, head, current);
            store.getRevisions().setHead(current.getRecordId(),
                    after.getRecordId());
        } finally {
            restore.close();
            store.close();
        }
        watch.stop();
        log.info("Restore finished in {}.", watch);
    }

    public static void restore(File source, NodeStore store) {
        log.warn("Restore not available as an online operation.");
    }

    private static boolean validFileStore(File source) {
        if (source == null || !source.isDirectory()) {
            return false;
        }
        for (String f : source.list()) {
            if (JOURNAL_FILE_NAME.equals(f)) {
                return true;
            }
        }
        return false;
    }
}
