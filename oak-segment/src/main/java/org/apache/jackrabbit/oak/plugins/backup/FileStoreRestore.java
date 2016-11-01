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
package org.apache.jackrabbit.oak.plugins.backup;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.plugins.segment.Compactor;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class FileStoreRestore {

    private static final Logger log = LoggerFactory
            .getLogger(FileStoreRestore.class);

    static int MAX_FILE_SIZE = 256;

    private static final String JOURNAL_FILE_NAME = "journal.log";

    @Deprecated
    public static void restore(File source, File destination) throws IOException, InvalidFileStoreVersionException {
        if (!validFileStore(source)) {
            throw new IOException("Folder " + source
                    + " is not a valid FileStore directory");
        }

        FileStore restore = FileStore.builder(source).buildReadOnly();
        Stopwatch watch = Stopwatch.createStarted();

        FileStore store = FileStore.builder(destination).build();
        SegmentNodeState current = store.getHead();
        try {
            Compactor compactor = new Compactor(store.getTracker());
            compactor.setDeepCheckLargeBinaries(true);
            SegmentNodeState after = compactor.compact(current,
                    restore.getHead(), current);
            store.setHead(current, after);
        } finally {
            restore.close();
            store.close();
        }
        watch.stop();
        log.info("Restore finished in {}.", watch);
    }

    @Deprecated
    public static void restore(File source, NodeStore store) {
        log.warn("Restore not available as an online operation.");
    }

    @Deprecated
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
