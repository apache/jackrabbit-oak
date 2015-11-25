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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.Compactor;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreRestore {

    private static final Logger log = LoggerFactory
            .getLogger(FileStoreRestore.class);

    static int MAX_FILE_SIZE = 256;

    private static final String JOURNAL_FILE_NAME = "journal.log";

    public static void restore(File source, NodeStore store)
            throws IOException, CommitFailedException {
        // 1. verify that this is an actual filestore
        if (!validFileStore(source)) {
            throw new IOException("Folder " + source
                    + " is not a valid FileStore directory");
        }

        // 2. init filestore
        FileStore restore = new FileStore(source, MAX_FILE_SIZE, false);
        try {
            SegmentNodeState state = restore.getHead();
            restore(state.getChildNode("root"), store, restore);
        } finally {
            restore.close();
        }
    }

    private static void restore(NodeState source, NodeStore store,
            SegmentStore restore) throws CommitFailedException {
        long s = System.currentTimeMillis();
        NodeState current = store.getRoot();
        RestoreCompactor compactor = new RestoreCompactor(restore);
        SegmentNodeBuilder builder = compactor.process(current, source, current);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        log.debug("Restore finished in {} ms.", System.currentTimeMillis() - s);
    }

    private static class RestoreCompactor extends Compactor {

        public RestoreCompactor(SegmentStore store) {
            super(store);
        }

        @Override
        protected SegmentNodeBuilder process(NodeState before, NodeState after, NodeState onto) {
            return super.process(before, after, onto);
        }
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
