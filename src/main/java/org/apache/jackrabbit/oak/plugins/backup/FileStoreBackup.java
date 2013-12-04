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
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.segment.Journal;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreBackup {

    private static final Logger log = LoggerFactory
            .getLogger(FileStoreBackup.class);

    private static final long DEFAULT_LIFETIME = TimeUnit.HOURS.toMillis(1);

    static int CACHE_SIZE = 256;

    public static void backup(NodeStore store, File destination)
            throws IOException {
        long s = System.currentTimeMillis();

        // 1. create a new checkpoint with the current state
        String checkpoint = store.checkpoint(DEFAULT_LIFETIME);
        NodeState current = store.retrieve(checkpoint);
        if (current == null) {
            log.debug("Unable to retrieve checkpoint {}", checkpoint);
            return;
        }

        // 2. init filestore
        destination.mkdirs();
        FileStore backup = null;
        try {
            backup = new FileStore(destination, current, CACHE_SIZE,
                    CACHE_SIZE, false);

            // TODO optimize incremental backup
            Journal root = backup.getJournal("root");
            SegmentNodeState state = new SegmentNodeState(backup.getWriter()
                    .getDummySegment(), root.getHead());
            SegmentNodeBuilder builder = state.builder();
            current.compareAgainstBaseState(state,
                    new ApplyDiff(builder.child("root")));
            root.setHead(state.getRecordId(), builder.getNodeState()
                    .getRecordId());

        } finally {
            if (backup != null) {
                backup.close();
            }
            log.debug("Backup done in {} ms.", System.currentTimeMillis() - s);
        }
    }
}
