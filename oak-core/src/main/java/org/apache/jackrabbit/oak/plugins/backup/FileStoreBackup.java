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

    static int MAX_FILE_SIZE = 256;

    public static void backup(NodeStore store, File destination)
            throws IOException {
        long s = System.currentTimeMillis();

        // 1. create a new checkpoint with the current state
        String checkpoint = store.checkpoint(DEFAULT_LIFETIME);
        NodeState current = store.retrieve(checkpoint);
        if (current == null) {
            // unable to retrieve the checkpoint; use root state instead
            current = store.getRoot();
        }

        // 2. init filestore
        FileStore backup = new FileStore(destination, MAX_FILE_SIZE, false);
        try {
            SegmentNodeState state = backup.getHead();
            SegmentNodeBuilder builder = state.builder();

            String beforeCheckpoint = state.getString("checkpoint");
            if (beforeCheckpoint == null) {
                // 3.1 no stored checkpoint, so do the initial full backup
                builder.setChildNode("root", current);
            } else {
                // 3.2 try to retrieve the previously backed up checkpoint
                NodeState before = store.retrieve(beforeCheckpoint);
                if (before == null) {
                    // the previous checkpoint is no longer available,
                    // so use the backed up state as the basis of the
                    // incremental backup diff
                    before = state.getChildNode("root");
                }
                current.compareAgainstBaseState(
                        before, new ApplyDiff(builder.child("root")));
            }
            builder.setProperty("checkpoint", checkpoint);

            // 4. commit the backup
            backup.setHead(state, builder.getNodeState());
        } finally {
            backup.close();
        }

        log.debug("Backup finished in {} ms.", System.currentTimeMillis() - s);
    }
}
