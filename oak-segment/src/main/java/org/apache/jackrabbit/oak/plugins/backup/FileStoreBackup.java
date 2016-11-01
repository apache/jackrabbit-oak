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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.segment.Compactor;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.plugins.segment.file.tooling.BasicReadOnlyBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

@Deprecated
public class FileStoreBackup {

    private static final Logger log = LoggerFactory
            .getLogger(FileStoreBackup.class);

    @Deprecated
    public static boolean USE_FAKE_BLOBSTORE = Boolean.getBoolean("oak.backup.UseFakeBlobStore");

    @Deprecated
    public static void backup(NodeStore store, File destination) throws IOException, InvalidFileStoreVersionException {
        checkArgument(store instanceof SegmentNodeStore);
        Stopwatch watch = Stopwatch.createStarted();
        NodeState current = ((SegmentNodeStore) store).getSuperRoot();
        FileStore.Builder builder = FileStore.builder(destination)
                .withDefaultMemoryMapping();
        if (USE_FAKE_BLOBSTORE) {
            builder.withBlobStore(new BasicReadOnlyBlobStore());
        }
        FileStore backup = builder.build();
        try {
            SegmentNodeState state = backup.getHead();
            Compactor compactor = new Compactor(backup.getTracker());
            compactor.setDeepCheckLargeBinaries(true);
            compactor.setContentEqualityCheck(true);
            SegmentNodeState after = compactor.compact(state, current, state);
            backup.setHead(state, after);
        } finally {
            backup.close();
        }
        watch.stop();
        log.info("Backup finished in {}.", watch);
    }
}
