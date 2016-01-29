/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.run;

import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.newBasicReadOnlyBlobStore;
import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.openReadOnlyFileStore;
import static org.apache.jackrabbit.oak.run.Utils.asCloseable;

import java.io.File;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.backup.FileStoreBackup;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

class BackupCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        boolean fakeBlobStore = FileStoreBackup.USE_FAKE_BLOBSTORE;
        Closer closer = Closer.create();
        try {
            FileStore fs;
            if (fakeBlobStore) {
                fs = openReadOnlyFileStore(new File(args[0]),
                        newBasicReadOnlyBlobStore());
            } else {
                fs = openReadOnlyFileStore(new File(args[0]));
            }
            closer.register(asCloseable(fs));
            NodeStore store = SegmentNodeStore.newSegmentNodeStore(fs).create();
            FileStoreBackup.backup(store, new File(args[1]));
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

}
