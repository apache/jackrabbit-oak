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

package org.apache.jackrabbit.oak.plugins.segment.file;

import java.io.File;

import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Ignore("OAK-4054")  // FIXME OAK-4054
    @Test
    public void containsSegment() throws Exception {
        FileStore fileStore = FileStore.builder(getFileStoreFolder()).build();
        try {
            SegmentId id = new SegmentId(fileStore.getTracker(), 0, 0);
            if (fileStore.containsSegment(id)) {
                fileStore.readSegment(id);
            }
        } finally {
            fileStore.close();
        }
    }

}
