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

import static java.io.File.createTempFile;
import static org.apache.commons.io.FileUtils.deleteDirectory;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class FileStoreTest {

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = createTempFile(FileStoreTest.class.getSimpleName(), "dir", new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @After
    public void tearDown() throws IOException {
        deleteDirectory(directory);
    }

    @Ignore("OAK-4054")  // FIXME OAK-4054
    @Test
    public void containsSegment() throws IOException {
        FileStore fileStore = FileStore.builder(directory).build();
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
