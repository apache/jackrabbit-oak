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
package org.apache.jackrabbit.oak.segment.azure;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;

import java.io.File;
import java.io.IOException;

public class FileStoreTestUtil {

    public static FileStore createFileStore(File folder, SegmentNodeStorePersistence persistence) throws InvalidFileStoreVersionException, IOException {
        return createFileStoreBuilder(folder, persistence).build();
    }

    public static ReadOnlyFileStore createReadOnlyFileStore(File folder, SegmentNodeStorePersistence persistence) throws InvalidFileStoreVersionException, IOException {
        return createFileStoreBuilder(folder, persistence)
                .buildReadOnly();
    }

    public static FileStoreBuilder createFileStoreBuilder(File folder, SegmentNodeStorePersistence persistence) {
        return FileStoreBuilder.fileStoreBuilder(folder)
                .withStringCacheSize(0)
                .withTemplateCacheSize(0)
                .withSegmentCacheSize(8)
                .withCustomPersistence(persistence);
    }
}
