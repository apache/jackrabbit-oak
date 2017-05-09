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

package org.apache.jackrabbit.oak.plugins.tika;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

class SegmentTarUtils {

    private SegmentTarUtils() {
        // Prevent instantiation
    }

    static NodeStore bootstrap(String path, BlobStore store, Closer closer) throws IOException {
        try {
            return SegmentNodeStoreBuilders.builder(fileStore(path, store, closer)).build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    private static FileStore fileStore(String path, BlobStore store, Closer closer) throws IOException, InvalidFileStoreVersionException {
        return closer.register(fileStore(path, store));
    }

    private static FileStore fileStore(String path, BlobStore store) throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(new File(path)).withBlobStore(store).build();
    }

}
