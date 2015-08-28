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
package org.apache.jackrabbit.oak.upgrade.cli.container;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.Files;

public class SegmentNodeStoreContainer implements NodeStoreContainer {

    private final File directory;

    private final BlobStoreContainer blob;

    private FileStore fs;

    public SegmentNodeStoreContainer() {
        this(Files.createTempDir());
    }

    public SegmentNodeStoreContainer(File directory) {
        this.blob = null;
        this.directory = directory;
    }

    public SegmentNodeStoreContainer(BlobStoreContainer blob) {
        this.blob = blob;
        this.directory = Files.createTempDir();
    }

    @Override
    public NodeStore open() throws IOException {
        FileStore.Builder builder = FileStore.newFileStore(new File(directory, "segmentstore"));
        if (blob != null) {
            builder.withBlobStore(blob.open());
        }
        fs = builder.create();
        return SegmentNodeStore.newSegmentNodeStore(fs).create();
    }

    @Override
    public void close() {
        fs.close();
    }

    @Override
    public void clean() throws IOException {
        FileUtils.deleteDirectory(directory);
        if (blob != null) {
            blob.clean();
        }
    }

    @Override
    public String getDescription() {
        return directory.getPath();
    }

}
