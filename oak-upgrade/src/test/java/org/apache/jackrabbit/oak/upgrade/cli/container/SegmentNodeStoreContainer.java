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
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer.deleteRecursive;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments.SEGMENT_OLD_PREFIX;

public class SegmentNodeStoreContainer implements NodeStoreContainer {

    private final File directory;

    private final BlobStoreContainer blob;

    private FileStore fs;

    public SegmentNodeStoreContainer() throws IOException {
        this(null, null);
    }

    public SegmentNodeStoreContainer(File directory) throws IOException {
        this(null, directory);
    }

    public SegmentNodeStoreContainer(BlobStoreContainer blob) throws IOException {
        this(blob, null);
    }

    private SegmentNodeStoreContainer(BlobStoreContainer blob, File directory) throws IOException {
        this.blob = blob;
        this.directory = directory == null ? Files.createTempDirectory(Paths.get("target"), "repo-segment").toFile() : directory;
    }
    @Override
    public NodeStore open() throws IOException {
        directory.mkdirs();
        FileStore.Builder builder = FileStore.builder(new File(directory, "segmentstore"));
        if (blob != null) {
            builder.withBlobStore(blob.open());
        }

        try {
            fs = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }

        return SegmentNodeStore.builder(fs).build();
    }

    @Override
    public void close() {
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

    @Override
    public void clean() throws IOException {
        deleteRecursive(directory);
        if (blob != null) {
            blob.clean();
        }
    }

    @Override
    public String getDescription() {
        return SEGMENT_OLD_PREFIX + directory.getPath();
    }

    public File getDirectory() {
        return directory;
    }

}
