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
        this.directory = directory == null ? createTempTargetDir("repo-segment") : directory;
    }

    private static File createTempTargetDir(String prefix) throws IOException {
        File f = File.createTempFile(prefix, "", new File("target"));
        f.delete();
        f.mkdir();
        return f;
    }

    @Override
    public NodeStore open() throws IOException {
        directory.mkdirs();
        FileStore.Builder builder = FileStore.newFileStore(new File(directory, "segmentstore"));
        if (blob != null) {
            builder.withBlobStore(blob.open());
        }
        fs = builder.create();
        return new SegmentNodeStore(fs);
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
        FileUtils.deleteQuietly(directory);
        if (blob != null) {
            blob.clean();
        }
    }

    @Override
    public String getDescription() {
        return directory.getPath();
    }

    public File getDirectory() {
        return directory;
    }
}
