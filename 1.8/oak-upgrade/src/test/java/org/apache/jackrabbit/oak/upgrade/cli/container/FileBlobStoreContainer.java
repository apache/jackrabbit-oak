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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;

import static org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer.deleteRecursive;

public class FileBlobStoreContainer implements BlobStoreContainer {

    private final File directory;

    public FileBlobStoreContainer() throws IOException {
        this.directory = Files.createTempDirectory(Paths.get("target"), "repo-fbs").toFile();
    }

    @Override
    public BlobStore open() throws IOException {
        return new FileBlobStore(directory.getPath());
    }

    @Override
    public void close() {
    }

    @Override
    public void clean() throws IOException {
        deleteRecursive(directory);
    }

    @Override
    public String getDescription() {
        return directory.getPath();
    }

}
