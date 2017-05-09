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

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentTarNodeStoreContainer implements NodeStoreContainer {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentTarNodeStoreContainer.class);

    private final File directory;

    private final BlobStoreContainer blob;

    private FileStore fs;

    public SegmentTarNodeStoreContainer() throws IOException {
        this(null, null);
    }

    public SegmentTarNodeStoreContainer(File directory) throws IOException {
        this(null, directory);
    }

    public SegmentTarNodeStoreContainer(BlobStoreContainer blob) throws IOException {
        this(blob, null);
    }

    private SegmentTarNodeStoreContainer(BlobStoreContainer blob, File directory) throws IOException {
        this.blob = blob;
        this.directory = directory == null ? Files.createTempDirectory(Paths.get("target"), "repo-segment-tar").toFile() : directory;
    }

    @Override
    public NodeStore open() throws IOException {
        directory.mkdirs();
        FileStoreBuilder builder = fileStoreBuilder(new File(directory, "segmentstore"));
        if (blob != null) {
            builder.withBlobStore(blob.open());
        }
        try {
            fs = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        return SegmentNodeStoreBuilders.builder(fs).build();
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
        return directory.getPath();
    }

    public File getDirectory() {
        return directory;
    }

    public static void deleteRecursive(File directory) {
        if (!directory.exists()) {
            return;
        }
        try {
            Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch(IOException e) {
            LOG.error("Can't remove directory " + directory, e);
        }
    }
}
