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
import org.apache.jackrabbit.oak.upgrade.cli.blob.S3DataStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import static org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer.deleteRecursive;

public class S3DataStoreContainer implements BlobStoreContainer {

    private static final Logger log = LoggerFactory.getLogger(S3DataStoreContainer.class);

    private final File directory;

    private final S3DataStoreFactory factory;

    private final Closer closer;

    public S3DataStoreContainer(String configFile) throws IOException {
        this.directory = Files.createTempDirectory(Paths.get("target"), "repo-s3").toFile();
        this.factory = new S3DataStoreFactory(configFile, directory.getPath(), false);
        this.closer = Closer.create();
    }

    @Override
    public BlobStore open() throws IOException {
        return factory.create(closer);
    }

    @Override
    public void close() {
        try {
            closer.close();
        } catch (IOException e) {
            log.error("Can't close store", e);
        }
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