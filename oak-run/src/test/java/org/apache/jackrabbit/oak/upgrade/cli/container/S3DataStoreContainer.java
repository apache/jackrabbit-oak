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
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.S3DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class S3DataStoreContainer implements BlobStoreContainer {

    private static final Logger log = LoggerFactory.getLogger(S3DataStoreContainer.class);

    private final String configFile;

    private final File homeDir;

    private S3DataStore s3DataStore;

    public S3DataStoreContainer(String configFile) {
        this.configFile = configFile;
        this.homeDir = Files.createTempDir();
    }

    @Override
    public BlobStore open() throws IOException {
        Properties props = new Properties();
        FileReader reader = new FileReader(new File(configFile));
        try {
            props.load(reader);
        } finally {
            IOUtils.closeQuietly(reader);
        }
        props.setProperty("path", new File(homeDir, "repository/datastore").getPath());

        S3DataStore delegate = new S3DataStore();
        delegate.setProperties(props);
        try {
            delegate.init(homeDir.getPath());
        } catch (RepositoryException e) {
            throw new IOException(e);
        }
        return new DataStoreBlobStore(delegate);
    }

    @Override
    public void close() {
        try {
            s3DataStore.close();
        } catch (DataStoreException e) {
            log.error("Can't close s3datastore", e);
        }
    }

    @Override
    public void clean() throws IOException {
        FileUtils.deleteDirectory(homeDir);
    }

    @Override
    public String getDescription() {
        return homeDir.getPath();
    }

}