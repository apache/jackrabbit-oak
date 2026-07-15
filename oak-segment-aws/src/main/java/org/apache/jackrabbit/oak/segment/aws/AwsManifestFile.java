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
package org.apache.jackrabbit.oak.segment.aws;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Properties;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3Object;

import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;

public class AwsManifestFile implements ManifestFile {

    private final S3Directory directory;
    private final String manifestFile;

    public AwsManifestFile(S3Directory directory, String manifestFile) throws IOException {
        this.directory = directory;
        this.manifestFile = manifestFile;
    }

    @Override
    public boolean exists() {
        return directory.doesObjectExist(manifestFile);
    }

    @Override
    public Properties load() throws IOException {
        Properties properties = new Properties();
        if (this.exists()) {
            try (S3Object object = directory.getObject(manifestFile)) {
                properties.load(object.getObjectContent());
            } catch (AmazonServiceException e) {
                throw new IOException(e);
            }
        }
        return properties;
    }

    @Override
    public void save(Properties properties) throws IOException {
        try (PipedInputStream input = new PipedInputStream()) {
            try (PipedOutputStream src = new PipedOutputStream(input)) {
                properties.store(src, null);
            }
            directory.putObject(manifestFile, input);
        }
    }
}
