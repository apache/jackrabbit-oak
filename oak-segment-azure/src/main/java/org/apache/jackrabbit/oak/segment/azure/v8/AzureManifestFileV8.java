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
package org.apache.jackrabbit.oak.segment.azure.v8;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

public class AzureManifestFileV8 implements ManifestFile {

    private static final Logger log = LoggerFactory.getLogger(AzureManifestFileV8.class);

    private final CloudBlockBlob manifestBlob;

    public AzureManifestFileV8(CloudBlockBlob manifestBlob) {
        this.manifestBlob = manifestBlob;
    }

    @Override
    public boolean exists() {
        try {
            return manifestBlob.exists();
        } catch (StorageException e) {
            log.error("Can't check if the manifest exists", e);
            return false;
        }
    }

    @Override
    public Properties load() throws IOException {
        Properties properties = new Properties();
        if (exists()) {
            long length = manifestBlob.getProperties().getLength();
            byte[] data = new byte[(int) length];
            try {
                manifestBlob.downloadToByteArray(data, 0);
            } catch (StorageException e) {
                throw new IOException(e);
            }
            properties.load(new ByteArrayInputStream(data));
        }
        return properties;
    }

    @Override
    public void save(Properties properties) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        properties.store(bos, null);

        byte[] data = bos.toByteArray();
        try {
            manifestBlob.uploadFromByteArray(data, 0, data.length);
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }
}
