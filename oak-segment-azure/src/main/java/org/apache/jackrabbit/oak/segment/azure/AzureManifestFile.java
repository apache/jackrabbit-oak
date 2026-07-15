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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

public class AzureManifestFile implements ManifestFile {

    private static final Logger log = LoggerFactory.getLogger(AzureManifestFile.class);

    private final BlockBlobClient manifestBlob;

    public AzureManifestFile(BlockBlobClient manifestBlob) {
        this.manifestBlob = manifestBlob;
    }

    @Override
    public boolean exists() {
        try {
            return manifestBlob.exists();
        } catch (BlobStorageException e) {
            log.error("Can't check if the manifest exists", e);
            return false;
        }
    }

    @Override
    public Properties load() throws IOException {
        Properties properties = new Properties();
        if (exists()) {
            try {
                byte[] data = manifestBlob.downloadContent().toBytes();
                properties.load(new ByteArrayInputStream(data));
            } catch (BlobStorageException e) {
                throw new IOException(e);
            }
        }
        return properties;
    }

    @Override
    public void save(Properties properties) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        properties.store(bos, null);

        byte[] data = bos.toByteArray();
        try {
            manifestBlob.upload(BinaryData.fromBytes(data), true);
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }
}
