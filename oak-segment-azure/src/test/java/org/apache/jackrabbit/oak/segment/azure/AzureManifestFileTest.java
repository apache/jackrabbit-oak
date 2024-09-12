/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AzureManifestFileTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient readBlobContainerClient;
    private BlobContainerClient writeBlobContainerClient;

    @Before
    public void setup() throws BlobStorageException, InvalidKeyException, URISyntaxException {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");
    }

    @Test
    public void testManifest() throws IOException {
        ManifestFile manifestFile = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, "oak").getManifestFile();
        assertFalse(manifestFile.exists());

        Properties props = new Properties();
        props.setProperty("xyz", "abc");
        props.setProperty("version", "123");
        manifestFile.save(props);

        Properties loaded = manifestFile.load();
        assertEquals(props, loaded);
    }

}
