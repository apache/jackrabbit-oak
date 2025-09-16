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
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.jackrabbit.oak.segment.remote.RemoteUtilities;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AzureUtilitiesTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient blobContainerClient;
    private String archivePrefix = "oak/data00000a.tar/";
    private String archiveName = "data00000a.tar";

    @Before
    public void setup() throws BlobStorageException {
        blobContainerClient = azurite.getReadBlobContainerClient("oak-test");
    }

    @Test
    public void testArchiveExistsWhenArchiveHasBlobs() {
        blobContainerClient.getBlobClient(archivePrefix + RemoteUtilities.getSegmentFileName(0, 0, 0)).getBlockBlobClient()
                .upload(BinaryData.fromString(""));

        assertTrue("Archive should exist when it contains segment blob",
                AzureUtilities.archiveExists(blobContainerClient, archivePrefix));
    }

    @Test
    public void testArchiveExistsWhenArchiveIsEmpty() {

        assertFalse("Archive should not exist when no   blobs are present",
                AzureUtilities.archiveExists(blobContainerClient, archivePrefix));
    }

    @Test
    public void testArchiveExistsWithArchiveMetadata() {
        blobContainerClient.getBlobClient(archivePrefix + archiveName + ".brf").getBlockBlobClient()
                .upload(BinaryData.fromString(""));
        blobContainerClient.getBlobClient(archivePrefix + archiveName + ".gph").getBlockBlobClient()
                .upload(BinaryData.fromString(""));

        assertTrue("Archive should exist when it contains metadata",
                AzureUtilities.archiveExists(blobContainerClient, archivePrefix));
    }

    @Test
    public void testArchiveExistsWithArchiveClosedMarker() {
        blobContainerClient.getBlobClient(archivePrefix + "closed").getBlockBlobClient()
                .upload(BinaryData.fromString(""));

        assertTrue("Archive should exist when it contains closed marker",
                AzureUtilities.archiveExists(blobContainerClient, archivePrefix));
    }
}
