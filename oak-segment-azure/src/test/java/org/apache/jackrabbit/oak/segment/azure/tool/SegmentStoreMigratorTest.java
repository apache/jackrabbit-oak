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
package org.apache.jackrabbit.oak.segment.azure.tool;

import java.io.File;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;

import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.SegmentStoreMigratorTestBase;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.junit.ClassRule;

public class SegmentStoreMigratorTest extends SegmentStoreMigratorTestBase {

    private static final String ROOT_PREFIX = "oak";

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient readBlobContainerClient;

    @Override
    protected SegmentNodeStorePersistence createSourcePersistence() throws Exception {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        return new AzurePersistence(readBlobContainerClient,
                azurite.getWriteBlobContainerClient("oak-test"),
                azurite.getNoRetryBlobContainerClient("oak-test"),
                ROOT_PREFIX);
    }

    @Override
    protected void migrate(SegmentNodeStorePersistence source, File tarDir) throws Exception {
        try (SegmentStoreMigrator migrator = new SegmentStoreMigrator.Builder()
                .withSourcePersistence(source, "azure")
                .withTargetPersistence(new TarPersistence(tarDir), "tar")
                .build()) {
            migrator.migrate();
        }
    }

    @Override
    protected void deleteArchiveFile(String archive, String name) {
        archiveBlob(archive, name).deleteIfExists();
    }

    @Override
    protected boolean archiveFileExists(String archive, String name) {
        return archiveBlob(archive, name).exists();
    }

    private BlobClient archiveBlob(String archive, String name) {
        return readBlobContainerClient.getBlobClient(ROOT_PREFIX + "/" + archive + "/" + name);
    }
}
