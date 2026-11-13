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

import com.azure.storage.blob.BlobContainerClient;

import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.SegmentStoreMigratorTestBase;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.junit.ClassRule;

public class SegmentStoreMigratorTest extends SegmentStoreMigratorTestBase {

    private static final String CONTAINER_NAME = "oak-test";

    private static final String ROOT_PREFIX = "oak";

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient readBlobContainerClient;

    @Override
    protected SegmentNodeStorePersistence createRemotePersistence() {
        readBlobContainerClient = azurite.getReadBlobContainerClient(CONTAINER_NAME);
        return new AzurePersistence(readBlobContainerClient,
                azurite.getWriteBlobContainerClient(CONTAINER_NAME),
                azurite.getNoRetryBlobContainerClient(CONTAINER_NAME),
                ROOT_PREFIX);
    }

    @Override
    protected void deleteRemoteArchiveFile(String archive, String name) {
        readBlobContainerClient.getBlobClient(ROOT_PREFIX + "/" + archive + "/" + name).deleteIfExists();
    }

    @Override
    protected void migrate(SegmentNodeStorePersistence source, SegmentNodeStorePersistence target) throws Exception {
        try (SegmentStoreMigrator migrator = new SegmentStoreMigrator.Builder()
                .withSourcePersistence(source, "source")
                .withTargetPersistence(target, "target")
                .build()) {
            migrator.migrate();
        }
    }
}
