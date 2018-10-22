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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.rmi.server.ExportException;
import java.security.InvalidKeyException;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.fail;

public class AzureRepositoryLockTest {

    private static final Logger log = LoggerFactory.getLogger(AzureRepositoryLockTest.class);

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
    }

    @Test
    public void testFailingLock() throws URISyntaxException, IOException, StorageException {
        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");
        new AzureRepositoryLock(blob, () -> {}, 0).lock();
        try {
            new AzureRepositoryLock(blob, () -> {}, 0).lock();
            fail("The second lock should fail.");
        } catch (IOException e) {
            // it's fine
        }
    }

    @Test
    public void testWaitingLock() throws URISyntaxException, IOException, StorageException, InterruptedException {
        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");
        Semaphore s = new Semaphore(0);
        new Thread(() -> {
            try {
                RepositoryLock lock = new AzureRepositoryLock(blob, () -> {}, 0).lock();
                s.release();
                Thread.sleep(1000);
                lock.unlock();
            } catch (Exception e) {
                log.error("Can't lock or unlock the repo", e);
            }
        }).start();

        s.acquire();
        new AzureRepositoryLock(blob, () -> {}, 10).lock();
    }

}
