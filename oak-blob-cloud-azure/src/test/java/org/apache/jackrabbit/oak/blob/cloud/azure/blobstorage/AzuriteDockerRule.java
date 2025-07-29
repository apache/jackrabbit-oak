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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class AzuriteDockerRule extends ExternalResource {

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.31.0");
    public static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    public static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final AtomicReference<Exception> STARTUP_EXCEPTION = new AtomicReference<>();

    private GenericContainer<?> azuriteContainer;

    @Override
    protected void before() throws Throwable {
        azuriteContainer = new GenericContainer<>(DOCKER_IMAGE_NAME)
                .withExposedPorts(10000)
                .withEnv(Map.of("executable", "blob"))
                .withStartupTimeout(Duration.ofSeconds(30));

        try {
            azuriteContainer.start();
        } catch (IllegalStateException e) {
            STARTUP_EXCEPTION.set(e);
            throw e;
        }
    }

    @Override
    protected void after() {
        if (azuriteContainer != null) {
            azuriteContainer.stop();
        }
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    before();
                } catch (IllegalStateException e) {
                    Assume.assumeNoException(STARTUP_EXCEPTION.get());
                    throw e;
                }

                List<Throwable> errors = new ArrayList<Throwable>();
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    try {
                        after();
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }
                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

    public String getBlobEndpoint() {
        return "http://127.0.0.1:" + getMappedPort() + "/devstoreaccount1";
    }

    public CloudBlobContainer getContainer(String name) throws URISyntaxException, StorageException, InvalidKeyException {
        CloudStorageAccount cloud = getCloudStorageAccount();
        CloudBlobClient cloudBlobClient = cloud.createCloudBlobClient();
        CloudBlobContainer container = cloudBlobClient.getContainerReference(name);
        container.deleteIfExists();
        container.create();
        return container;
    }

    public BlobContainerClient getContainer(String containerName, String connectionString) {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();

        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
        blobContainerClient.deleteIfExists();
        blobContainerClient.create();
        return blobContainerClient;
    }

    public CloudStorageAccount getCloudStorageAccount() throws URISyntaxException, InvalidKeyException {
        String blobEndpoint = "BlobEndpoint=" + getBlobEndpoint();
        String accountName = "AccountName=" + ACCOUNT_NAME;
        String accountKey = "AccountKey=" + ACCOUNT_KEY;
        return CloudStorageAccount.parse("DefaultEndpointsProtocol=http;" + ";" + accountName + ";" + accountKey + ";" + blobEndpoint);
    }

    public int getMappedPort() {
        return azuriteContainer.getMappedPort(10000);
    }
}
