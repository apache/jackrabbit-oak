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

import com.arakelian.docker.junit.DockerRule;
import com.arakelian.docker.junit.model.ImmutableDockerConfig;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.auth.FixedRegistryAuthSupplier;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import org.jetbrains.annotations.NotNull;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class AzuriteDockerRule implements TestRule {

    private static final String IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.15.0";
    
    public static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    public static final String ACCOUNT_NAME = "devstoreaccount1";

    private final DockerRule wrappedRule;

    public AzuriteDockerRule() {
        wrappedRule = new DockerRule(ImmutableDockerConfig.builder()
            .image(IMAGE)
            .name("oak-test-azurite")
            .ports("10000")
            .addStartedListener(container -> {
                container.waitForPort("10000/tcp");
                container.waitForLog("Azurite Blob service is successfully listening at http://0.0.0.0:10000");
            })
            .addContainerConfigurer(builder -> builder.env("executable=blob"))
            .alwaysRemoveContainer(true)
            .build());
    }

    public CloudBlobContainer getContainer(String name) throws URISyntaxException, StorageException, InvalidKeyException {
        CloudStorageAccount cloud = getCloudStorageAccount();
        CloudBlobClient cloudBlobClient = cloud.createCloudBlobClient();
        CloudBlobContainer container = cloudBlobClient.getContainerReference(name);
        container.deleteIfExists();
        container.create();
        return container;
    }

    public CloudStorageAccount getCloudStorageAccount() throws URISyntaxException, InvalidKeyException {
        String blobEndpoint = "BlobEndpoint=" + getBlobEndpoint();
        String accountName = "AccountName=" + ACCOUNT_NAME;
        String accountKey = "AccountKey=" + ACCOUNT_KEY;
        return CloudStorageAccount.parse("DefaultEndpointsProtocol=http;" + ";" + accountName + ";" + accountKey + ";" + blobEndpoint);
    }

    @NotNull
    public String getBlobEndpoint() {
        int mappedPort = getMappedPort();
        return "http://127.0.0.1:" + mappedPort + "/devstoreaccount1";
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        try {
            DefaultDockerClient client = DefaultDockerClient.fromEnv()
                .connectTimeoutMillis(5000L)
                .readTimeoutMillis(20000L)
                .registryAuthSupplier(new FixedRegistryAuthSupplier())
                .build();
            client.ping();
            client.pull(IMAGE);
            client.close();
        } catch (Throwable t) {
            Assume.assumeNoException(t);
        }

        return wrappedRule.apply(statement, description);
    }

    public int getMappedPort() {
        return wrappedRule.getContainer().getPortBinding("10000/tcp").getPort();
    }
}
