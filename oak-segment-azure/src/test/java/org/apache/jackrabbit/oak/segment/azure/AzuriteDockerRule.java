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

import com.arakelian.docker.junit.DockerRule;
import com.arakelian.docker.junit.model.ImmutableDockerConfig;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.auth.FixedRegistryAuthSupplier;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobContainer;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class AzuriteDockerRule implements TestRule {

    private static final String IMAGE = "trekawek/azurite";
    private static final boolean USE_REAL_AZURE_CONNECTION = false;

    private final DockerRule wrappedRule;

    public AzuriteDockerRule() {
        wrappedRule = new DockerRule(ImmutableDockerConfig.builder()
                .image(IMAGE)
                .name("oak-test-azurite")
                .ports("10000")
                .addStartedListener(container -> {
                    container.waitForPort("10000/tcp");
                    container.waitForLog("Azure Blob Storage Emulator listening on port 10000");
                })
                .addContainerConfigurer(builder -> builder.env("executable=blob"))
                .alwaysRemoveContainer(true)
                .build());
    }

    public CloudBlobContainer getContainer(String name) {
        return getContainer(name, null);
    }

    public CloudBlobContainer getContainer(String name, HttpPipelinePolicy pipelinePolicy) {
        // Creating a unique container ID for each run because there were problems with the container.delete() command.
        String containerName = name + System.currentTimeMillis();

        String connectionString = USE_REAL_AZURE_CONNECTION
                ? System.getenv("AZURE_CONNECTION")
                : "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:" + getMappedPort() + "/devstoreaccount1;";

        BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder()
                .connectionString(connectionString);
        if (pipelinePolicy != null) {
            blobServiceClientBuilder.addPolicy(pipelinePolicy);
        }

        BlobContainerClient container = blobServiceClientBuilder
                .buildClient()
                .getBlobContainerClient(containerName);
        container.create();
        return CloudBlobContainer.withContainerClient(container, containerName);
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
