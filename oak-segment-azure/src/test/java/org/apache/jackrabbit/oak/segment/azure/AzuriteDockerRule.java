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
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.ContainerClient;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.auth.FixedRegistryAuthSupplier;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobContainer;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;


public class AzuriteDockerRule implements TestRule {

    private static final String IMAGE = "mcr.microsoft.com/azure-storage/azurite";

    private final DockerRule wrappedRule = new DockerRule(ImmutableDockerConfig.builder()
            .image(IMAGE)
            .name("oak-test-azurite")
            .ports("10000")
            .addStartedListener(container -> {
                container.waitForPort("10000/tcp");
                container.waitForLog("Azurite Blob service successfully listens");
            })
            .addContainerConfigurer(builder -> builder.env("executable=blob"))
            .alwaysRemoveContainer(true)
            .build());

    public CloudBlobContainer getContainer(String name) {
        ContainerClient container = new BlobServiceClientBuilder()
                .connectionString("UseDevelopmentStorage=true;")
                .buildClient()
                .getContainerClient(name);
        if (container.exists()) container.delete();
        container.create();
        return CloudBlobContainer.withContainerClient(container, name);
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
