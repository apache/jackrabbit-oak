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
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.spotify.docker.client.DefaultDockerClient;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzuriteDockerRule implements TestRule {

    private final DockerRule wrappedRule;

    public AzuriteDockerRule() {
        wrappedRule = new DockerRule(ImmutableDockerConfig.builder()
                .image("trekawek/azurite")
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

    public CloudBlobContainer getContainer(String name) throws URISyntaxException, StorageException, InvalidKeyException {
        int mappedPort = wrappedRule.getContainer().getPortBinding("10000/tcp").getPort();
        CloudStorageAccount cloud = CloudStorageAccount.parse("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:" + mappedPort + "/devstoreaccount1;");
        CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(name);
        container.deleteIfExists();
        container.create();
        return container;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        try {
            DefaultDockerClient client = DefaultDockerClient.fromEnv().connectTimeoutMillis(5000L).readTimeoutMillis(20000L).build();
            client.ping();
            client.close();
        } catch (Exception e) {
            Assume.assumeNoException(e);
        }

        return wrappedRule.apply(statement, description);
    }
}
