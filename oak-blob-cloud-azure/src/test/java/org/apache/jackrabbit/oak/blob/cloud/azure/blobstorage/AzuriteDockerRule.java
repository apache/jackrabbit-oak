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

import com.arakelian.docker.junit.Container;
import com.arakelian.docker.junit.DockerRule;
import com.arakelian.docker.junit.model.ImmutableDockerConfig;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import repackaged.com.arakelian.docker.junit.com.github.dockerjava.api.DockerClient;
import repackaged.com.arakelian.docker.junit.com.github.dockerjava.api.command.PullImageResultCallback;
import repackaged.com.arakelian.docker.junit.com.github.dockerjava.api.model.ExposedPort;
import repackaged.com.arakelian.docker.junit.com.github.dockerjava.api.model.PortBinding;
import repackaged.com.arakelian.docker.junit.com.github.dockerjava.api.model.Ports;
import repackaged.com.arakelian.docker.junit.com.github.dockerjava.core.AbstractDockerCmdExecFactory;
import repackaged.com.arakelian.docker.junit.com.github.dockerjava.core.DefaultDockerClientConfig;
import repackaged.com.arakelian.docker.junit.com.github.dockerjava.core.DockerClientImpl;
import repackaged.com.arakelian.docker.junit.com.github.dockerjava.okhttp.OkHttpDockerCmdExecFactory;

import static repackaged.com.arakelian.docker.junit.com.github.dockerjava.core.DefaultDockerClientConfig.*;

public class AzuriteDockerRule implements TestRule {

    private static final String IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.19.0";

    public static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    public static final String ACCOUNT_NAME = "devstoreaccount1";

    private static final String CONTAINER_SUFFIX = UUID.randomUUID().toString().substring(0, 8);

    private final DockerRule wrappedRule;

    private static final AtomicReference<Exception> STARTUP_EXCEPTION = new AtomicReference<>();

    public AzuriteDockerRule() {
        wrappedRule = new DockerRule(ImmutableDockerConfig.builder()
                .image(IMAGE)
                .addHostConfigConfigurer(hostConfig -> {
                    hostConfig.withAutoRemove(true);
                    hostConfig.withPortBindings(new PortBinding(Ports.Binding.bindIp("127.0.0.1"), ExposedPort.tcp(10000)));
                })
                .addCreateContainerConfigurer(create -> {
                    create.withName("oak-test-azurite-" + CONTAINER_SUFFIX);
                    create.withCmd("azurite-blob", "--blobHost=0.0.0.0");
                })

                .addStartedListener(container -> {
                    try {
                        container.waitForPort(ExposedPort.tcp(10000));
                        container.waitForLog("Azurite Blob service is successfully listening at http://0.0.0.0:10000");
                    } catch (IllegalStateException e) {
                        STARTUP_EXCEPTION.set(e);
                        throw e;
                    }
                })
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
        try (AbstractDockerCmdExecFactory dockerCmdExecFactory = dockerCmdExecFactory();
             DockerClient client = DockerClientImpl.getInstance(createDefaultConfigBuilder().build())
                     .withDockerCmdExecFactory(dockerCmdExecFactory)) {
            client.pingCmd().exec();
            client.pullImageCmd(IMAGE).exec(new PullImageResultCallback());
        } catch (Throwable t) {
            Assume.assumeNoException(t);
        }

        Statement base = wrappedRule.apply(statement, description);
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } catch (Throwable e) {
                    Assume.assumeNoException(STARTUP_EXCEPTION.get());
                    throw e;
                }
            }
        };
    }

    private static AbstractDockerCmdExecFactory dockerCmdExecFactory() {
        return new OkHttpDockerCmdExecFactory().withConnectTimeout(5000).withReadTimeout(20000);
    }

    public int getMappedPort() {
        Ports.Binding binding = wrappedRule.getContainer().getBinding(ExposedPort.tcp(10000));
        Container.SimpleBinding simpleBinding = Container.SimpleBinding.of(binding);
        return simpleBinding.getPort();
    }
}
