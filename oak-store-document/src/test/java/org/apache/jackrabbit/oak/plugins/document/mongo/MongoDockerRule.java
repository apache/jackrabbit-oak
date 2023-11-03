/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.arakelian.docker.junit.Container;
import com.arakelian.docker.junit.DockerRule;
import com.arakelian.docker.junit.model.ImmutableDockerConfig;

import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * A MongoDB {@link DockerRule}.
 */
public class MongoDockerRule implements TestRule {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDockerRule.class);

    private static final String CONFIG_NAME = "MongoDB-" + UUID.randomUUID().toString().substring(0, 8);

    private static final String VERSION = System.getProperty("mongo.version", "3.6");

    private static final String IMAGE = "mongo:" + VERSION;

    private static final boolean DOCKER_AVAILABLE;

    static {
        boolean available = false;
        try (AbstractDockerCmdExecFactory dockerCmdExecFactory = dockerCmdExecFactory();
             DockerClient dockerClient = DockerClientImpl.getInstance(createDefaultConfigBuilder().build())
                             .withDockerCmdExecFactory(dockerCmdExecFactory)) {
            dockerClient.pingCmd().exec();
            dockerClient.pullImageCmd(IMAGE).exec(new PullImageResultCallback());
            available = true;
        } catch (Throwable t) {
            LOG.info("Cannot connect to docker or pull image", t);
        }
        DOCKER_AVAILABLE = available;
    }

    private static AbstractDockerCmdExecFactory dockerCmdExecFactory() {
        return new OkHttpDockerCmdExecFactory().withConnectTimeout(5000).withReadTimeout(20000);
    }

    private final DockerRule wrappedRule;

    private static final AtomicReference<Exception> STARTUP_EXCEPTION = new AtomicReference<>();

    public MongoDockerRule() {
        wrappedRule = new DockerRule(ImmutableDockerConfig.builder()
                .image(IMAGE)
                .allowRunningBetweenUnitTests(true)
                .addHostConfigConfigurer(hostConfig -> {
                    hostConfig.withAutoRemove(true);
                    hostConfig.withPortBindings(new PortBinding(Ports.Binding.empty(), ExposedPort.tcp(27017)));
                })
                .addCreateContainerConfigurer(create -> {
                    create.withName(CONFIG_NAME);
                })
                .addStartedListener(container -> {
                    try {
                        container.waitForPort(ExposedPort.tcp(27017));
                    } catch (IllegalStateException e) {
                        STARTUP_EXCEPTION.set(e);
                        throw e;
                    }
                })
                .build());
    }

    @Override
    public Statement apply(Statement statement, Description description) {
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

    public int getPort() {
        Ports.Binding binding = wrappedRule.getContainer().getBinding(ExposedPort.tcp(27017));
        Container.SimpleBinding simpleBinding = Container.SimpleBinding.of(binding);
        return simpleBinding.getPort();
    }

    public String getHost() {
        Ports.Binding binding = wrappedRule.getContainer().getBinding(ExposedPort.tcp(27017));
        Container.SimpleBinding simpleBinding = Container.SimpleBinding.of(binding);
        return simpleBinding.getHost();
    }

    public static boolean isDockerAvailable() {
        return DOCKER_AVAILABLE;
    }
}
