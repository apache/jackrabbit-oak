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

import com.arakelian.docker.junit.DockerRule;
import com.arakelian.docker.junit.model.ImmutableDockerConfig;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.PullImageResultCallback;

import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;

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
        try {
            DockerClient client = DockerClientFactory.instance().client();
            client.pingCmd().exec();
            client.pullImageCmd(IMAGE).exec(new PullImageResultCallback()).awaitCompletion();
            available = true;
        } catch (Exception t) {
            LOG.info("Cannot connect to docker or pull image", t);
        }
        DOCKER_AVAILABLE = available;
    }

    private final DockerRule wrappedRule;

    private static final AtomicReference<Exception> STARTUP_EXCEPTION = new AtomicReference<>();

    public MongoDockerRule() {
        wrappedRule = new DockerRule(ImmutableDockerConfig.builder()
                .name(CONFIG_NAME)
                .image(IMAGE)
                .ports("27017")
                .allowRunningBetweenUnitTests(true)
                .alwaysRemoveContainer(true)
                .addStartedListener(container -> {
                    try {
                        container.waitForPort("27017/tcp");
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
        return wrappedRule.getContainer().getPortBinding("27017/tcp").getPort();
    }

    public String getHost() {
        return wrappedRule.getContainer().getPortBinding("27017/tcp").getHost();
    }

    public static boolean isDockerAvailable() {
        return DOCKER_AVAILABLE;
    }
}
