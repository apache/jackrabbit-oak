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

import com.arakelian.docker.junit.DockerRule;
import com.arakelian.docker.junit.model.ImmutableDockerConfig;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.auth.FixedRegistryAuthSupplier;

import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A MongoDB {@link DockerRule}.
 */
public class MongoDockerRule extends DockerRule {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDockerRule.class);

    private static final String CONFIG_NAME = "MongoDB";

    private static final String VERSION = System.getProperty("mongo.version", "3.6");

    private static final String IMAGE = "mongo:" + VERSION;

    private static final boolean DOCKER_AVAILABLE;

    static {
        boolean available = false;
        try (DefaultDockerClient client = DefaultDockerClient.fromEnv()
                .connectTimeoutMillis(5000L).readTimeoutMillis(20000L)
                .registryAuthSupplier(new FixedRegistryAuthSupplier())
                .build()) {
            client.ping();
            client.pull(IMAGE);
            available = true;
        } catch (Throwable t) {
            LOG.info("Cannot connect to docker or pull image", t);
        }
        DOCKER_AVAILABLE = available;
    }

    public MongoDockerRule() {
        super(ImmutableDockerConfig.builder()
                .name(CONFIG_NAME)
                .image(IMAGE)
                .ports("27017")
                .allowRunningBetweenUnitTests(true)
                .alwaysRemoveContainer(true)
                .addStartedListener(container -> container.waitForPort("27017/tcp"))
                .build());
    }

    public int getPort() {
        return getContainer().getPortBinding("27017/tcp").getPort();
    }

    public boolean isDockerAvailable() {
        return DOCKER_AVAILABLE;
    }
}
