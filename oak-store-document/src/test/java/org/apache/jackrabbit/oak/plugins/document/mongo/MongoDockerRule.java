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
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A MongoDB {@link DockerRule}.
 */
public class MongoDockerRule extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDockerRule.class);

    private static final String VERSION = System.getProperty("mongo.version", "3.6");
    private static final AtomicReference<Exception> STARTUP_EXCEPTION = new AtomicReference<>();
    private static final int DEFAULT_MONGO_PORT = 27017;
    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("mongo:" + VERSION);
    private static final boolean DOCKER_AVAILABLE;
    private static GenericContainer<?> mongoDBContainer;

    static {

        boolean dockerAvailable = false;
        boolean imageAvailable = false;
        try {
            dockerAvailable = checkDockerAvailability();
            if (dockerAvailable) {
                imageAvailable = checkImageAvailability();
            }
        } catch (Throwable t) {
            LOG.error("either docker is not available or specified mongo image cannot be pulled, dockerAvailable: {}, imageAvailable: {}, error: ", dockerAvailable, imageAvailable, t);
        }
        DOCKER_AVAILABLE = dockerAvailable && imageAvailable;
    }

    @Override
    protected void before() throws Throwable {
        if (mongoDBContainer != null) {
            return;
        }
        mongoDBContainer = new GenericContainer<>(DOCKER_IMAGE_NAME)
                .withExposedPorts(DEFAULT_MONGO_PORT)
                .withStartupTimeout(Duration.ofMinutes(1));

        try {
            long startTime = Instant.now().toEpochMilli();
            mongoDBContainer.start();
            LOG.info("mongo container started in: " + (Instant.now().toEpochMilli() - startTime) + " ms");
        } catch (Exception e) {
            LOG.error("error while starting mongoDb container, error: ", e);
            STARTUP_EXCEPTION.set(e);
            throw e;
        }
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    before();
                } catch (Throwable e) {
                    Assume.assumeNoException(STARTUP_EXCEPTION.get());
                    throw e;
                }

                List<Throwable> errors = new ArrayList<>();
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    errors.add(t);
                }
                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

    private static boolean checkImageAvailability() throws TimeoutException {
        RemoteDockerImage remoteDockerImage = new RemoteDockerImage(DOCKER_IMAGE_NAME);
        remoteDockerImage.get(1, TimeUnit.MINUTES);
        return true;
    }

    private static boolean checkDockerAvailability() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    public int getPort() {
        return mongoDBContainer.getMappedPort(DEFAULT_MONGO_PORT);
    }

    public String getHost() {
        return mongoDBContainer.getHost();
    }

    public static boolean isDockerAvailable() {
        return DOCKER_AVAILABLE;
    }
}
