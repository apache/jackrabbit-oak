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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.oak.plugins.document.RdbUtils;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A MongoDB {@link GenericContainer}.
 */
public class RdbDockerRule extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(RdbDockerRule.class);

    private static final AtomicReference<Exception> STARTUP_EXCEPTION = new AtomicReference<>();
    private static final boolean RDB_AVAILABLE;
    private GenericContainer<?> rdbContainer;

    private static DockerImageName IMAGE = null;
    private static int exposedPort = getPortFromJdbcURL(RdbUtils.URL);

    static {
        if (!Strings.isNullOrEmpty(RdbUtils.IMAGE)) {
            IMAGE = DockerImageName.parse(RdbUtils.IMAGE);
        }
        boolean dockerAvailable = false;
        boolean imageAvailable = false;
        try {
            dockerAvailable = checkDockerAvailability();
            if (dockerAvailable) {
                imageAvailable = checkImageAvailability();
            } else {
                LOG.info("docker not available");
            }
        } catch (Throwable t) {
            LOG.error("not able to pull specified docker image: {}, error: ", RdbUtils.IMAGE, t);
        }
        RDB_AVAILABLE = dockerAvailable && imageAvailable;
    }

    @Override
    protected void before() throws Throwable {
        if (!RDB_AVAILABLE || rdbContainer != null && rdbContainer.isRunning()) {
            return;
        }
        rdbContainer = new GenericContainer<>(IMAGE)
                .withPrivilegedMode(true)
                .withExposedPorts(exposedPort)
                .withStartupTimeout(Duration.ofMinutes(15));

        try {
            long startTime = Instant.now().toEpochMilli();
            rdbContainer.start();
            LOG.info("RDB container started in: " + (Instant.now().toEpochMilli() - startTime) + " ms");
        } catch (Exception e) {
            LOG.error("error while starting RDB container, error: ", e);
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
        if (Strings.isNullOrEmpty(RdbUtils.IMAGE)) {
            return false;
        }
        RemoteDockerImage remoteDockerImage = new RemoteDockerImage(IMAGE);
        remoteDockerImage.get(60, TimeUnit.MINUTES);
        return true;
    }

    private static boolean checkDockerAvailability() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    public static boolean isDockerImageAvailable() {
        return RDB_AVAILABLE;
    }

    public int getExposedPort() {
        return exposedPort;
    }

    public String getHost() {
        return rdbContainer.getHost();
    }

    public int getMappedPort() {
        return rdbContainer.getMappedPort(exposedPort);
    }

    public static int getPortFromJdbcURL(String jdbcURL) {
        String normalizedJdbcUri = jdbcURL.replaceFirst("@//", "//").replaceFirst("@", "//");
        Pattern pattern = Pattern.compile("//[^:/]+(:(\\d+))?");
        Matcher matcher = pattern.matcher(normalizedJdbcUri);
        if (matcher.find()) {
            if (matcher.groupCount() > 1) {
                try {
                    return Integer.parseInt(matcher.group(2));
                } catch (NumberFormatException ignored) {
                    //should not happen
                }
            }
        }
        return -1;
    }

}
