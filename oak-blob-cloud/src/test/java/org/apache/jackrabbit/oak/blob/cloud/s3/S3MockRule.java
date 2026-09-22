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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JUnit rule that starts an Adobe S3Mock Docker container for emulator-based S3 integration tests.
 * Mirrors the pattern used by {@code AzuriteDockerRule} in oak-blob-cloud-azure.
 *
 * <p>On Docker failure, the rule calls {@link Assume#assumeNoException} so tests are skipped
 * rather than failed, preserving the same behaviour as when no credentials are configured.</p>
 */
public class S3MockRule extends ExternalResource {

    private static final DockerImageName DOCKER_IMAGE = DockerImageName.parse("adobe/s3mock:5.0.0");
    static final int HTTP_PORT = 9090;
    private final AtomicReference<Exception> startupException = new AtomicReference<>();

    private GenericContainer<?> container;

    @Override
    protected void before() throws Throwable {
        startupException.set(null);
        container = new GenericContainer<>(DOCKER_IMAGE)
                .withExposedPorts(HTTP_PORT)
                .withStartupTimeout(Duration.ofSeconds(60));
        try {
            container.start();
        } catch (Exception e) {
            startupException.set(e);
            throw e;
        }
    }

    @Override
    protected void after() {
        if (container != null) {
            container.stop();
        }
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    before();
                } catch (Exception e) {
                    Assume.assumeNoException(e);
                    throw e;
                }
                List<Throwable> errors = new ArrayList<>();
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    try {
                        after();
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }
                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

    /**
     * Returns the mapped HTTP port on the host for the running S3Mock container.
     */
    public int getHttpPort() {
        return container.getMappedPort(HTTP_PORT);
    }

    /**
     * Returns the HTTP endpoint URL for the running S3Mock container, e.g. {@code http://127.0.0.1:PORT}.
     */
    public String getHttpEndpoint() {
        return "http://127.0.0.1:" + getHttpPort();
    }
}
