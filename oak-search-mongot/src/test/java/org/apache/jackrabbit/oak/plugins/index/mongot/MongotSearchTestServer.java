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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.mongodb.MongoDBAtlasLocalContainer;

final class MongotSearchTestServer implements AutoCloseable {

    static final String IMAGE = "mongodb/mongodb-atlas-local:8.2.6-20260715T144108Z";

    private static final Logger LOG = LoggerFactory.getLogger(MongotSearchTestServer.class);
    private static final MongotSearchTestServer SERVER = new MongotSearchTestServer();

    private static volatile MongoDBAtlasLocalContainer container;
    private static boolean shutdownHookRegistered;

    private MongotSearchTestServer() {
    }

    static synchronized MongoDBAtlasLocalContainer getTestServer() {
        if (container == null || !container.isRunning()) {
            SERVER.start();
            if (!shutdownHookRegistered) {
                Runtime.getRuntime().addShutdownHook(new Thread(SERVER::close,
                        "oak-mongot-search-test-server-shutdown"));
                shutdownHookRegistered = true;
            }
        }
        return container;
    }

    private void start() {
        verifyDockerAvailable();

        MongoDBAtlasLocalContainer candidate = new MongoDBAtlasLocalContainer(IMAGE)
                .withEnv("DO_NOT_TRACK", "1")
                .withStartupTimeout(Duration.ofMinutes(5))
                .withStartupAttempts(3);
        try {
            LOG.info("Starting Mongot test server using {}", IMAGE);
            candidate.start();
            candidate.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams());
            container = candidate;
        } catch (RuntimeException e) {
            String logs = availableLogs(candidate);
            candidate.stop();
            throw new IllegalStateException("Unable to start Mongot test server. Container logs:\n" + logs, e);
        }
    }

    private static void verifyDockerAvailable() {
        try {
            DockerClientFactory.instance().client().pingCmd().exec();
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "Docker is required when the mongoSearchConnectionString system property is not set", e);
        }
    }

    private static String availableLogs(MongoDBAtlasLocalContainer candidate) {
        try {
            return candidate.getLogs();
        } catch (RuntimeException logFailure) {
            return "<container logs unavailable: " + logFailure.getMessage() + ">";
        }
    }

    @Override
    public synchronized void close() {
        if (container != null) {
            LOG.info("Stopping Mongot test server");
            container.stop();
            container = null;
        }
    }
}
