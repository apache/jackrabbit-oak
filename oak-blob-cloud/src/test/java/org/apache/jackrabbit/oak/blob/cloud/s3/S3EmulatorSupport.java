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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Lazy singleton that starts an {@link S3MockRule} Docker container on first use and exposes
 * S3 emulator properties for use in {@link S3DataStoreUtils#getS3Config()}.
 *
 * <p>GCP emulator coverage is intentionally out of scope because S3Mock is not compatible
 * with Oak's GCP client configuration.</p>
 *
 * <p>If Docker is unavailable, {@link #isAvailable()} returns {@code false} and tests
 * skip rather than fail.</p>
 */
public final class S3EmulatorSupport {

    private static final Logger log = LoggerFactory.getLogger(S3EmulatorSupport.class);

    /** System property reserved for selecting emulator mode; only S3 is currently supported. */
    static final String S3_TEST_MODE_PROP = "s3.test.mode";

    /** Dummy access key accepted by S3Mock. */
    static final String ACCESS_KEY = "foo";

    /** Dummy secret key accepted by S3Mock. */
    static final String SECRET_KEY = "bar";

    /** Default bucket name used when no test-specific bucket is configured. */
    static final String DEFAULT_BUCKET = "s3mock-default-test-bucket";

    private static volatile S3MockRule instance;
    private static volatile boolean startupAttempted = false;

    private S3EmulatorSupport() {}

    /**
     * Returns {@code true} if the S3Mock container started successfully and is available for tests.
     */
    public static boolean isAvailable() {
        if (isUnsupportedMode()) {
            return false;
        }
        ensureStarted();
        return instance != null;
    }

    /**
     * Returns a {@link Properties} instance configured for the S3 emulator.
     * Returns an empty {@link Properties} if the container is not available.
     */
    public static Properties getEmulatorProperties() {
        if (isUnsupportedMode()) {
            return new Properties();
        }
        ensureStarted();
        S3MockRule rule = instance;
        if (rule == null) {
            return new Properties();
        }
        String endpoint = rule.getHttpEndpoint();
        Properties props = new Properties();
        props.setProperty(S3Constants.ACCESS_KEY, ACCESS_KEY);
        props.setProperty(S3Constants.SECRET_KEY, SECRET_KEY);
        props.setProperty(S3Constants.S3_BUCKET, DEFAULT_BUCKET);
        props.setProperty(S3Constants.S3_CONN_PROTOCOL, "http");
        props.setProperty(S3Constants.PATH_STYLE_ACCESS, "true");
        props.setProperty(S3Constants.S3_ENCRYPTION, S3Constants.S3_ENCRYPTION_NONE);
        props.setProperty(S3Constants.S3_END_POINT, endpoint);
        props.setProperty(S3Constants.S3_REGION, "us-east-1");
        props.setProperty(S3Constants.S3_MAX_ERR_RETRY, "3");
        props.setProperty(S3Constants.S3_CONN_TIMEOUT, "10000");
        props.setProperty(S3Constants.S3_SOCK_TIMEOUT, "30000");
        props.setProperty(S3Constants.S3_MAX_CONNS, "10");
        return props;
    }

    private static void ensureStarted() {
        if (isUnsupportedMode() || instance != null) {
            return;
        }
        synchronized (S3EmulatorSupport.class) {
            if (instance != null || startupAttempted) {
                return;
            }
            startupAttempted = true;
            S3MockRule rule = new S3MockRule();
            try {
                rule.before();
                instance = rule;
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        rule.after();
                    } catch (Exception ignored) {
                    }
                }));
            } catch (Throwable e) {
                try {
                    rule.after();
                } catch (Exception ignored) {
                }
                log.warn("S3Mock container failed to start — emulator tests will be skipped", e);
            }
        }
    }

    private static boolean isUnsupportedMode() {
        return !"S3".equalsIgnoreCase(System.getProperty(S3_TEST_MODE_PROP, "S3"));
    }
}
