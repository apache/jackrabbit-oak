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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.profiles.ProfileFile;

/**
 * Tests {@link ImdsV2CredentialsProvider} against a local EC2 metadata endpoint.
 */
public class ImdsV2CredentialsProviderTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final Map<String, String> savedProperties = new LinkedHashMap<>();
    private final AtomicInteger tokenRequests = new AtomicInteger();
    private final AtomicInteger tokenlessRequests = new AtomicInteger();
    private final AtomicReference<String> tokenMethod = new AtomicReference<>();
    private final AtomicReference<String> tokenTtl = new AtomicReference<>();
    private final CountDownLatch fallbackReady = new CountDownLatch(1);
    private final CountDownLatch secondAttempt = new CountDownLatch(1);
    private HttpServer server;
    private ExecutorService serverExecutor;
    private volatile int tokenStatus = 200;
    private volatile boolean holdFallback;
    private Path config;
    private Path credentials;
    private Logger logger;
    private ListAppender<ILoggingEvent> logs;
    private ImdsV2CredentialsProvider provider;

    /** Isolates AWS configuration and starts an ephemeral local metadata server. */
    @Before
    public void setUp() throws Exception {
        for (String variable : List.of("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "AWS_EC2_METADATA_V1_DISABLED")) {
            Assume.assumeTrue("Metadata tests require " + variable + " to be unset", System.getenv(variable) == null);
        }
        config = folder.newFile("config").toPath();
        credentials = folder.newFile("credentials").toPath();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverExecutor = Executors.newCachedThreadPool();
        server.setExecutor(serverExecutor);
        server.createContext("/", this::handle);
        server.start();
        setProperty("aws.ec2MetadataServiceEndpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        setProperty("aws.sharedCredentialsFile", credentials.toString());
        setProperty("aws.configFile", config.toString());
        setProperty("aws.profile", "default");
        setProperty("aws.disableEc2Metadata", "false");
        setProperty("aws.disableEc2MetadataV1", null);
        setProperty("aws.accessKeyId", null);
        setProperty("aws.secretAccessKey", null);
        setProperty("aws.sessionToken", null);
        logger = (Logger) LoggerFactory.getLogger(ImdsV2CredentialsProvider.class);
        logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
    }

    /** Releases the server, provider and appender and restores every changed property. */
    @After
    public void tearDown() {
        try {
            if (provider != null) {
                provider.close();
            }
        } finally {
            if (logger != null && logs != null) {
                logger.detachAppender(logs);
                logs.stop();
            }
            if (server != null) {
                server.stop(0);
            }
            if (serverExecutor != null) {
                serverExecutor.shutdownNow();
            }
            savedProperties.forEach((key, value) -> {
                if (value == null) {
                    System.clearProperty(key);
                } else {
                    System.setProperty(key, value);
                }
            });
        }
    }

    /** Verifies token acquisition and token-bearing metadata requests. */
    @Test
    public void tokenSuccessUsesV2() {
        provider = new ImdsV2CredentialsProvider();
        Assert.assertEquals("test-imds-key", provider.resolveCredentials().accessKeyId());
        Assert.assertEquals("PUT", tokenMethod.get());
        Assert.assertTrue(Long.parseLong(tokenTtl.get()) > 0);
        Assert.assertEquals(1, tokenRequests.get());
        Assert.assertEquals(0, tokenlessRequests.get());
        Assert.assertEquals(0, errorCount());
    }

    /** Keeps the compatibility retry visible for a selected named profile. */
    @Test
    public void namedProfileFallbackIsVisible() throws Exception {
        tokenStatus = 403;
        Files.writeString(config, "[profile work]\n");
        setProperty("aws.profile", "work");
        provider = new ImdsV2CredentialsProvider();
        Assert.assertEquals("test-imds-key", provider.resolveCredentials().accessKeyId());
        Assert.assertTrue(tokenlessRequests.get() > 0);
        Assert.assertEquals(1, errorCount());
        Assert.assertTrue(logs.list.stream().anyMatch(event ->
                event.getFormattedMessage().contains("HttpTokens=required")));
    }

    /** A hard operator prohibition must not issue tokenless requests or claim a fallback. */
    @Test
    public void operatorDisableDoesNotClaimFallback() {
        tokenStatus = 403;
        setProperty("aws.disableEc2MetadataV1", "true");
        provider = new ImdsV2CredentialsProvider();
        Assert.assertThrows(SdkClientException.class, () -> provider.resolveCredentials());
        Assert.assertEquals(0, tokenlessRequests.get());
        Assert.assertEquals(0, errorCount());
    }

    /** Preserves the SDK's explicit system-property override of the profile policy. */
    @Test
    public void explicitV1PermissionTakesPrecedence() throws Exception {
        tokenStatus = 403;
        Files.writeString(config, "[default]\nec2_metadata_v1_disabled=true\n");
        setProperty("aws.disableEc2MetadataV1", "false");
        provider = new ImdsV2CredentialsProvider();
        Assert.assertEquals("test-imds-key", provider.resolveCredentials().accessKeyId());
        Assert.assertTrue(tokenlessRequests.get() > 0);
        Assert.assertEquals(0, errorCount());
    }

    /** Profile-level IMDSv1 prohibitions also apply to the retry provider. */
    @Test
    public void profileDisableDoesNotClaimFallback() throws Exception {
        tokenStatus = 403;
        Files.writeString(config, "[profile work]\nec2_metadata_v1_disabled=true\n");
        setProperty("aws.profile", "work");
        provider = new ImdsV2CredentialsProvider();
        Assert.assertThrows(SdkClientException.class, () -> provider.resolveCredentials());
        Assert.assertEquals(0, tokenlessRequests.get());
        Assert.assertEquals(0, errorCount());
    }

    /** Earlier credentials must succeed without parsing an unrelated broken profile. */
    @Test
    public void systemCredentialsIgnoreMalformedProfile() throws Exception {
        Files.writeString(config, "[broken\n");
        setProperty("aws.accessKeyId", "test-system-key");
        setProperty("aws.secretAccessKey", "test-system-secret");
        provider = new ImdsV2CredentialsProvider();
        Assert.assertEquals("test-system-key", provider.resolveCredentials().accessKeyId());
        Assert.assertEquals(0, tokenRequests.get());
    }

    /** Cached compatibility credentials must not cause a failing token request per S3 operation. */
    @Test
    public void cachedFallbackDoesNotRetryStrictProvider() {
        tokenStatus = 403;
        provider = new ImdsV2CredentialsProvider();
        provider.resolveCredentials();
        int initialRequests = tokenRequests.get();
        provider.resolveCredentials();
        Assert.assertEquals(initialRequests, tokenRequests.get());
        Assert.assertEquals(1, errorCount());
    }

    /** Concurrent initial failures must emit only one successful-fallback diagnostic. */
    @Test
    public void concurrentFallbackLogsOnce() throws Exception {
        tokenStatus = 403;
        holdFallback = true;
        provider = new ImdsV2CredentialsProvider();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            var first = callers.submit(provider::resolveCredentials);
            Assert.assertTrue("First retry reached the metadata endpoint", fallbackReady.await(10, TimeUnit.SECONDS));
            var second = callers.submit(provider::resolveCredentials);
            Assert.assertEquals("test-imds-key", first.get(10, TimeUnit.SECONDS).accessKeyId());
            Assert.assertEquals("test-imds-key", second.get(10, TimeUnit.SECONDS).accessKeyId());
            Assert.assertEquals(1, errorCount());
        } finally {
            secondAttempt.countDown();
            callers.shutdownNow();
        }
    }

    /** Aggregating the policy profile must retain credentials in the real named profile. */
    @Test
    public void namedProfileCredentialsArePreserved() throws Exception {
        Files.writeString(credentials, "[work]\naws_access_key_id=test-profile-key\n"
                + "aws_secret_access_key=test-profile-secret\n");
        setProperty("aws.profile", "work");
        provider = new ImdsV2CredentialsProvider();
        Assert.assertEquals("test-profile-key", provider.resolveCredentials().accessKeyId());
        Assert.assertEquals(0, tokenRequests.get());
    }

    /** Reuses unchanged profile settings and retains credentials after a source update. */
    @Test
    public void cachesPolicyUntilSourceChanges() {
        provider = new ImdsV2CredentialsProvider();
        ProfileFile original = ProfileFile.builder().type(ProfileFile.Type.CREDENTIALS)
                .content("[default]\naws_access_key_id=original-key\n").build();
        ProfileFile first = provider.withImdsV1Disabled(original);
        Assert.assertSame(first, provider.withImdsV1Disabled(original));

        ProfileFile updated = ProfileFile.builder().type(ProfileFile.Type.CREDENTIALS)
                .content("[default]\naws_access_key_id=updated-key\n").build();
        ProfileFile refreshed = provider.withImdsV1Disabled(updated);
        Assert.assertNotSame(first, refreshed);
        Assert.assertEquals("updated-key", refreshed.profile("default").orElseThrow()
                .property("aws_access_key_id").orElseThrow());
        Assert.assertEquals("true", refreshed.profile("default").orElseThrow()
                .property("ec2_metadata_v1_disabled").orElseThrow());
        Assert.assertSame(refreshed, provider.withImdsV1Disabled(updated));
    }

    /** A different selected profile gets its own IMDSv2 policy. */
    @Test
    public void refreshesPolicyWhenSelectedProfileChanges() {
        provider = new ImdsV2CredentialsProvider();
        ProfileFile original = ProfileFile.builder().type(ProfileFile.Type.CREDENTIALS).content("").build();
        ProfileFile first = provider.withImdsV1Disabled(original);
        setProperty("aws.profile", "work");
        ProfileFile refreshed = provider.withImdsV1Disabled(original);
        Assert.assertNotSame(first, refreshed);
        Assert.assertEquals("true", refreshed.profile("work").orElseThrow()
                .property("ec2_metadata_v1_disabled").orElseThrow());
        Assert.assertSame(refreshed, provider.withImdsV1Disabled(original));
    }

    /** Disabling metadata entirely must produce neither requests nor a successful-fallback message. */
    @Test
    public void metadataDisabledDoesNotClaimFallback() {
        setProperty("aws.disableEc2Metadata", "true");
        provider = new ImdsV2CredentialsProvider();
        Assert.assertThrows(SdkClientException.class, () -> provider.resolveCredentials());
        Assert.assertEquals(0, tokenRequests.get());
        Assert.assertEquals(0, errorCount());
    }

    /** A nonrecoverable token error must not be reported as a successful compatibility retry. */
    @Test
    public void failedRetryDoesNotClaimFallback() {
        tokenStatus = 400;
        provider = new ImdsV2CredentialsProvider();
        Assert.assertThrows(SdkClientException.class, () -> provider.resolveCredentials());
        Assert.assertEquals(0, tokenlessRequests.get());
        Assert.assertEquals(0, errorCount());
    }

    /** Explicit Oak access keys bypass the new environment credential policy. */
    @Test
    public void staticOakCredentialsAreUnchanged() {
        Properties properties = new Properties();
        properties.setProperty(S3Constants.ACCESS_KEY, "test-oak-key");
        properties.setProperty(S3Constants.SECRET_KEY, "test-oak-secret");
        AwsCredentialsProvider credentialsProvider = Utils.getAwsCredentials(properties);
        Assert.assertEquals("test-oak-key", credentialsProvider.resolveCredentials().accessKeyId());
        Assert.assertEquals(0, tokenRequests.get());
    }

    /** The no-key Oak configuration uses the tested provider in its actual credential path. */
    @Test
    public void environmentOakCredentialsUseV2Provider() {
        AwsCredentialsProvider credentialsProvider = Utils.getAwsCredentials(new Properties());
        Assert.assertTrue(credentialsProvider instanceof ImdsV2CredentialsProvider);
        provider = (ImdsV2CredentialsProvider) credentialsProvider;
        Assert.assertEquals("test-imds-key", provider.resolveCredentials().accessKeyId());
        Assert.assertEquals(0, tokenlessRequests.get());
    }

    private long errorCount() {
        return logs.list.stream().filter(event -> event.getLevel() == Level.ERROR).count();
    }

    private void setProperty(String key, String value) {
        if (!savedProperties.containsKey(key)) {
            savedProperties.put(key, System.getProperty(key));
        }
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String body;
        int status = 200;
        if (path.equals("/latest/api/token")) {
            if (tokenRequests.incrementAndGet() >= 3) {
                secondAttempt.countDown();
            }
            tokenMethod.set(exchange.getRequestMethod());
            tokenTtl.set(exchange.getRequestHeaders().getFirst("X-aws-ec2-metadata-token-ttl-seconds"));
            status = tokenStatus;
            body = "test-token";
        } else {
            if (!"test-token".equals(exchange.getRequestHeaders().getFirst("X-aws-ec2-metadata-token"))) {
                tokenlessRequests.incrementAndGet();
            }
            if (holdFallback && path.endsWith("/test-role")) {
                fallbackReady.countDown();
                try {
                    if (!secondAttempt.await(10, TimeUnit.SECONDS)) {
                        throw new IOException("Concurrent metadata request did not arrive");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            body = path.endsWith("/") ? "test-role" : """
                    {"Code":"Success","AccessKeyId":"test-imds-key","SecretAccessKey":"test-secret",
                    "Token":"test-session","Expiration":"%s"}
                    """.formatted(Instant.now().plusSeconds(3600));
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (exchange; var output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }
}
