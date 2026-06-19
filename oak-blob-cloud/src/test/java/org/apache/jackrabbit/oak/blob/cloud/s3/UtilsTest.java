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

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

/**
 * Unit cases for Utils class
 */
public class UtilsTest {

    @Test
    public void testGetRegionFromEndpoint() {
        Properties props = new Properties();
        props.setProperty("s3EndPoint", "https://s3.eu-west-1.amazonaws.com");
        props.setProperty("protocol", "https");
        Assert.assertEquals("eu-west-1", Utils.getRegion(props));
    }

    @Test
    public void testGetRegionFromEndpointUsEast1() {
        Properties props = new Properties();
        props.setProperty("s3EndPoint", "https://s3.amazonaws.com");
        Assert.assertEquals("us-east-1", Utils.getRegion(props));
    }

    @Test
    public void testGetRegionFromProperty() {
        Properties props = new Properties();
        props.setProperty("s3Region", "ap-south-1");
        Assert.assertEquals("ap-south-1", Utils.getRegion(props));
    }

    @Test
    public void testGetRegionFromPropertyUsStandard() {
        Properties props = new Properties();
        props.setProperty("s3Region", "us-standard");
        Assert.assertEquals("us-east-1", Utils.getRegion(props));
    }

    @Test
    public void testGetRegionFallbackToDefault() {
        String previous = System.getProperty("aws.region");
        System.setProperty("aws.region", "us-west-2");
        try {
            Properties props = new Properties();
            props.setProperty("s3Region", "");
            String region = Utils.getRegion(props);
            Assert.assertNotNull(region);
            Assert.assertFalse(region.isEmpty());
        } finally {
            if (previous != null) {
                System.setProperty("aws.region", previous);
            } else {
                System.clearProperty("aws.region");
            }
        }
    }

    @Test
    public void testGetEndPointUriWithCustomEndpoint() {
        Properties props = new Properties();
        props.setProperty("s3EndPoint", "https://custom.endpoint.com");
        props.setProperty("s3ConnProtocol", "http");
        URI uri = Utils.getEndPointUri(props, false, "eu-west-1");
        Assert.assertEquals("https://custom.endpoint.com", uri.toString());
    }

    @Test
    public void testGetEndPointUriWithAcceleration() {
        Properties props = new Properties();
        props.setProperty("s3ConnProtocol", "https");
        URI uri = Utils.getEndPointUri(props, true, "ap-south-1");
        Assert.assertEquals("https://s3-accelerate.ap-south-1.amazonaws.com", uri.toString());
    }

    @Test
    public void testGetEndPointUriWithRegion() {
        Properties props = new Properties();
        props.setProperty("s3ConnProtocol", "https");
        URI uri = Utils.getEndPointUri(props, false, "us-east-2");
        Assert.assertEquals("https://s3.us-east-2.amazonaws.com", uri.toString());
    }

    @Test
    public void testGetEndPointUriDefaultProtocol() {
        Properties props = new Properties();
        // No protocol set, should default to https
        URI uri = Utils.getEndPointUri(props, false, "us-west-1");
        Assert.assertEquals("https://s3.us-west-1.amazonaws.com", uri.toString());
    }

    @Test
    public void testGetRegionFromStandardEndpoint() {
        Assert.assertEquals("eu-west-1", Utils.getRegionFromEndpoint("https://s3.eu-west-1.amazonaws.com", null));
    }

    @Test
    public void testGetRegionFromVirtualHostedEndpoint() {
        Assert.assertEquals("ap-south-1", Utils.getRegionFromEndpoint("https://bucket.s3.ap-south-1.amazonaws.com", null));
    }

    @Test
    public void testGetRegionFromUsEast1Endpoint() {
        Assert.assertEquals("us-east-1", Utils.getRegionFromEndpoint("https://s3.amazonaws.com", null));
    }

    @Test
    public void testGetRegionFromVirtualHostedUsEast1() {
        Assert.assertEquals("us-east-1", Utils.getRegionFromEndpoint("https://bucket.s3.amazonaws.com", null));
    }

    @Test
    public void testGetRegionFromInvalidEndpoint() {
        Assert.assertNull(Utils.getRegionFromEndpoint("https://example.com", null));
    }

    @Test
    public void testGetRegionFromMalformedEndpoint() {
        Assert.assertNull(Utils.getRegionFromEndpoint("not-a-valid-uri", "https"));
    }

    @Test
    public void testGetRegionFromEndpointWithoutProtocol() {
        Assert.assertEquals("us-east-1", Utils.getRegionFromEndpoint("s3.us-east-1.amazonaws.com", "https"));
    }

    @Test
    public void testGetDataEncryptionNoneWhenUnset() {
        Properties props = new Properties();
        Assert.assertEquals(DataEncryption.NONE, Utils.getDataEncryption(props));
    }

    @Test
    public void testGetDataEncryptionSSEKMS() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_KMS");
        Assert.assertEquals(DataEncryption.SSE_KMS, Utils.getDataEncryption(props));
    }

    @Test
    public void testGetDataEncryptionSSES3() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_S3");
        Assert.assertEquals(DataEncryption.SSE_S3, Utils.getDataEncryption(props));
    }

    @Test
    public void testGetDataEncryptionInvalidValue() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "INVALID");
        try {
            Utils.getDataEncryption(props);
            Assert.fail("Expected IllegalArgumentException for invalid encryption type");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testSetRemoteStorageModeS3() {
        Properties props = new Properties();
        props.setProperty("s3EndPoint", "https://s3.amazonaws.com");
        Utils.setRemoteStorageMode(props);
        Assert.assertEquals(S3Backend.RemoteStorageMode.S3, props.get(S3Constants.MODE));
    }

    @Test
    public void testSetRemoteStorageModeGCP() {
        Properties props = new Properties();
        props.setProperty("s3EndPoint", "https://storage.googleapis.com");
        Utils.setRemoteStorageMode(props);
        Assert.assertEquals(S3Backend.RemoteStorageMode.GCP, props.get(S3Constants.MODE));
    }

    @Test
    public void testSetRemoteStorageModeOverrideWarning() {
        Properties props = new Properties();
        props.setProperty("s3EndPoint", "https://storage.googleapis.com");
        props.put(S3Constants.MODE, S3Backend.RemoteStorageMode.S3);
        Utils.setRemoteStorageMode(props);
        Assert.assertEquals(S3Backend.RemoteStorageMode.GCP, props.get(S3Constants.MODE));
    }

    @Test
    public void testSetRemoteStorageModeDefaultsPreSetGCPToS3() {
        Properties props = new Properties();
        props.setProperty("s3EndPoint", "http://127.0.0.1:9090");
        props.put(S3Constants.MODE, S3Backend.RemoteStorageMode.GCP);
        Utils.setRemoteStorageMode(props);
        Assert.assertEquals(S3Backend.RemoteStorageMode.S3, props.get(S3Constants.MODE));
    }

    @Test
    public void testSetRemoteStorageModeDefaultsBlankModeToS3() {
        Properties props = new Properties();
        props.setProperty("s3EndPoint", "https://s3.amazonaws.com");
        props.setProperty(S3Constants.MODE, "");
        Utils.setRemoteStorageMode(props);
        Assert.assertEquals(S3Backend.RemoteStorageMode.S3, props.get(S3Constants.MODE));
    }

    @Test
    public void testPathStyleAccessConstantHasExpectedStringValue() {
        Assert.assertEquals("pathStyleAccess", S3Constants.PATH_STYLE_ACCESS);
    }

    @Test
    public void testPathStyleAccessDefaultsToFalseWhenPropertyAbsent() {
        Properties props = new Properties();
        Assert.assertFalse(Boolean.parseBoolean(props.getProperty(S3Constants.PATH_STYLE_ACCESS, "false")));
    }

    @Test
    public void testPathStyleAccessTrueWhenPropertySetToTrue() {
        Properties props = new Properties();
        props.setProperty(S3Constants.PATH_STYLE_ACCESS, "true");
        Assert.assertTrue(Boolean.parseBoolean(props.getProperty(S3Constants.PATH_STYLE_ACCESS, "false")));
    }

    @Test
    public void clientConfigurationDoesNotExposeUnmanagedTimeoutExecutorByDefault() throws Exception {
        ClientOverrideConfiguration configuration = getClientConfiguration();
        Assert.assertFalse(configuration.scheduledExecutorService().isPresent());
    }

    @Test
    public void clientConfigurationUsesProvidedTimeoutExecutor() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            ClientOverrideConfiguration configuration = getClientConfiguration(executor);
            Assert.assertSame(executor, configuration.scheduledExecutorService().orElseThrow());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void createPresignerUsesConfiguredEndpointOverride() {
        Properties props = clientProperties();
        props.setProperty(S3Constants.S3_END_POINT, "http://127.0.0.1:9090");
        props.setProperty(S3Constants.PATH_STYLE_ACCESS, "true");

        try (S3Client client = Utils.openService(props, false);
             S3Presigner presigner = Utils.createPresigner(client, props)) {
            GetObjectRequest getObject = GetObjectRequest.builder().bucket("bucket").key("key").build();
            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(5))
                    .getObjectRequest(getObject)
                    .build();
            PresignedGetObjectRequest presigned = presigner.presignGetObject(presignRequest);

            Assert.assertEquals("127.0.0.1", presigned.url().getHost());
            Assert.assertEquals(9090, presigned.url().getPort());
            Assert.assertEquals("/bucket/key", presigned.url().getPath());
        }
    }

    @Test
    public void createPresignerUsesAccelerationEndpointWhenRequested() {
        Properties props = clientProperties();

        try (S3Client client = Utils.openService(props, true);
             S3Presigner presigner = Utils.createPresigner(client, props, true)) {
            GetObjectRequest getObject = GetObjectRequest.builder().bucket("bucket").key("key").build();
            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(5))
                    .getObjectRequest(getObject)
                    .build();
            PresignedGetObjectRequest presigned = presigner.presignGetObject(presignRequest);

            Assert.assertTrue(presigned.url().getHost().contains("s3-accelerate"));
        }
    }

    @Test
    public void isS3ConfiguredReturnsFalseForPartialCredentialsWithNoRegionOrEndpoint() throws IOException {
        // accessKey + secretKey without region or endpoint must not count as real credentials
        File tmp = File.createTempFile("s3test-partial-creds", ".properties");
        String previousConfig = System.getProperty("s3.config");
        try {
            Properties partial = new Properties();
            partial.setProperty(S3Constants.ACCESS_KEY, "somekey");
            partial.setProperty(S3Constants.SECRET_KEY, "somesecret");
            try (OutputStream out = new FileOutputStream(tmp)) {
                partial.store(out, null);
            }
            System.setProperty("s3.config", tmp.getAbsolutePath());
            // Skip when emulator is available — getS3Config() would fall back to it, making isS3Configured() true
            Assume.assumeFalse("Emulator available — would override partial creds", S3EmulatorSupport.isAvailable());
            Assert.assertFalse("Partial credentials (no region/endpoint) must not be treated as real",
                    S3DataStoreUtils.isS3Configured());
        } finally {
            if (previousConfig == null) {
                System.clearProperty("s3.config");
            } else {
                System.setProperty("s3.config", previousConfig);
            }
            tmp.delete();
        }
    }

    @Test
    public void isS3EmulatorConfiguredReturnsTrueForEmulatorProperties() throws IOException {
        Properties props = clientProperties();
        props.setProperty(S3Constants.ACCESS_KEY, S3EmulatorSupport.ACCESS_KEY);
        props.setProperty(S3Constants.SECRET_KEY, S3EmulatorSupport.SECRET_KEY);
        props.setProperty(S3Constants.S3_END_POINT, "http://127.0.0.1:9090");
        props.setProperty(S3Constants.PATH_STYLE_ACCESS, "true");

        withS3Config(props, () -> Assert.assertTrue(S3DataStoreUtils.isS3EmulatorConfigured()));
    }

    @Test
    public void isS3EmulatorConfiguredReturnsFalseForRealS3Properties() throws IOException {
        Properties props = clientProperties();
        props.setProperty(S3Constants.S3_END_POINT, "https://s3.amazonaws.com");

        withS3Config(props, () -> Assert.assertFalse(S3DataStoreUtils.isS3EmulatorConfigured()));
    }

    private static ClientOverrideConfiguration getClientConfiguration() throws Exception {
        Method method = Utils.class.getDeclaredMethod("getClientConfiguration", Properties.class);
        method.setAccessible(true);
        return (ClientOverrideConfiguration) method.invoke(null, clientProperties());
    }

    private static ClientOverrideConfiguration getClientConfiguration(ScheduledExecutorService timeoutExecutor) throws Exception {
        Method method = Utils.class.getDeclaredMethod("getClientConfiguration", Properties.class, ScheduledExecutorService.class);
        method.setAccessible(true);
        return (ClientOverrideConfiguration) method.invoke(null, clientProperties(), timeoutExecutor);
    }

    private static Properties clientProperties() {
        Properties props = new Properties();
        props.setProperty(S3Constants.ACCESS_KEY, "accessKey");
        props.setProperty(S3Constants.SECRET_KEY, "secretKey");
        props.setProperty(S3Constants.S3_REGION, "us-east-1");
        props.setProperty(S3Constants.S3_MAX_ERR_RETRY, "3");
        props.setProperty(S3Constants.S3_CONN_TIMEOUT, "1000");
        props.setProperty(S3Constants.S3_SOCK_TIMEOUT, "1000");
        props.setProperty(S3Constants.S3_MAX_CONNS, "2");
        props.setProperty(S3Constants.S3_CONN_PROTOCOL, "http");
        props.setProperty(S3Constants.S3_CROSS_REGION_ACCESS, "false");
        return props;
    }

    private static void withS3Config(Properties props, ConfigAssertion assertion) throws IOException {
        File tmp = File.createTempFile("s3test-config", ".properties");
        String previousConfig = System.getProperty("s3.config");
        try {
            try (OutputStream out = new FileOutputStream(tmp)) {
                props.store(out, null);
            }
            System.setProperty("s3.config", tmp.getAbsolutePath());
            assertion.run();
        } finally {
            if (previousConfig == null) {
                System.clearProperty("s3.config");
            } else {
                System.setProperty("s3.config", previousConfig);
            }
            tmp.delete();
        }
    }

    private interface ConfigAssertion {
        void run();
    }

}
