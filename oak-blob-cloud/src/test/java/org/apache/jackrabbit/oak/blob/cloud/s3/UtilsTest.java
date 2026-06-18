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
import java.net.URI;
import java.util.Properties;

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

}
