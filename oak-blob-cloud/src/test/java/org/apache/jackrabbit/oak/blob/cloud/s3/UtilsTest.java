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
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.util.Properties;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        Assert.assertEquals("eu-west-1", Utils.getRegionFromEndpoint("https://s3.eu-west-1.amazonaws.com"));
    }

    @Test
    public void testGetRegionFromVirtualHostedEndpoint() {
        Assert.assertEquals("ap-south-1", Utils.getRegionFromEndpoint("https://bucket.s3.ap-south-1.amazonaws.com"));
    }

    @Test
    public void testGetRegionFromUsEast1Endpoint() {
        Assert.assertEquals("us-east-1", Utils.getRegionFromEndpoint("https://s3.amazonaws.com"));
    }

    @Test
    public void testGetRegionFromVirtualHostedUsEast1() {
        Assert.assertEquals("us-east-1", Utils.getRegionFromEndpoint("https://bucket.s3.amazonaws.com"));
    }

    @Test
    public void testGetRegionFromInvalidEndpoint() {
        Assert.assertNull(Utils.getRegionFromEndpoint("https://example.com"));
    }

    @Test
    public void testGetRegionFromMalformedEndpoint() {
        Assert.assertNull(Utils.getRegionFromEndpoint("not-a-valid-uri"));
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
    public void testBucketExistsTrue() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        // No exception means bucket exists
        assertTrue(Utils.bucketExists(mockClient, Mockito.anyString()));
        Mockito.verify(mockClient).headBucket(Mockito.any(Consumer.class));
    }

    @Test
    public void testBucketExistsFalse() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        Mockito.doThrow(NoSuchBucketException.builder().build())
                .when(mockClient).headBucket(Mockito.any(Consumer.class));
        assertFalse(Utils.bucketExists(mockClient, Mockito.anyString()));
        Mockito.verify(mockClient).headBucket(Mockito.any(Consumer.class));
    }

    @Test
    public void testObjectExistsTrue() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        S3RequestDecorator decorator = Mockito.mock(S3RequestDecorator.class);

        // identity decorator
        Mockito.when(decorator.decorate(Mockito.any(HeadObjectRequest.class))).thenReturn(Mockito.mock(HeadObjectRequest.class));
        assertTrue(Utils.objectExists(mockClient, "bucket", "key", decorator));
        Mockito.verify(mockClient).headObject(Mockito.any(HeadObjectRequest.class));
    }

    @Test
    public void testObjectExistsNotFound() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        S3RequestDecorator decorator = Mockito.mock(S3RequestDecorator.class);

        // identity decorator
        Mockito.when(decorator.decorate(Mockito.any(HeadObjectRequest.class))).thenReturn(Mockito.mock(HeadObjectRequest.class));
        AwsServiceException notFound = S3Exception.builder().statusCode(404).build();
        Mockito.doThrow(notFound).when(mockClient).headObject(Mockito.any(HeadObjectRequest.class));
        assertFalse(Utils.objectExists(mockClient, "bucket", "key", decorator));
    }

    @Test
    public void testObjectExistsForbidden() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        S3RequestDecorator decorator = Mockito.mock(S3RequestDecorator.class);

        // identity decorator
        Mockito.when(decorator.decorate(Mockito.any(HeadObjectRequest.class))).thenReturn(Mockito.mock(HeadObjectRequest.class));
        AwsServiceException forbidden = S3Exception.builder().statusCode(403).build();
        Mockito.doThrow(forbidden).when(mockClient).headObject(Mockito.any(HeadObjectRequest.class));
        assertFalse(Utils.objectExists(mockClient, "bucket", "key", decorator));
    }

    @Test(expected = S3Exception.class)
    public void testObjectExistsOtherException() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        S3RequestDecorator decorator = Mockito.mock(S3RequestDecorator.class);

        // identity decorator
        Mockito.when(decorator.decorate(Mockito.any(HeadObjectRequest.class))).thenReturn(Mockito.mock(HeadObjectRequest.class));
        AwsServiceException other = S3Exception.builder().statusCode(500).build();
        Mockito.doThrow(other).when(mockClient).headObject(Mockito.any(HeadObjectRequest.class));
        Utils.objectExists(mockClient, "bucket", "key", decorator);
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

}