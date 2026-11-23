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

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAccelerateStatus;
import software.amazon.awssdk.services.s3.model.GetBucketAccelerateConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAccelerateConfigurationResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/**
 * Tests credential ownership and presigner replacement in {@link S3Backend}.
 */
public class S3CredentialsLifecycleTest {

    /** The SDK closes the credentials attached to a synchronous S3 client. */
    @Test
    public void syncClientClosesCredentials() {
        ImdsV2CredentialsProvider credentials = Mockito.mock(ImdsV2CredentialsProvider.class);
        S3Client client = S3Client.builder().region(Region.US_EAST_1)
                .credentialsProvider(credentials).build();
        client.close();
        Mockito.verify(credentials, Mockito.atLeastOnce()).close();
    }

    /** The SDK closes the credentials attached to an asynchronous S3 client. */
    @Test
    public void asyncClientClosesCredentials() {
        ImdsV2CredentialsProvider credentials = Mockito.mock(ImdsV2CredentialsProvider.class);
        S3AsyncClient client = S3AsyncClient.builder().region(Region.US_EAST_1)
                .credentialsProvider(credentials).build();
        client.close();
        Mockito.verify(credentials, Mockito.atLeastOnce()).close();
    }

    /** The SDK closes the credentials attached to an S3 presigner. */
    @Test
    public void presignerClosesCredentials() {
        ImdsV2CredentialsProvider credentials = Mockito.mock(ImdsV2CredentialsProvider.class);
        S3Presigner presigner = S3Presigner.builder().region(Region.US_EAST_1)
                .credentialsProvider(credentials).build();
        presigner.close();
        Mockito.verify(credentials, Mockito.atLeastOnce()).close();
    }

    /** Reconfiguration closes the old presigner after creating its replacement. */
    @Test
    public void reconfigurationClosesPreviousPresigner() {
        S3Backend backend = new S3Backend();
        Properties properties = new Properties();
        backend.setProperties(properties);
        S3Presigner original = Mockito.mock(S3Presigner.class);
        S3Presigner replacement = Mockito.mock(S3Presigner.class);
        try (MockedStatic<Utils> utilities = Mockito.mockStatic(Utils.class)) {
            utilities.when(() -> Utils.createPresigner(null, properties, false))
                    .thenReturn(original).thenAnswer(invocation -> {
                        Mockito.verify(original, Mockito.never()).close();
                        return replacement;
                    });
            backend.setBinaryTransferAccelerationEnabled(false);
            Mockito.verify(original, Mockito.never()).close();
            backend.setBinaryTransferAccelerationEnabled(false);
            Mockito.verify(original).close();
            Mockito.verify(replacement, Mockito.never()).close();
        }
    }

    /** Replacement can still sign downloads without closing the shared main client. */
    @Test
    public void reconfigurationKeepsMainClientAndSigning() throws Exception {
        S3Client client = Mockito.mock(S3Client.class);
        Properties properties = signingProperties();
        S3Backend backend = signingBackend(client, properties);
        Mockito.when(client.getBucketAccelerateConfiguration(
                Mockito.<Consumer<GetBucketAccelerateConfigurationRequest.Builder>>any()))
                .thenReturn(GetBucketAccelerateConfigurationResponse.builder()
                        .status(BucketAccelerateStatus.SUSPENDED).build());
        try {
            backend.setBinaryTransferAccelerationEnabled(false);
            assertDownloadUri(backend);
            backend.setBinaryTransferAccelerationEnabled(true);
            assertDownloadUri(backend);
            Mockito.verify(client, Mockito.never()).close();
            Mockito.verify(client, Mockito.times(2)).headObject(Mockito.any(HeadObjectRequest.class));
        } finally {
            backend.close();
        }
    }

    /** A failed bucket configuration lookup must leave the working signer available. */
    @Test
    public void failedAccelerationCheckPreservesSigning() throws Exception {
        S3Client client = Mockito.mock(S3Client.class);
        S3Backend backend = signingBackend(client, signingProperties());
        SdkClientException failure = SdkClientException.create("Acceleration lookup failed");
        Mockito.when(client.getBucketAccelerateConfiguration(
                Mockito.<Consumer<GetBucketAccelerateConfigurationRequest.Builder>>any())).thenThrow(failure);
        try {
            backend.setBinaryTransferAccelerationEnabled(false);
            assertDownloadUri(backend);
            Assert.assertSame(failure, Assert.assertThrows(SdkClientException.class,
                    () -> backend.setBinaryTransferAccelerationEnabled(true)));
            assertDownloadUri(backend);
            Mockito.verify(client, Mockito.never()).close();
        } finally {
            backend.close();
        }
    }

    /** A failed replacement build must not discard the working signer. */
    @Test
    public void failedPresignerConstructionPreservesSigning() throws Exception {
        S3Client client = Mockito.mock(S3Client.class);
        Properties properties = signingProperties();
        S3Backend backend = signingBackend(client, properties);
        try (S3Presigner original = Utils.createPresigner(client, properties);
             MockedStatic<Utils> utilities = Mockito.mockStatic(Utils.class)) {
            SdkClientException failure = SdkClientException.create("Presigner creation failed");
            utilities.when(() -> Utils.createPresigner(client, properties, false))
                    .thenReturn(original).thenThrow(failure);
            backend.setBinaryTransferAccelerationEnabled(false);
            Assert.assertSame(failure, Assert.assertThrows(SdkClientException.class,
                    () -> backend.setBinaryTransferAccelerationEnabled(false)));
            assertDownloadUri(backend);
            Mockito.verify(client, Mockito.never()).close();
        } finally {
            backend.close();
        }
    }

    /** A failed accelerated signer build releases its new client and retains working signing. */
    @Test
    public void failedAcceleratedPresignerClosesNewClient() throws Exception {
        S3Client client = Mockito.mock(S3Client.class);
        S3Client acceleratedClient = Mockito.mock(S3Client.class);
        Properties properties = signingProperties();
        S3Backend backend = signingBackend(client, properties);
        Mockito.when(client.getBucketAccelerateConfiguration(
                Mockito.<Consumer<GetBucketAccelerateConfigurationRequest.Builder>>any()))
                .thenReturn(GetBucketAccelerateConfigurationResponse.builder()
                        .status(BucketAccelerateStatus.ENABLED).build());
        try (S3Presigner original = Utils.createPresigner(client, properties);
             MockedStatic<Utils> utilities = Mockito.mockStatic(Utils.class)) {
            utilities.when(() -> Utils.createPresigner(client, properties, false)).thenReturn(original);
            utilities.when(() -> Utils.openService(properties, true)).thenReturn(acceleratedClient);
            SdkClientException failure = SdkClientException.create("Accelerated presigner creation failed");
            utilities.when(() -> Utils.createPresigner(acceleratedClient, properties, true)).thenThrow(failure);
            backend.setBinaryTransferAccelerationEnabled(false);
            Assert.assertSame(failure, Assert.assertThrows(SdkClientException.class,
                    () -> backend.setBinaryTransferAccelerationEnabled(true)));
            Mockito.verify(acceleratedClient).close();
            assertDownloadUri(backend);
            Mockito.verify(client, Mockito.never()).close();
        } finally {
            backend.close();
        }
    }

    /** Disabling acceleration closes its dedicated client while retaining the main client. */
    @Test
    public void disablingAccelerationClosesDedicatedClient() throws Exception {
        S3Client client = Mockito.mock(S3Client.class);
        S3Client acceleratedClient = Mockito.mock(S3Client.class);
        Properties properties = signingProperties();
        S3Backend backend = signingBackend(client, properties);
        S3Presigner acceleratedSigner = Mockito.mock(S3Presigner.class);
        S3Presigner replacement = Mockito.mock(S3Presigner.class);
        Mockito.when(client.getBucketAccelerateConfiguration(
                Mockito.<Consumer<GetBucketAccelerateConfigurationRequest.Builder>>any()))
                .thenReturn(GetBucketAccelerateConfigurationResponse.builder()
                        .status(BucketAccelerateStatus.ENABLED).build());
        try (MockedStatic<Utils> utilities = Mockito.mockStatic(Utils.class)) {
            utilities.when(() -> Utils.openService(properties, true)).thenReturn(acceleratedClient);
            utilities.when(() -> Utils.createPresigner(acceleratedClient, properties, true))
                    .thenReturn(acceleratedSigner);
            utilities.when(() -> Utils.createPresigner(client, properties, false)).thenReturn(replacement);
            backend.setBinaryTransferAccelerationEnabled(true);
            Mockito.verify(acceleratedClient, Mockito.never()).close();
            backend.setBinaryTransferAccelerationEnabled(false);
            Mockito.verify(acceleratedSigner).close();
            Mockito.verify(acceleratedClient).close();
            Mockito.verify(replacement, Mockito.never()).close();
            Mockito.verify(client, Mockito.never()).close();
        } finally {
            backend.close();
        }
    }

    private Properties signingProperties() {
        Properties properties = new Properties();
        properties.setProperty(S3Constants.ACCESS_KEY, "test-access-key");
        properties.setProperty(S3Constants.SECRET_KEY, "test-secret-key");
        properties.setProperty(S3Constants.S3_REGION, "us-east-1");
        properties.setProperty(S3Constants.S3_END_POINT, "http://127.0.0.1:9090");
        properties.setProperty(S3Constants.PATH_STYLE_ACCESS, "true");
        return properties;
    }

    private S3Backend signingBackend(S3Client client, Properties properties) throws ReflectiveOperationException {
        S3Backend backend = new S3Backend();
        backend.setProperties(properties);
        backend.setBucket("bucket");
        backend.setHttpDownloadURIExpirySeconds(300);
        Field clientField = S3Backend.class.getDeclaredField("s3Client");
        clientField.setAccessible(true);
        clientField.set(backend, client);
        Field decoratorField = S3Backend.class.getDeclaredField("s3ReqDecorator");
        decoratorField.setAccessible(true);
        decoratorField.set(backend, new S3RequestDecorator(properties));
        Mockito.when(client.headBucket(Mockito.<Consumer<HeadBucketRequest.Builder>>any()))
                .thenThrow(NoSuchBucketException.builder().build());
        return backend;
    }

    private void assertDownloadUri(S3Backend backend) {
        URI uri = backend.createHttpDownloadURI(new DataIdentifier("0123456789abcdef"),
                DataRecordDownloadOptions.DEFAULT);
        Assert.assertNotNull(uri);
        Assert.assertEquals("127.0.0.1", uri.getHost());
        Assert.assertEquals(9090, uri.getPort());
        Assert.assertEquals("/bucket/0123-456789abcdef", uri.getPath());
        Assert.assertTrue(uri.getRawQuery().contains("X-Amz-Signature="));
    }

}
