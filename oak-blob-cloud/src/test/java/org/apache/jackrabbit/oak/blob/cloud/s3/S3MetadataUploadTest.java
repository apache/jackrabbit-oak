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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.UUID;

import org.apache.jackrabbit.oak.spi.blob.data.DataRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

/**
 * Tests {@link S3Backend#addMetadataRecord(InputStream, String)} against S3Mock.
 */
public class S3MetadataUploadTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private S3Backend backend;
    private S3Client client;
    private String bucket;

    /** Uses an isolated bucket on the local emulator, without loading AWS credentials. */
    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue("Docker-backed S3 emulator required", S3EmulatorSupport.isAvailable());
        Properties properties = S3EmulatorSupport.getEmulatorProperties();
        bucket = "oak-metadata-" + UUID.randomUUID();
        properties.setProperty(S3Constants.S3_BUCKET, bucket);
        backend = new S3Backend();
        backend.setProperties(properties);
        backend.init();
        client = Utils.openService(properties, false);
    }

    /** Removes the isolated bucket and releases both clients. */
    @After
    public void tearDown() {
        try {
            if (backend != null) {
                backend.close();
            }
        } finally {
            if (client != null) {
                try (S3Client cleanupClient = client) {
                    var objects = cleanupClient.listObjectsV2(request -> request.bucket(bucket)).contents().stream()
                            .map(object -> ObjectIdentifier.builder().key(object.key()).build()).toList();
                    if (!objects.isEmpty()) {
                        cleanupClient.deleteObjects(request -> request.bucket(bucket)
                                .delete(Delete.builder().objects(objects).build()));
                    }
                    cleanupClient.deleteBucket(request -> request.bucket(bucket));
                }
            }
        }
    }

    /** Byte-array metadata supplies an exact nonempty length to the SDK. */
    @Test
    public void uploadsByteArrayMetadata() throws Exception {
        byte[] expected = new byte[16384];
        expected[8192] = 47;
        assertUpload(new ByteArrayInputStream(expected), expected);
    }

    /** Empty metadata supplies zero rather than an unknown content length. */
    @Test
    public void uploadsEmptyMetadata() throws Exception {
        assertUpload(new ByteArrayInputStream(new byte[0]), new byte[0]);
    }

    /** File-backed metadata is read and uploaded with its known length. */
    @Test
    public void uploadsFileMetadata() throws Exception {
        byte[] expected = { 1, 2, 3, 4 };
        var file = folder.newFile().toPath();
        Files.write(file, expected);
        assertUpload(new FileInputStream(file.toFile()), expected);
    }

    /** Mark/reset streams retain their bytes after the length probe. */
    @Test
    public void uploadsBufferedMetadata() throws Exception {
        byte[] expected = { 5, 6, 7, 8 };
        assertUpload(new BufferedInputStream(new ByteArrayInputStream(expected)), expected);
    }

    /** Streams without mark/reset support exercise the temporary-file upload path. */
    @Test
    public void uploadsNonResettableMetadata() throws Exception {
        byte[] expected = { 9, 10, 11, 12 };
        InputStream input = new FilterInputStream(new ByteArrayInputStream(expected)) {
            @Override
            public boolean markSupported() {
                return false;
            }
        };
        assertUpload(input, expected);
    }

    private void assertUpload(InputStream input, byte[] expected) throws Exception {
        try (input) {
            backend.addMetadataRecord(input, "repositoryId");
        }
        DataRecord record = backend.getMetadataRecord("repositoryId");
        Assert.assertNotNull(record);
        Assert.assertEquals(expected.length, record.getLength());
        try (InputStream downloaded = record.getStream()) {
            Assert.assertArrayEquals(expected, downloaded.readAllBytes());
        }
    }
}
