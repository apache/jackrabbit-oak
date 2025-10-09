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

import org.junit.Test;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.utils.BinaryUtils;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit cases for S3RequestDecorator
 */
public class S3RequestDecoratorTest {

    private final String sseCustomerKey = BinaryUtils.toBase64("customer-key".getBytes(StandardCharsets.UTF_8));
    private final String kmsKeyId = "kms-key-id";

    @Test
    public void testConstructorWithNoneEncryption() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "NONE");
        S3RequestDecorator decorator = new S3RequestDecorator(props);
        assertEquals(DataEncryption.NONE, decorator.dataEncryption);
        assertNull(decorator.sseKmsKey);
        assertNull(decorator.sseCustomerKey);
    }

    @Test
    public void testConstructorWithSSES3Encryption() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_S3");
        S3RequestDecorator decorator = new S3RequestDecorator(props);
        assertEquals(DataEncryption.SSE_S3, decorator.dataEncryption);
        assertNull(decorator.sseKmsKey);
        assertNull(decorator.sseCustomerKey);
    }

    @Test
    public void testConstructorWithSSEKMSEncryptionWithKey() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_KMS");
        props.setProperty(S3Constants.S3_SSE_KMS_KEYID, "kms-key-id");
        S3RequestDecorator decorator = new S3RequestDecorator(props);
        assertEquals(DataEncryption.SSE_KMS, decorator.dataEncryption);
        assertEquals("kms-key-id", decorator.sseKmsKey);
        assertNull(decorator.sseCustomerKey);
    }

    @Test
    public void testConstructorWithSSEKMSEncryptionWithoutKey() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_KMS");
        S3RequestDecorator decorator = new S3RequestDecorator(props);
        assertEquals(DataEncryption.SSE_KMS, decorator.dataEncryption);
        assertNull(decorator.sseKmsKey);
        assertNull(decorator.sseCustomerKey);
    }

    @Test
    public void testConstructorWithSSECEncryptionWithKey() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_C");
        props.setProperty(S3Constants.S3_SSE_C_KEY, "customer-key");
        S3RequestDecorator decorator = new S3RequestDecorator(props);
        assertEquals(DataEncryption.SSE_C, decorator.dataEncryption);
        assertNull(decorator.sseKmsKey);
        assertEquals("customer-key", decorator.sseCustomerKey);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithSSECEncryptionWithoutKey() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_C");
        new S3RequestDecorator(props);
    }

    @Test
    public void testConstructorWithNullEncryptionType() {
        Properties props = new Properties();
        S3RequestDecorator decorator = new S3RequestDecorator(props);
        assertEquals(DataEncryption.NONE, decorator.dataEncryption);
        assertNull(decorator.sseKmsKey);
        assertNull(decorator.sseCustomerKey);
    }

    @Test
    public void testDecorateHeadObjectWithSSEC() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_C");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        HeadObjectRequest req = HeadObjectRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        HeadObjectRequest decorated = decorator.decorate(req);

        assertEquals(sseCustomerKey, decorated.sseCustomerKey());
        assertEquals("AES256", decorated.sseCustomerAlgorithm());
        assertEquals(Utils.calculateMD5(sseCustomerKey), decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecorateHeadObjectWithNone() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "NONE");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        HeadObjectRequest req = HeadObjectRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        HeadObjectRequest decorated = decorator.decorate(req);

        assertNull(decorated.sseCustomerKey());
        assertNull(decorated.sseCustomerAlgorithm());
        assertNull(decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecorateGetObjectWithSSEC() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_C");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        GetObjectRequest req = GetObjectRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        GetObjectRequest decorated = decorator.decorate(req);

        assertEquals(sseCustomerKey, decorated.sseCustomerKey());
        assertEquals("AES256", decorated.sseCustomerAlgorithm());
        assertEquals(Utils.calculateMD5(sseCustomerKey), decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecorateGetObjectWithNone() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "NONE");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        GetObjectRequest req = GetObjectRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        GetObjectRequest decorated = decorator.decorate(req);

        assertNull(decorated.sseCustomerKey());
        assertNull(decorated.sseCustomerAlgorithm());
        assertNull(decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecorateCompleteMultipartUploadWithSSEC() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_C");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CompleteMultipartUploadRequest req = CompleteMultipartUploadRequest.builder()
                .bucket("bucket")
                .key("key")
                .uploadId("uploadId")
                .build();

        CompleteMultipartUploadRequest decorated = decorator.decorate(req);

        assertEquals(sseCustomerKey, decorated.sseCustomerKey());
        assertEquals("AES256", decorated.sseCustomerAlgorithm());
        assertEquals(Utils.calculateMD5(sseCustomerKey), decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
        assertEquals("uploadId", decorated.uploadId());
    }

    @Test
    public void testDecorateCompleteMultipartUploadRequestWithNone() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "NONE");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CompleteMultipartUploadRequest req = CompleteMultipartUploadRequest.builder()
                .bucket("bucket")
                .key("key")
                .uploadId("uploadId")
                .build();

        CompleteMultipartUploadRequest decorated = decorator.decorate(req);

        assertNull(decorated.sseCustomerKey());
        assertNull(decorated.sseCustomerAlgorithm());
        assertNull(decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
        assertEquals("uploadId", decorated.uploadId());
    }

    @Test
    public void testDecoratePutObjectWithSSES3() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_S3");
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        PutObjectRequest decorated = decorator.decorate(req);

        assertEquals(ServerSideEncryption.AES256, decorated.serverSideEncryption());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecoratePutObjectWithSSEKMS() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_KMS");
        props.setProperty(S3Constants.S3_SSE_KMS_KEYID, kmsKeyId);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        PutObjectRequest decorated = decorator.decorate(req);

        assertEquals(ServerSideEncryption.AWS_KMS, decorated.serverSideEncryption());
        assertEquals(kmsKeyId, decorated.ssekmsKeyId());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecoratePutObjectWithSSEC() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_C");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        PutObjectRequest decorated = decorator.decorate(req);

        assertEquals("AES256", decorated.sseCustomerAlgorithm());
        assertEquals(sseCustomerKey, decorated.sseCustomerKey());
        assertEquals(Utils.calculateMD5(sseCustomerKey), decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecoratePutObjectWithNone() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "NONE");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        PutObjectRequest decorated = decorator.decorate(req);

        assertNull(decorated.serverSideEncryption());
        assertNull(decorated.ssekmsKeyId());
        assertNull(decorated.sseCustomerAlgorithm());
        assertNull(decorated.sseCustomerKey());
        assertNull(decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecorateCopyObjectWithSSES3() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_S3");
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CopyObjectRequest req = CopyObjectRequest.builder()
                .destinationBucket("bucket")
                .destinationKey("key")
                .sourceBucket("src-bucket")
                .sourceKey("src-key")
                .build();

        CopyObjectRequest decorated = decorator.decorate(req);

        assertEquals(ServerSideEncryption.AES256, decorated.serverSideEncryption());
        assertEquals("bucket", decorated.destinationBucket());
        assertEquals("key", decorated.destinationKey());
        assertEquals("src-bucket", decorated.sourceBucket());
        assertEquals("src-key", decorated.sourceKey());
    }

    @Test
    public void testDecorateCopyObjectWithSSEKMS() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_KMS");
        props.setProperty(S3Constants.S3_SSE_KMS_KEYID, kmsKeyId);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CopyObjectRequest req = CopyObjectRequest.builder()
                .destinationBucket("bucket")
                .destinationKey("key")
                .sourceBucket("src-bucket")
                .sourceKey("src-key")
                .build();

        CopyObjectRequest decorated = decorator.decorate(req);

        assertEquals(ServerSideEncryption.AWS_KMS, decorated.serverSideEncryption());
        assertEquals(kmsKeyId, decorated.ssekmsKeyId());
        assertEquals("bucket", decorated.destinationBucket());
        assertEquals("key", decorated.destinationKey());
        assertEquals("src-bucket", decorated.sourceBucket());
        assertEquals("src-key", decorated.sourceKey());
    }

    @Test
    public void testDecorateCopyObjectWithSSEC() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_C");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CopyObjectRequest req = CopyObjectRequest.builder()
                .destinationBucket("bucket")
                .destinationKey("key")
                .sourceBucket("src-bucket")
                .sourceKey("src-key")
                .build();

        CopyObjectRequest decorated = decorator.decorate(req);

        assertEquals("AES256", decorated.sseCustomerAlgorithm());
        assertEquals("AES256", decorated.copySourceSSECustomerAlgorithm());
        assertEquals(sseCustomerKey, decorated.sseCustomerKey());
        assertEquals(sseCustomerKey, decorated.copySourceSSECustomerKey());
        assertEquals(Utils.calculateMD5(sseCustomerKey), decorated.sseCustomerKeyMD5());
        assertEquals(Utils.calculateMD5(sseCustomerKey), decorated.copySourceSSECustomerKeyMD5());
        assertEquals("bucket", decorated.destinationBucket());
        assertEquals("key", decorated.destinationKey());
        assertEquals("src-bucket", decorated.sourceBucket());
        assertEquals("src-key", decorated.sourceKey());
    }

    @Test
    public void testDecorateCopyObjectWithNone() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "NONE");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CopyObjectRequest req = CopyObjectRequest.builder()
                .destinationBucket("bucket")
                .destinationKey("key")
                .sourceBucket("src-bucket")
                .sourceKey("src-key")
                .build();

        CopyObjectRequest decorated = decorator.decorate(req);

        assertNull(decorated.serverSideEncryption());
        assertNull(decorated.ssekmsKeyId());
        assertNull(decorated.sseCustomerAlgorithm());
        assertNull(decorated.copySourceSSECustomerAlgorithm());
        assertNull(decorated.sseCustomerKey());
        assertNull(decorated.copySourceSSECustomerKey());
        assertNull(decorated.sseCustomerKeyMD5());
        assertNull(decorated.copySourceSSECustomerKeyMD5());
        assertEquals("bucket", decorated.destinationBucket());
        assertEquals("key", decorated.destinationKey());
        assertEquals("src-bucket", decorated.sourceBucket());
        assertEquals("src-key", decorated.sourceKey());
    }

    @Test
    public void testDecorateCreateMultipartUploadWithSSES3() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_S3");
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CreateMultipartUploadRequest req = CreateMultipartUploadRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        CreateMultipartUploadRequest decorated = decorator.decorate(req);

        assertEquals(ServerSideEncryption.AES256, decorated.serverSideEncryption());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecorateCreateMultipartUploadWithSSEKMS() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_KMS");
        props.setProperty(S3Constants.S3_SSE_KMS_KEYID, kmsKeyId);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CreateMultipartUploadRequest req = CreateMultipartUploadRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        CreateMultipartUploadRequest decorated = decorator.decorate(req);

        assertEquals(ServerSideEncryption.AWS_KMS, decorated.serverSideEncryption());
        assertEquals(kmsKeyId, decorated.ssekmsKeyId());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecorateCreateMultipartUploadWithSSEC() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "SSE_C");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CreateMultipartUploadRequest req = CreateMultipartUploadRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        CreateMultipartUploadRequest decorated = decorator.decorate(req);

        assertEquals("AES256", decorated.sseCustomerAlgorithm());
        assertEquals(sseCustomerKey, decorated.sseCustomerKey());
        assertEquals(Utils.calculateMD5(sseCustomerKey), decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

    @Test
    public void testDecorateCreateMultipartUploadWithNone() {
        Properties props = new Properties();
        props.setProperty(S3Constants.S3_ENCRYPTION, "NONE");
        props.setProperty(S3Constants.S3_SSE_C_KEY, sseCustomerKey);
        S3RequestDecorator decorator = new S3RequestDecorator(props);

        CreateMultipartUploadRequest req = CreateMultipartUploadRequest.builder()
                .bucket("bucket")
                .key("key")
                .build();

        CreateMultipartUploadRequest decorated = decorator.decorate(req);

        assertNull(decorated.serverSideEncryption());
        assertNull(decorated.ssekmsKeyId());
        assertNull(decorated.sseCustomerAlgorithm());
        assertNull(decorated.sseCustomerKey());
        assertNull(decorated.sseCustomerKeyMD5());
        assertEquals("bucket", decorated.bucket());
        assertEquals("key", decorated.key());
    }

}