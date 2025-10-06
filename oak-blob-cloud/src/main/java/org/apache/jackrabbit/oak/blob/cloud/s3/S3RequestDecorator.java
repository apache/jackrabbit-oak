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

import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.utils.StringUtils;

import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION_SSE_C;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION_SSE_KMS;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_SSE_C_KEY;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_SSE_KMS_KEYID;

/**
 * This class to sets encryption mode related properties for S3 request.
 *
 */
public class S3RequestDecorator {
    DataEncryption dataEncryption = DataEncryption.NONE;
    String sseKmsKey;
    String sseCustomerKey;

    public S3RequestDecorator(Properties props) {
        final String encryptionType = props.getProperty(S3_ENCRYPTION);
        if (encryptionType != null) {
            this.dataEncryption = DataEncryption.valueOf(encryptionType);

            switch (encryptionType) {
                case S3_ENCRYPTION_SSE_KMS: {
                    final String keyId = props.getProperty(S3_SSE_KMS_KEYID);
                    if (StringUtils.isNotBlank(keyId)) {
                        sseKmsKey = keyId;
                    }
                    break;
                }
                case S3_ENCRYPTION_SSE_C: {
                    final String keyId = props.getProperty(S3_SSE_C_KEY);
                    if (StringUtils.isNotBlank(keyId)) {
                        sseCustomerKey = keyId;
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }

    /**
     * Set encryption in {@link HeadObjectRequest}
     */
    public HeadObjectRequest decorate(final HeadObjectRequest request) {
        if (requireNonNull(getDataEncryption()) == DataEncryption.SSE_C) {
            // Assume sseCustomerKey is already of type software.amazon.awssdk.services.s3.model.SseCustomerKey
            return request.toBuilder()
                    .sseCustomerKey(sseCustomerKey)
                    .sseCustomerAlgorithm(ServerSideEncryption.AES256.toString())
                    .sseCustomerKeyMD5(Utils.calculateMD5(sseCustomerKey))
                    .build();
        }
        return request;
    }

    /**
     * Set encryption in {@link GetObjectRequest}
     */
    public GetObjectRequest decorate(final GetObjectRequest request) {
        if (requireNonNull(getDataEncryption()) == DataEncryption.SSE_C) {
            return request.toBuilder()
                    .sseCustomerAlgorithm(ServerSideEncryption.AES256.toString())
                    .sseCustomerKey(sseCustomerKey)
                    .sseCustomerKeyMD5(Utils.calculateMD5(sseCustomerKey))
                    .build();
        }
        return request;
    }

    /**
     * Set encryption in {@link CompleteMultipartUploadRequest}
     */
    public CompleteMultipartUploadRequest decorate(final CompleteMultipartUploadRequest request) {
        if (requireNonNull(getDataEncryption()) == DataEncryption.SSE_C) {
            return request.toBuilder()
                    .sseCustomerAlgorithm(ServerSideEncryption.AES256.toString())
                    .sseCustomerKey(sseCustomerKey)
                    .sseCustomerKeyMD5(Utils.calculateMD5(sseCustomerKey))
                    .build();
        }
        return request;
    }

    /**
     * Set encryption in {@link PutObjectRequest}
     */
    public PutObjectRequest decorate(PutObjectRequest request) {
        PutObjectRequest.Builder builder = request.toBuilder();

        DataEncryption encryption = getDataEncryption();

        switch (encryption) {
            case SSE_S3:
                builder.serverSideEncryption(ServerSideEncryption.AES256);
                break;
            case SSE_KMS:
                builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
                if (sseKmsKey != null) {
                    builder.ssekmsKeyId(sseKmsKey);
                }
                break;
            case SSE_C:
                builder.sseCustomerAlgorithm(ServerSideEncryption.AES256.toString());
                if (sseCustomerKey != null) {
                    builder.sseCustomerKey(sseCustomerKey);
                    builder.sseCustomerKeyMD5(Utils.calculateMD5(sseCustomerKey));
                }
                break;
            case NONE:
                break;
        }
        return builder.build();
    }

    /**
     * Set encryption in {@link CopyObjectRequest}
     */
    public CopyObjectRequest decorate(CopyObjectRequest request) {

        CopyObjectRequest.Builder builder = request.toBuilder();

        switch (getDataEncryption()) {
            case SSE_S3:
                builder.serverSideEncryption(ServerSideEncryption.AES256);
                break;
            case SSE_KMS:
                builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
                if (sseKmsKey != null) {
                    // sseParams is typically a KMS Key ID string in SDK 2.x.
                    builder.ssekmsKeyId(sseKmsKey);
                }
                break;
            case SSE_C:
                builder.sseCustomerAlgorithm(ServerSideEncryption.AES256.toString());
                builder.copySourceSSECustomerAlgorithm(ServerSideEncryption.AES256.toString());
                if (sseCustomerKey != null) {
                    builder.sseCustomerKey(sseCustomerKey)
                            .copySourceSSECustomerKey(sseCustomerKey)
                            .copySourceSSECustomerKeyMD5(Utils.calculateMD5(sseCustomerKey))
                            .sseCustomerKeyMD5(Utils.calculateMD5(sseCustomerKey));
                }
                break;
            case NONE:
                break;
        }
        return builder.build();
    }

    public CreateMultipartUploadRequest decorate(CreateMultipartUploadRequest request) {

        CreateMultipartUploadRequest.Builder builder = request.toBuilder();

        switch (getDataEncryption()) {
            case SSE_S3:
                builder.serverSideEncryption(ServerSideEncryption.AES256);
                break;
            case SSE_KMS:
                builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
                if (sseKmsKey != null) {
                    builder.ssekmsKeyId(sseKmsKey);
                }
                break;
            case SSE_C:
                builder.sseCustomerAlgorithm(ServerSideEncryption.AES256.toString());
                if (sseCustomerKey != null) {
                    builder.sseCustomerKey(sseCustomerKey);
                    builder.sseCustomerKeyMD5(Utils.calculateMD5(sseCustomerKey));
                }
                break;
            case NONE:
                break;
        }
        return builder.build();
    }

    private DataEncryption getDataEncryption() {
        return this.dataEncryption;
    }

}
