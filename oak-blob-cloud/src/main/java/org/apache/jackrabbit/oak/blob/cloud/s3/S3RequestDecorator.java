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

import java.util.Properties;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;

import static com.amazonaws.HttpMethod.GET;
import static com.amazonaws.services.s3.model.SSEAlgorithm.AES256;
import static com.amazonaws.util.StringUtils.hasValue;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION_SSE_C;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION_SSE_KMS;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_SSE_C_KEYID;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_SSE_KMS_KEYID;

/**
 * This class to sets encrption mode in S3 request.
 *
 */
public class S3RequestDecorator {
    DataEncryption dataEncryption = DataEncryption.NONE;
    SSEAwsKeyManagementParams sseParams;

    SSECustomerKey sseCustomerKey;

    public S3RequestDecorator(Properties props) {
        final String encryptionType = props.getProperty(S3_ENCRYPTION);
        if (encryptionType != null) {
            this.dataEncryption = DataEncryption.valueOf(encryptionType);

            switch (encryptionType) {
                case S3_ENCRYPTION_SSE_KMS: {
                    final String keyId = props.getProperty(S3_SSE_KMS_KEYID);
                    sseParams = new SSEAwsKeyManagementParams();
                    if (hasValue(keyId)) {
                        sseParams.withAwsKmsKeyId(keyId);
                    }
                    break;
                }
                case S3_ENCRYPTION_SSE_C: {
                    final String keyId = props.getProperty(S3_SSE_C_KEYID);
                    if (hasValue(keyId)) {
                        sseCustomerKey = new SSECustomerKey(keyId);
                    }
                    break;
                }
            }
        }
    }

    /**
     * Set encryption in {@link GetObjectMetadataRequest}
     */
    public GetObjectMetadataRequest decorate(final GetObjectMetadataRequest request) {
        switch (getDataEncryption()) {
            case SSE_C:
                request.withSSECustomerKey(sseCustomerKey);
                break;
            case NONE:
                break;
        }
        return request;
    }

    /**
     * Set encryption in {@link GetObjectRequest}
     */
    public GetObjectRequest decorate(final GetObjectRequest request) {
        switch (getDataEncryption()) {
            case SSE_C:
                request.withSSECustomerKey(sseCustomerKey);
                break;
            case NONE:
                break;
        }
        return request;
    }

    /**
     * Set encryption in {@link PutObjectRequest}
     */
    public PutObjectRequest decorate(PutObjectRequest request) {
        ObjectMetadata metadata = request.getMetadata() == null
                                      ? new ObjectMetadata()
                                      : request.getMetadata();
        switch (getDataEncryption()) {
            case SSE_S3:
                metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                break;
            case SSE_KMS:
                metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm());
                /*Set*/
                request.withSSEAwsKeyManagementParams(sseParams);
                break;
            case SSE_C:
                request.withSSECustomerKey(sseCustomerKey);
                break;
            case NONE:
                break;
        }
        request.setMetadata(metadata);
        return request;
    }

    /**
     * Set encryption in {@link CopyObjectRequest}
     */
    public CopyObjectRequest decorate(CopyObjectRequest request) {
        ObjectMetadata metadata = request.getNewObjectMetadata() == null
                                      ? new ObjectMetadata()
                                      : request.getNewObjectMetadata();;
        switch (getDataEncryption()) {
            case SSE_S3:
                metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                break;
            case SSE_KMS:
                metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm());
                request.withSSEAwsKeyManagementParams(sseParams);
                break;
            case SSE_C:
                metadata.setSSEAlgorithm(AES256.getAlgorithm());
                request.withSourceSSECustomerKey(sseCustomerKey).withDestinationSSECustomerKey(sseCustomerKey);
                break;
            case NONE:
                break;
        }
        request.setNewObjectMetadata(metadata);
        return request;
    }

    public InitiateMultipartUploadRequest decorate(InitiateMultipartUploadRequest request) {
        ObjectMetadata metadata = request.getObjectMetadata() == null
                                      ? new ObjectMetadata()
                                      : request.getObjectMetadata();;
        switch (getDataEncryption()) {
            case SSE_S3:
                metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                break;
            case SSE_KMS:
                metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm());
                request.withSSEAwsKeyManagementParams(sseParams);
                break;
            case SSE_C:
                request.withSSECustomerKey(sseCustomerKey);
                break;
            case NONE:
                break;
        }
        request.setObjectMetadata(metadata);
        return request;
    }

    public GeneratePresignedUrlRequest decorate(GeneratePresignedUrlRequest request) {
        switch (getDataEncryption()) {
          case SSE_KMS:
              if (request.getMethod() == GET) break; // KMS is not valid for GET Requests
              String keyId = getSSEParams().getAwsKmsKeyId();
              request = request.withSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm());
              if (keyId != null) {
                  request = request.withKmsCmkId(keyId);
              }
              break;
          case SSE_C:
              request = request.withSSECustomerKey(sseCustomerKey);
              break;
        }
        return request;
    }

    private SSEAwsKeyManagementParams getSSEParams() {
        return this.sseParams;
    }

    private DataEncryption getDataEncryption() {
        return this.dataEncryption;
    }

    /**
     * Enum to indicate S3 encryption mode
     *
     */
    private enum DataEncryption {
        SSE_S3, SSE_KMS, SSE_C, NONE;
    }

}
