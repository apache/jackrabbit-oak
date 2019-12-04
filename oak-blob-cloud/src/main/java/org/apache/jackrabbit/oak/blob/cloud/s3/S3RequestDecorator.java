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

import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.util.StringUtils;

/**
 * This class to sets encrption mode in S3 request.
 *
 */
public class S3RequestDecorator {
    DataEncryption dataEncryption = DataEncryption.NONE;
    Properties props;
    SSEAwsKeyManagementParams sseParams;

    public S3RequestDecorator(Properties props) {
        String encryptionType = props.getProperty(S3Constants.S3_ENCRYPTION);
        if (encryptionType != null) {
            this.dataEncryption = dataEncryption.valueOf(encryptionType);

            if (encryptionType.equals(S3Constants.S3_ENCRYPTION_SSE_KMS)) {
                String keyId = props.getProperty(S3Constants.S3_SSE_KMS_KEYID);
                sseParams = new SSEAwsKeyManagementParams();
                if (!StringUtils.isNullOrEmpty(keyId)) {
                    sseParams.withAwsKmsKeyId(keyId);
                }
            }
        }
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
            case NONE:
                break;
        }
        request.setObjectMetadata(metadata);
        return request;
    }

    public GeneratePresignedUrlRequest decorate(GeneratePresignedUrlRequest request) {
        switch (getDataEncryption()) {
          case SSE_KMS:
              String keyId = getSSEParams().getAwsKmsKeyId();
              request = request.withSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm());
              if (keyId != null) {
                  request = request.withKmsCmkId(keyId);
              }
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
        SSE_S3, SSE_KMS, NONE;
    }

}
