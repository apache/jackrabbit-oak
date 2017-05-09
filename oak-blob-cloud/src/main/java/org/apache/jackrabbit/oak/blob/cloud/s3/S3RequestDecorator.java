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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * This class to sets encrption mode in S3 request.
 *
 */
public class S3RequestDecorator {
    DataEncryption dataEncryption = DataEncryption.NONE;

    public S3RequestDecorator(Properties props) {
        if (props.getProperty(S3Constants.S3_ENCRYPTION) != null) {
            this.dataEncryption = dataEncryption.valueOf(props.getProperty(S3Constants.S3_ENCRYPTION));
        }
    }

    /**
     * Set encryption in {@link PutObjectRequest}
     */
    public PutObjectRequest decorate(PutObjectRequest request) {
        switch (getDataEncryption()) {
            case SSE_S3:
                ObjectMetadata metadata = request.getMetadata() == null
                                ? new ObjectMetadata()
                                : request.getMetadata();
                metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                request.setMetadata(metadata);
                break;
            case NONE:
                break;
        }
        return request;
    }

    /**
     * Set encryption in {@link CopyObjectRequest}
     */
    public CopyObjectRequest decorate(CopyObjectRequest request) {
        switch (getDataEncryption()) {
            case SSE_S3:
                ObjectMetadata metadata = request.getNewObjectMetadata() == null
                                ? new ObjectMetadata()
                                : request.getNewObjectMetadata();
                metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                request.setNewObjectMetadata(metadata);
                break;
            case NONE:
                break;
        }
        return request;
    }

    private DataEncryption getDataEncryption() {
        return this.dataEncryption;
    }

    /**
     * Enum to indicate S3 encryption mode
     *
     */
    private enum DataEncryption {
        SSE_S3, NONE;
    }

}
