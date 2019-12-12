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
package org.apache.jackrabbit.oak.segment.aws;

import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwsContext {

    private static final Logger log = LoggerFactory.getLogger(AwsContext.class);

    private final AmazonS3 s3;
    private final String bucketName;
    private final String rootDirectory;

    private AwsContext(AmazonS3 s3, String bucketName, String rootDirectory) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.rootDirectory = rootDirectory.endsWith("/") ? rootDirectory : rootDirectory + "/";
    }

    /**
     * Creates the context used to interact with AWS services.
     * 
     * @param s3               Client for accessing Amazon S3.
     * @param bucketName       Name for the bucket that will store segments.
     * @param rootDirectory    The root directory under which the segment store is
     *                         setup.
     * @return The context.
     * @throws IOException
     */
    public static AwsContext create(AmazonS3 s3, String bucketName, String rootDirectory) throws IOException {
        AwsContext awsContext = new AwsContext(s3, bucketName, rootDirectory);
        try {
            if (!s3.doesBucketExistV2(bucketName)) {
                s3.createBucket(bucketName);
            }
        } catch(AmazonServiceException e) {
            throw new IOException(e);
        }

        return awsContext;
    }

    public boolean doesObjectExist(String name) {
        try {
            return s3.doesObjectExist(bucketName, rootDirectory + name);
        } catch (AmazonServiceException e) {
            log.error("Can't check if the manifest exists", e);
            return false;
        }
    }

    public S3Object getObject(String name) throws IOException {
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, rootDirectory + name);
            return s3.getObject(request);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void putObject(String name, InputStream input) throws IOException {
        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, rootDirectory + name, input,
                    new ObjectMetadata());
            s3.putObject(request);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }
}
