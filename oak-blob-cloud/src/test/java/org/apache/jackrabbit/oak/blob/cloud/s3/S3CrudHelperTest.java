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

import java.util.function.Consumer;

public class S3CrudHelperTest {

    @Test
    public void testBucketExistsTrue() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        // No exception means bucket exists
        Assert.assertTrue(S3CrudHelper.bucketExists(mockClient, Mockito.anyString()));
        Mockito.verify(mockClient).headBucket(Mockito.any(Consumer.class));
    }

    @Test
    public void testBucketExistsFalse() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        Mockito.doThrow(NoSuchBucketException.builder().build())
                .when(mockClient).headBucket(Mockito.any(Consumer.class));
        Assert.assertFalse(S3CrudHelper.bucketExists(mockClient, Mockito.anyString()));
        Mockito.verify(mockClient).headBucket(Mockito.any(Consumer.class));
    }

    @Test
    public void testObjectExistsTrue() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        S3RequestDecorator decorator = Mockito.mock(S3RequestDecorator.class);

        // identity decorator
        Mockito.when(decorator.decorate(Mockito.any(HeadObjectRequest.class))).thenReturn(Mockito.mock(HeadObjectRequest.class));
        Assert.assertTrue(S3CrudHelper.objectExists(mockClient, "bucket", "key", decorator));
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
        Assert.assertFalse(S3CrudHelper.objectExists(mockClient, "bucket", "key", decorator));
    }

    @Test
    public void testObjectExistsForbidden() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        S3RequestDecorator decorator = Mockito.mock(S3RequestDecorator.class);

        // identity decorator
        Mockito.when(decorator.decorate(Mockito.any(HeadObjectRequest.class))).thenReturn(Mockito.mock(HeadObjectRequest.class));
        AwsServiceException forbidden = S3Exception.builder().statusCode(403).build();
        Mockito.doThrow(forbidden).when(mockClient).headObject(Mockito.any(HeadObjectRequest.class));
        Assert.assertFalse(S3CrudHelper.objectExists(mockClient, "bucket", "key", decorator));
    }

    @Test(expected = S3Exception.class)
    public void testObjectExistsOtherException() {
        S3Client mockClient = Mockito.mock(S3Client.class);
        S3RequestDecorator decorator = Mockito.mock(S3RequestDecorator.class);

        // identity decorator
        Mockito.when(decorator.decorate(Mockito.any(HeadObjectRequest.class))).thenReturn(Mockito.mock(HeadObjectRequest.class));
        AwsServiceException other = S3Exception.builder().statusCode(500).build();
        Mockito.doThrow(other).when(mockClient).headObject(Mockito.any(HeadObjectRequest.class));
        S3CrudHelper.objectExists(mockClient, "bucket", "key", decorator);
    }

}