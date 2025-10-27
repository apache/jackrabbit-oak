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

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListMultipartUploadsIterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * A Utils class to handle S3 related crud operations
 */
public class S3BackendHelper {

    private S3BackendHelper() {
        // no instances for you
    }

    private static final Logger LOG = LoggerFactory.getLogger(S3BackendHelper.class);

    /**
     * Delete S3 bucket. This method first deletes all objects from bucket and
     * then delete empty bucket.
     *
     * @param bucketName the bucket name.
     */
    public static void deleteBucket(final String bucketName) throws IOException {
        Properties prop = Utils.readConfig(Utils.DEFAULT_CONFIG_FILE);
        S3Client s3Client = Utils.openService(prop, false);

        deleteBucketObjects(bucketName, prop, s3Client);

        // Delete the actual bucket
        s3Client.deleteBucket(delReq -> delReq.bucket(bucketName).build());
    }

    public static void deleteBucketAndAbortMultipartUploads(final String bucket, final Date date, final Properties props) {
        try (S3Client s3Client = Utils.openService(props, false)) {
            if (!bucketExists(s3Client, bucket)) {
                LOG.info("bucket [{}] doesn't exists", bucket);
                return;
            }

            for (int i = 0; i < 4; i++) {
                abortMultipartUpload(bucket, date, s3Client);
            }
            // Delete objects in the bucket with pagination
            deleteBucketObjects(bucket, props, s3Client);
            // Delete the bucket
            s3Client.deleteBucket(delBucket -> delBucket.bucket(bucket).build());
            LOG.info("bucket [ {} ] cleaned", bucket);
        }
    }

    public static void deleteBucketObjects(final String bucket, final Properties props, final S3Client s3Client) {
        deleteBucketObjects(bucket, props, s3Client, s3Client.listObjectsV2Paginator(
                ListObjectsV2Request.builder().bucket(bucket).build()));
    }

    static void deleteBucketObjects(final String bucket, final Properties props, final S3Client s3Client,
                                    final ListObjectsV2Iterable listResponses) {
        for (ListObjectsV2Response listRes : listResponses) {
            List<ObjectIdentifier> deleteList = new ArrayList<>();
            for (S3Object s3Obj : listRes.contents()) {
                deleteList.add(ObjectIdentifier.builder().key(s3Obj.key()).build());
            }

            if (!deleteList.isEmpty()) {
                S3Backend.RemoteStorageMode mode = Utils.getMode(props);
                if (mode == S3Backend.RemoteStorageMode.S3) {
                    s3Client.deleteObjects(delReq ->
                            delReq.bucket(bucket)
                                    .delete(delObj ->
                                            delObj.objects(deleteList)
                                                    .build())
                                    .build());
                } else {
                    // Delete objects one by one
                    deleteList.forEach(obj -> s3Client.deleteObject(delObj -> delObj
                            .bucket(bucket)
                            .key(obj.key())
                            .build()));

                }
            }
        }
    }

    static void abortMultipartUpload(String bucket, Date date, S3Client s3Client) {
        ListMultipartUploadsIterable multiUploadsResponse = s3Client.listMultipartUploadsPaginator(listReq ->
                listReq.bucket(bucket).build());
        for (MultipartUpload multipartUpload : multiUploadsResponse.uploads()) {
            Instant initiated = multipartUpload.initiated();
            if (initiated.isBefore(date.toInstant())) {
                s3Client.abortMultipartUpload(abortReq -> abortReq
                        .bucket(bucket)
                        .key(multipartUpload.key())
                        .uploadId(multipartUpload.uploadId())
                        .build());
            }
        }
    }

    /**
     * Checks if the specified Amazon S3 bucket exists and is accessible with the provided S3 client.
     * <p>
     * This method attempts to perform a {@code headBucket} request. If the bucket exists and is accessible,
     * it returns {@code true}. If the bucket does not exist, it returns {@code false}.
     * Other unexpected exceptions (such as permission errors or network issues) will propagate.
     * </p>
     *
     * @param s3Client   the {@link S3Client} to use for the request
     * @param bucketName the name of the S3 bucket to check
     * @return {@code true} if the bucket exists and is accessible; {@code false} if the bucket does not exist
     * @throws NullPointerException if {@code s3Client} or {@code bucketName} is null
     * @throws S3Exception         if an AWS error other than {@link NoSuchBucketException} occurs
     */
    static boolean bucketExists(final S3Client s3Client, final String bucketName) {
        try {
            s3Client.headBucket(request -> request.bucket(bucketName));
            return true;
        }
        catch (NoSuchBucketException exception) {
            return false;
        }
    }

    /**
     * Checks if a specific object exists in the given S3 bucket.
     * <p>
     * Performs a {@code headObject} request using the provided {@link S3RequestDecorator}.
     * Returns {@code true} if the object exists, {@code false} if it does not (404 or 403).
     * Other {@link S3Exception}s are propagated.
     * </p>
     *
     * @param s3Client        the {@link S3Client} to use for the request
     * @param bucket          the S3 bucket name
     * @param key             the object key
     * @param s3ReqDecorator  decorator for the {@link HeadObjectRequest}
     * @return {@code true} if the object exists, {@code false} if not found or forbidden
     * @throws S3Exception for AWS errors other than 404 or 403
     */
    static boolean objectExists(final S3Client s3Client, final String bucket, final String key, S3RequestDecorator s3ReqDecorator) {
        try {
            s3Client.headObject(s3ReqDecorator.decorate(HeadObjectRequest.builder().bucket(bucket).key(key).build()));

            LOG.debug("Object {} exists in bucket {}", key, bucket);
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404 || e.statusCode() == 403) {
                LOG.info("Object {} doesn't exists in bucket {}", key, bucket);
                return false;
            } else {
                throw e;
            }
        }
    }

    /**
     * Waits for an S3 bucket, one we expect to exist, to report that it exists.
     * A check for the bucket is called with a limited number of repeats with
     * an increasing backoff.
     *
     * Usually you would call this after creating a bucket to block until the
     * bucket is actually available before moving forward with other tasks that
     * expect the bucket to be available.
     *
     * @param s3Client The AmazonS3 client connection to the storage service.
     * @param bucketName The name of the bucket to check.
     * @return True if the bucket exists; false otherwise.
     */
    public static boolean waitForBucket(@NotNull final S3Client s3Client, @NotNull final String bucketName,
                                        final int maxAttempts, final long delayMillis) {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                s3Client.headBucket(headReq -> headReq.bucket(bucketName).build());
                return true;
            } catch (NoSuchBucketException e) {
                // Bucket doesn't exist yet; backoff and retry
                try {
                    Thread.sleep(delayMillis * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // Preserve interrupt status!
                    break;
                }
            } catch (Exception e) {
                // Log or handle unexpected errors
                LOG.error("Error during waitForBucket:", e);
                break;
            }
        }
        return false;
    }
}
