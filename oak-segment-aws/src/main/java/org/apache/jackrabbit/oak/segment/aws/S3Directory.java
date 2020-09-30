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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class S3Directory {

    private static final Logger log = LoggerFactory.getLogger(AwsContext.class);

    private final AmazonS3 s3;
    private final String bucketName;
    private final String rootDirectory;

    public S3Directory(AmazonS3 s3, String bucketName, String rootDirectory) {
        this.s3 = s3;
        this.bucketName = bucketName;
        rootDirectory = rootDirectory.startsWith("/") ? rootDirectory.substring(1) : rootDirectory;
        this.rootDirectory = rootDirectory.endsWith("/") ? rootDirectory : rootDirectory + "/";
    }

    public S3Directory withDirectory(String childDirectory) {
        return new S3Directory(s3, bucketName, rootDirectory + childDirectory);
    }

    public void ensureBucket() throws IOException {
        try {
            if (!s3.doesBucketExistV2(bucketName)) {
                s3.createBucket(bucketName);
            }
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public String getConfig() {
        return bucketName + ";" + rootDirectory;
    }

    public String getPath() {
        return rootDirectory;
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

    public ObjectMetadata getObjectMetadata(String key) {
        return s3.getObjectMetadata(bucketName, key);
    }

    public Buffer readObjectToBuffer(String name, boolean offHeap) throws IOException {
        byte[] data = readObject(rootDirectory + name);
        Buffer buffer = offHeap ? Buffer.allocateDirect(data.length) : Buffer.allocate(data.length);
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    public void readObjectToBuffer(String name, Buffer buffer) throws IOException {
        byte[] data = readObject(rootDirectory + name);
        buffer.put(data);
        buffer.flip();
    }

    public byte[] readObject(String key) throws IOException {
        try (S3Object object = s3.getObject(bucketName, key)) {
            int length = (int) object.getObjectMetadata().getContentLength();
            byte[] data = new byte[length];
            if (length > 0) {
                try (InputStream stream = object.getObjectContent()) {
                    int off = 0;
                    int remaining = length;
                    while (remaining > 0) {
                        int read = stream.read(data, off, remaining);
                        off += read;
                        remaining -= read;
                    }
                }
            }
            return data;
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void writeObject(String name, byte[] data) throws IOException {
        InputStream input = new ByteArrayInputStream(data);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(data.length);
        PutObjectRequest request = new PutObjectRequest(bucketName, rootDirectory + name, input, metadata);
        try {
            s3.putObject(request);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void putObject(String name, InputStream input) throws IOException {
        try {
            byte[] bytes = IOUtils.toByteArray(input);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);
            InputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            PutObjectRequest request = new PutObjectRequest(bucketName, rootDirectory + name, byteArrayInputStream,
                    metadata);
            s3.putObject(request);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void copyObject(S3Directory from, String fromKey) throws IOException {
        String toKey = rootDirectory + fromKey.substring(from.rootDirectory.length());
        try {
            s3.copyObject(new CopyObjectRequest(from.bucketName, fromKey, bucketName, toKey));
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public boolean deleteObjects(List<KeyVersion> keys) {
        try {
            DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName).withKeys(keys);
            s3.deleteObjects(request);
            return true;
        } catch (AmazonServiceException e) {
            log.error("Can't delete objects from {}", rootDirectory, e);
            return false;
        }
    }


    public boolean deleteAllObjects() {
        try {
            List<KeyVersion> keys = listObjects("").stream().map(i -> new KeyVersion(i.getKey()))
                    .collect(Collectors.toList());
            return deleteObjects(keys);
        } catch (IOException e) {
            log.error("Can't delete objects from {}", rootDirectory, e);
            return false;
        }
    }

    public List<String> listPrefixes() throws IOException {
        return listObjectsInternal("", result -> result.getCommonPrefixes());
    }

    public List<S3ObjectSummary> listObjects(String prefix) throws IOException {
        return listObjectsInternal(prefix, result -> result.getObjectSummaries());
    }

    private <T> List<T> listObjectsInternal(String prefix, Function<ListObjectsV2Result, List<T>> callback)
            throws IOException {
        List<T> objects = new ArrayList<>();
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                .withPrefix(rootDirectory + prefix).withDelimiter("/");
        ListObjectsV2Result result;
        do {
            try {
                result = s3.listObjectsV2(request);
            } catch (AmazonServiceException e) {
                throw new IOException(e);
            }
            objects.addAll(callback.apply(result));
            request.setContinuationToken(result.getContinuationToken());
        } while (result.isTruncated());

        return objects;
    }
}
