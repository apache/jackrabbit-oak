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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwsContext {

    private static final Logger log = LoggerFactory.getLogger(AwsContext.class);

    private static final String TABLE_ATTR_TIMESTAMP = "timestamp";

    private static final String TABLE_ATTR_FILENAME = "filename";

    public static final String TABLE_ATTR_CONTENT = "content";

    private static final int TABLE_MAX_BATCH_WRITE_SIZE = 25;

    private static final String LOCKTABLE_KEY = "key";

    private final AmazonS3 s3;
    private final String bucketName;
    private final String rootDirectory;

    private final AmazonDynamoDB ddb;
    private final Table journalTable;
    private final String lockTableName;

    private RemoteStoreMonitor remoteStoreMonitor;

    private AwsContext(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
            String journalTableName, String lockTableName) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.rootDirectory = rootDirectory.endsWith("/") ? rootDirectory : rootDirectory + "/";
        this.ddb = ddb;
        this.journalTable = new DynamoDB(ddb).getTable(journalTableName);
        this.lockTableName = lockTableName;
    }

    /**
     * Creates the context used to interact with AWS services.
     * 
     * @param bucketName       Name for the bucket that will store segments.
     * @param rootDirectory    The root directory under which the segment store is
     *                         setup.
     * @param journalTableName Name of table used for storing log entries for
     *                         journal and gc. The table will be created if it
     *                         doesn't already exist. It should have a partition key
     *                         on "{@link #TABLE_ATTR_FILENAME}" and sort key on
     *                         "{@link #TABLE_ATTR_TIMESTAMP}".
     * @param lockTableName    Name of table used for managing the distributed lock.
     *                         The table will be created if it doesn't already
     *                         exist. It should have a partition key on
     *                         "{@link #LOCKTABLE_KEY}".
     * @return The context.
     * @throws IOException
     */
    public static AwsContext create(String bucketName, String rootDirectory, String journalTableName,
            String lockTableName) throws IOException {
        AmazonS3 s3 = AmazonS3Client.builder().build();
        AmazonDynamoDB ddb = AmazonDynamoDBClient.builder().build();
        return create(s3, bucketName, rootDirectory, ddb, journalTableName, lockTableName);
    }

    /**
     * Creates the context used to interact with AWS services.
     * 
     * @param configuration The configuration used to initialize the context.
     * @return The context.
     * @throws IOException
     */
    public static AwsContext create(Configuration configuration) throws IOException {
        String region = configuration.region();
        String rootDirectory = configuration.rootDirectory();
        if (rootDirectory != null && rootDirectory.length() > 0 && rootDirectory.charAt(0) == '/') {
            rootDirectory = rootDirectory.substring(1);
        }

        AWSCredentials credentials = configuration.sessionToken() == null || configuration.sessionToken().isEmpty()
                ? new BasicAWSCredentials(configuration.accessKey(), configuration.secretKey())
                : new BasicSessionCredentials(configuration.accessKey(), configuration.secretKey(),
                        configuration.sessionToken());
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(region).build();
        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard().withCredentials(credentialsProvider)
                .withRegion(region).build();
        return create(s3, configuration.bucketName(), rootDirectory, ddb, configuration.journalTableName(),
                configuration.lockTableName());
    }

    /**
     * Creates the context used to interact with AWS services.
     * 
     * @param s3               Client for accessing Amazon S3.
     * @param bucketName       Name for the bucket that will store segments.
     * @param rootDirectory    The root directory under which the segment store is
     *                         setup.
     * @param ddb              Client for accessing Amazon DynamoDB.
     * @param journalTableName Name of table used for storing log entries for
     *                         journal and gc. The table will be created if it
     *                         doesn't already exist. It should have a partition key
     *                         on "{@link #TABLE_ATTR_FILENAME}" and sort key on
     *                         "{@link #TABLE_ATTR_TIMESTAMP}".
     * @param lockTableName    Name of table used for managing the distributed lock.
     *                         The table will be created if it doesn't already
     *                         exist. It should have a partition key on
     *                         "{@link #LOCKTABLE_KEY}".
     * @return The context.
     * @throws IOException
     */
    public static AwsContext create(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
            String journalTableName, String lockTableName) throws IOException {
        AwsContext awsContext = new AwsContext(s3, bucketName, rootDirectory, ddb, journalTableName, lockTableName);
        try {
            if (!s3.doesBucketExistV2(bucketName)) {
                s3.createBucket(bucketName);
            }

            CreateTableRequest createJournalTableRequest = new CreateTableRequest().withTableName(journalTableName)
                    .withKeySchema(new KeySchemaElement(TABLE_ATTR_FILENAME, KeyType.HASH),
                            new KeySchemaElement(TABLE_ATTR_TIMESTAMP, KeyType.RANGE))
                    .withAttributeDefinitions(new AttributeDefinition(TABLE_ATTR_FILENAME, ScalarAttributeType.S),
                            new AttributeDefinition(TABLE_ATTR_TIMESTAMP, ScalarAttributeType.N))
                    .withProvisionedThroughput(new ProvisionedThroughput(1000L, 1500L));
            TableUtils.createTableIfNotExists(ddb, createJournalTableRequest);

            CreateTableRequest createLockTableRequest = new CreateTableRequest().withTableName(lockTableName)
                    .withKeySchema(new KeySchemaElement(LOCKTABLE_KEY, KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition(LOCKTABLE_KEY, ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput(1000L, 1500L));
            TableUtils.createTableIfNotExists(ddb, createLockTableRequest);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }

        return awsContext;
    }

    public AmazonDynamoDBLockClientOptionsBuilder getLockClientOptionsBuilder() {
        return AmazonDynamoDBLockClientOptions.builder(ddb, lockTableName).withPartitionKeyName(LOCKTABLE_KEY);
    }

    public AwsContext withDirectory(String childDirectory) {
        return new AwsContext(s3, bucketName, rootDirectory + childDirectory, ddb, journalTable.getTableName(),
                lockTableName);
    }

    public String getConfig() {
        StringBuilder uri = new StringBuilder("aws:");
        uri.append(bucketName).append(';');
        uri.append(rootDirectory).append(';');
        uri.append(journalTable.getTableName()).append(';');
        uri.append(lockTableName);
        return uri.toString();
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
        writeObject(name, data, new HashMap<>());
    }

    public void writeObject(String name, byte[] data, Map<String, String> userMetadata) throws IOException {
        InputStream input = new ByteArrayInputStream(data);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setUserMetadata(userMetadata);
        PutObjectRequest request = new PutObjectRequest(bucketName, rootDirectory + name, input, metadata);
        try {
            s3.putObject(request);
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

    public void copyObject(AwsContext fromContext, String fromKey) throws IOException {
        String toKey = rootDirectory + fromKey.substring(fromContext.rootDirectory.length());
        try {
            s3.copyObject(new CopyObjectRequest(bucketName, fromKey, bucketName, toKey));
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public boolean deleteAllObjects() {
        try {
            List<KeyVersion> keys = listObjects("").stream().map(i -> new KeyVersion(i.getKey()))
                    .collect(Collectors.toList());
            DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName).withKeys(keys);
            s3.deleteObjects(request);
            return true;
        } catch (AmazonServiceException | IOException e) {
            log.error("Can't delete objects from {}", rootDirectory, e);
            return false;
        }
    }

    public List<String> listPrefixes() throws IOException {
        return listObjectsInternal("").getCommonPrefixes();
    }

    public List<S3ObjectSummary> listObjects(String prefix) throws IOException {
        return listObjectsInternal(prefix).getObjectSummaries();
    }

    private ListObjectsV2Result listObjectsInternal(String prefix) throws IOException {
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                .withPrefix(rootDirectory + prefix).withDelimiter("/");
        try {
            return s3.listObjectsV2(request);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void deleteAllDocuments(String fileName) throws IOException {
        List<PrimaryKey> primaryKeys = getDocumentsStream(fileName).map(item -> {
            return new PrimaryKey(TABLE_ATTR_FILENAME, item.getString(TABLE_ATTR_FILENAME), TABLE_ATTR_TIMESTAMP,
                    item.getNumber(TABLE_ATTR_TIMESTAMP));
        }).collect(Collectors.toList());

        for (int i = 0; i < primaryKeys.size(); i += TABLE_MAX_BATCH_WRITE_SIZE) {
            PrimaryKey[] currentKeys = new PrimaryKey[Math.min(TABLE_MAX_BATCH_WRITE_SIZE, primaryKeys.size() - i)];
            for (int j = 0; j < currentKeys.length; j++) {
                currentKeys[j] = primaryKeys.get(i + j);
            }

            new DynamoDB(ddb).batchWriteItem(
                    new TableWriteItems(journalTable.getTableName()).withPrimaryKeysToDelete(currentKeys));
        }
    }

    public List<String> getDocumentContents(String fileName) throws IOException {
        return getDocumentsStream(fileName).map(item -> item.getString(TABLE_ATTR_CONTENT))
                .collect(Collectors.toList());
    }

    public Stream<Item> getDocumentsStream(String fileName) throws IOException {
        String FILENAME_KEY = ":v_filename";
        QuerySpec spec = new QuerySpec().withScanIndexForward(false)
                .withKeyConditionExpression(TABLE_ATTR_FILENAME + " = " + FILENAME_KEY)
                .withValueMap(new ValueMap().withString(FILENAME_KEY, fileName));
        try {
            ItemCollection<QueryOutcome> outcome = journalTable.query(spec);
            return StreamSupport.stream(outcome.spliterator(), false);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

    public void putDocument(String fileName, String line) throws IOException {
        Item item = new Item().with(TABLE_ATTR_TIMESTAMP, new Date().getTime()).with(TABLE_ATTR_FILENAME, fileName)
                .with(TABLE_ATTR_CONTENT, line);
        try {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
            }
            journalTable.putItem(item);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }
}
