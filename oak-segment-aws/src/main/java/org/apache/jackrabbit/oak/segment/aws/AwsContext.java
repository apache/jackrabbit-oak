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
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwsContext {

    private static final Logger log = LoggerFactory.getLogger(AwsContext.class);

    private static final String TABLE_ATTR_TIMESTAMP = "timestamp";

    private static final String TABLE_ATTR_FILENAME = "filename";

    public static final String TABLE_ATTR_CONTENT = "content";

    private static final int TABLE_MAX_BATCH_WRITE_SIZE = 25;

    private final AmazonS3 s3;
    private final String bucketName;
    private final String rootDirectory;

    private final AmazonDynamoDB ddb;
    private final Table journalTable;

    private AwsContext(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
            String journalTableName) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.rootDirectory = rootDirectory.endsWith("/") ? rootDirectory : rootDirectory + "/";
        this.ddb = ddb;
        this.journalTable = new DynamoDB(ddb).getTable(journalTableName);
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
     *                         on "filename" and sort key on "timestamp".
     * @return The context.
     * @throws IOException
     */
    public static AwsContext create(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
            String journalTableName) throws IOException {
        AwsContext awsContext = new AwsContext(s3, bucketName, rootDirectory, ddb, journalTableName);
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
        } catch (AmazonServiceException e) {
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
