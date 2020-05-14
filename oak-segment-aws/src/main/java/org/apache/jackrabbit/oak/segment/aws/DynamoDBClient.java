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
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
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
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

public final class DynamoDBClient {

    private static final String TABLE_ATTR_TIMESTAMP = "timestamp";

    private static final String TABLE_ATTR_FILENAME = "filename";

    public static final String TABLE_ATTR_CONTENT = "content";

    private static final int TABLE_MAX_BATCH_WRITE_SIZE = 25;

    private static final String LOCKTABLE_KEY = "key";

    private final AmazonDynamoDB ddb;
    private final String journalTableName;
    private final Table journalTable;
    private final String lockTableName;
    private DynamoDBProvisioningData provisioningData;

    public DynamoDBClient(AmazonDynamoDB ddb, String journalTableName, String lockTableName) {
        this(ddb, journalTableName, lockTableName, DynamoDBProvisioningData.DEFAULT);
    }

    public DynamoDBClient(AmazonDynamoDB ddb, String journalTableName, String lockTableName, DynamoDBProvisioningData provisioningData) {
        this.ddb = ddb;
        this.journalTableName = journalTableName;
        this.journalTable = new DynamoDB(ddb).getTable(journalTableName);
        this.lockTableName = lockTableName;
        if (provisioningData == null) {
            this.provisioningData = DynamoDBProvisioningData.DEFAULT;
        } else {
            this.provisioningData = provisioningData;
        }
    }

    public void ensureTables() throws IOException {
        try {
            CreateTableRequest createJournalTableRequest = new CreateTableRequest().withTableName(journalTableName)
                    .withKeySchema(new KeySchemaElement(TABLE_ATTR_FILENAME, KeyType.HASH),
                            new KeySchemaElement(TABLE_ATTR_TIMESTAMP, KeyType.RANGE))
                    .withAttributeDefinitions(new AttributeDefinition(TABLE_ATTR_FILENAME, ScalarAttributeType.S),
                            new AttributeDefinition(TABLE_ATTR_TIMESTAMP, ScalarAttributeType.N))
                    .withBillingMode(provisioningData.getBillingMode());

            ensureTable(createJournalTableRequest, provisioningData.getJournalTableProvisionedRcu(), provisioningData.getJournalTableProvisionedWcu());

            CreateTableRequest createLockTableRequest = new CreateTableRequest().withTableName(lockTableName)
                    .withKeySchema(new KeySchemaElement(LOCKTABLE_KEY, KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition(LOCKTABLE_KEY, ScalarAttributeType.S))
                    .withBillingMode(provisioningData.getBillingMode());

            ensureTable(createLockTableRequest, provisioningData.getLockTableProvisionedRcu(), provisioningData.getLockTableProvisionedWcu());
        } catch (SdkClientException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    private void ensureTable(CreateTableRequest createTableRequest, Long tableRcu, Long tableWcu) throws InterruptedException {
        if (provisioningData.getBillingMode().equals(BillingMode.PROVISIONED)) {
            createTableRequest.withProvisionedThroughput(new ProvisionedThroughput(tableRcu, tableWcu));
        }

        TableUtils.createTableIfNotExists(ddb, createTableRequest);

        TableUtils.waitUntilActive(ddb, createTableRequest.getTableName());
    }

    /**
     * Updates table billing mode.
     *
     * @param table
     * @return true if billing mode has changed
     */
    private boolean updateBillingMode(Table table, Long tableRcu, Long tableWcu) {
        final BillingMode currentBillingMode = BillingMode.valueOf(table.describe().getBillingModeSummary().getBillingMode());

        ProvisionedThroughputDescription throughputDescription = table.getDescription().getProvisionedThroughput();

        //update the table if billing mode is different or provisioned capacity has changed
        if (currentBillingMode != provisioningData.getBillingMode() ||
                (provisioningData.getBillingMode() == BillingMode.PROVISIONED &&
                        (!throughputDescription.getReadCapacityUnits().equals(tableRcu) || !throughputDescription.getReadCapacityUnits().equals(tableWcu)))) {

            UpdateTableRequest tableUpdateRequest = new UpdateTableRequest()
                    .withTableName(table.getTableName())
                    .withBillingMode(provisioningData.getBillingMode());

            if (provisioningData.getBillingMode().equals(BillingMode.PROVISIONED)) {
                tableUpdateRequest.withProvisionedThroughput(new ProvisionedThroughput(tableRcu, tableWcu));
            }

            ddb.updateTable(tableUpdateRequest);

            return true;
        }

        return false;
    }

    public String getConfig() {
        return journalTableName + ";" + lockTableName;
    }

    public AmazonDynamoDBLockClientOptionsBuilder getLockClientOptionsBuilder() {
        return AmazonDynamoDBLockClientOptions.builder(ddb, lockTableName).withPartitionKeyName(LOCKTABLE_KEY);
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
                    new TableWriteItems(journalTableName).withPrimaryKeysToDelete(currentKeys));
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
                // TO DO: why is this needed here
                Thread.sleep(1L);
            } catch (InterruptedException e) {
            }
            journalTable.putItem(item);
        } catch (AmazonServiceException e) {
            throw new IOException(e);
        }
    }

}
