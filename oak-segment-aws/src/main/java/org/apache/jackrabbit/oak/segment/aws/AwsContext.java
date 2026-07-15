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
import java.util.concurrent.TimeUnit;

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.util.TimingInfo;

import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;

public final class AwsContext {

    public final S3Directory directory;
    public final DynamoDBClient dynamoDBClient;
    private final String path;
    private RemoteStoreMonitor monitor;

    private AwsContext(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
            String journalTableName, String lockTableName) {
        this(s3, bucketName, rootDirectory, ddb, journalTableName, lockTableName, DynamoDBProvisioningData.DEFAULT);
    }

    private AwsContext(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
                       String journalTableName, String lockTableName, DynamoDBProvisioningData provisioningData) {
        this.directory = new S3Directory(s3, bucketName, rootDirectory);
        this.dynamoDBClient = new DynamoDBClient(ddb, journalTableName, lockTableName, provisioningData);
        this.path = bucketName + "/" + rootDirectory + "/";
    }

    private AwsContext(Configuration configuration) {
        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        AmazonDynamoDBClientBuilder dynamoDBClientBuilder = AmazonDynamoDBClientBuilder.standard();

        if (!isEmpty(configuration.accessKey())) {
            AWSCredentials credentials = isEmpty(configuration.sessionToken())
                    ? new BasicAWSCredentials(configuration.accessKey(), configuration.secretKey())
                    : new BasicSessionCredentials(configuration.accessKey(), configuration.secretKey(),
                            configuration.sessionToken());
            AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

            s3ClientBuilder = s3ClientBuilder.withCredentials(credentialsProvider);
            dynamoDBClientBuilder = dynamoDBClientBuilder.withCredentials(credentialsProvider);
        }

        String region = configuration.region();
        if (!isEmpty(region)) {
            s3ClientBuilder = s3ClientBuilder.withRegion(region);
            dynamoDBClientBuilder = dynamoDBClientBuilder.withRegion(region);
        }

        RequestHandler2 handler = new RequestHandler2() {
            @Override
            public void afterError(Request<?> request, Response<?> response, Exception e) {
                process(request, response, e);
            }

            @Override
            public void afterResponse(Request<?> request, Response<?> response) {
                process(request, response, null);
            }

            private void process(Request<?> request, Response<?> response, Exception e) {
                if (monitor != null) {
                    TimingInfo timing = request.getAWSRequestMetrics().getTimingInfo();
                    if (timing.isEndTimeKnown()) {
                        long requestDuration = timing.getEndTimeNano() - timing.getStartTimeNano();
                        monitor.requestDuration(requestDuration, TimeUnit.NANOSECONDS);
                    }

                    if (e == null) {
                        monitor.requestCount();
                    } else {
                        monitor.requestError();
                    }
                }
            }
        };

        s3ClientBuilder = s3ClientBuilder.withRequestHandlers(handler);
        dynamoDBClientBuilder = dynamoDBClientBuilder.withRequestHandlers(handler);

        this.directory = new S3Directory(s3ClientBuilder.build(), configuration.bucketName(),
                configuration.rootDirectory());
        this.dynamoDBClient = new DynamoDBClient(dynamoDBClientBuilder.build(), configuration.journalTableName(),
                configuration.lockTableName(), DynamoDBProvisioningData.DEFAULT);
        this.path = configuration.bucketName() + "/" + configuration.rootDirectory() + "/";
    }

    /**
     * Creates the context used to interact with AWS services.
     *
     * @param configuration The configuration used to initialize the context.
     * @return The context.
     * @throws IOException
     */
    public static AwsContext create(Configuration configuration) throws IOException {
        AwsContext awsContext = new AwsContext(configuration);
        awsContext.directory.ensureBucket();
        awsContext.dynamoDBClient.ensureTables();
        return awsContext;
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
     *                         on "{@link DynamoDBClient#TABLE_ATTR_FILENAME}" and sort key on
     *                         "{@link DynamoDBClient#TABLE_ATTR_TIMESTAMP}".
     * @param lockTableName    Name of table used for managing the distributed lock.
     *                         The table will be created if it doesn't already
     *                         exist. It should have a partition key on
     *                         "{@link DynamoDBClient#LOCKTABLE_KEY}".
     * @return The context.
     * @throws IOException
     */
    public static AwsContext create(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
            String journalTableName, String lockTableName) throws IOException {
        AwsContext awsContext = new AwsContext(s3, bucketName, rootDirectory, ddb, journalTableName, lockTableName);
        awsContext.directory.ensureBucket();
        awsContext.dynamoDBClient.ensureTables();
        return awsContext;
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
     *                         on "{@link DynamoDBClient#TABLE_ATTR_FILENAME}" and sort key on
     *                         "{@link DynamoDBClient#TABLE_ATTR_TIMESTAMP}".
     * @param lockTableName    Name of table used for managing the distributed lock.
     *                         The table will be created if it doesn't already
     *                         exist. It should have a partition key on
     *                         "{@link DynamoDBClient#LOCKTABLE_KEY}".
     * @param provisioningData DynamoDB provisioning data
     * @return The context.
     * @throws IOException
     */
    public static AwsContext create(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
                                    String journalTableName, String lockTableName, DynamoDBProvisioningData provisioningData) throws IOException {
        AwsContext awsContext = new AwsContext(s3, bucketName, rootDirectory, ddb, journalTableName, lockTableName, provisioningData);
        awsContext.directory.ensureBucket();
        awsContext.dynamoDBClient.ensureTables();
        return awsContext;
    }

    public void setRemoteStoreMonitor(RemoteStoreMonitor monitor) {
        this.monitor = monitor;
    }

    public String getPath(String fileName) {
        return path + fileName;
    }

    public String getConfig() {
        return "aws:" + directory.getConfig() + ';' + dynamoDBClient.getConfig();
    }

    private static boolean isEmpty(String input) {
        return input == null || input.isEmpty();
    }
}
