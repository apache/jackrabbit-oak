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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.apache.commons.lang3.StringUtils;

public final class AwsContext {

    public final S3Directory directory;
    public final DynamoDBClient dynamoDBClient;
    private final String path;

    private AwsContext(AmazonS3 s3, String bucketName, String rootDirectory, AmazonDynamoDB ddb,
            String journalTableName, String lockTableName) {
        this.directory = new S3Directory(s3, bucketName, rootDirectory);
        this.dynamoDBClient = new DynamoDBClient(ddb, journalTableName, lockTableName);
        this.path = bucketName + "/" + rootDirectory + "/";
    }

    private AwsContext(Configuration configuration) {
        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        AmazonDynamoDBClientBuilder dynamoDBClientBuilder = AmazonDynamoDBClientBuilder.standard();

        if (StringUtils.isNotBlank(configuration.accessKey())) {
            AWSCredentials credentials = StringUtils.isBlank(configuration.sessionToken())
                    ? new BasicAWSCredentials(configuration.accessKey(), configuration.secretKey())
                    : new BasicSessionCredentials(configuration.accessKey(), configuration.secretKey(),
                            configuration.sessionToken());
            AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

            s3ClientBuilder = s3ClientBuilder.withCredentials(credentialsProvider);
            dynamoDBClientBuilder = dynamoDBClientBuilder.withCredentials(credentialsProvider);
        }

        String region = configuration.region();
        if (StringUtils.isNotBlank(region)) {
            s3ClientBuilder = s3ClientBuilder.withRegion(region);
            dynamoDBClientBuilder = dynamoDBClientBuilder.withRegion(region);
        }

        this.directory = new S3Directory(s3ClientBuilder.build(), configuration.bucketName(),
                configuration.rootDirectory());
        this.dynamoDBClient = new DynamoDBClient(dynamoDBClientBuilder.build(), configuration.journalTableName(),
                configuration.lockTableName());
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
        awsContext.directory.ensureBucket();
        awsContext.dynamoDBClient.ensureTables();
        return awsContext;
    }

    public String getPath(String fileName) {
        return path + fileName;
    }

    public String getConfig() {
        StringBuilder uri = new StringBuilder("aws:");
        uri.append(directory.getConfig()).append(';');
        uri.append(dynamoDBClient.getConfig());
        return uri.toString();
    }
}
