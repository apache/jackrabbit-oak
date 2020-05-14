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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;

import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsRepositoryLock implements RepositoryLock {

    private static final Logger log = LoggerFactory.getLogger(AwsRepositoryLock.class);

    private static final int TIMEOUT_SEC = Integer.getInteger("oak.segment.aws.lock.timeout", 0);

    private static final long INTERVAL = 60;

    private final AmazonDynamoDBLockClient lockClient;
    private final String lockName;
    private final long timeoutSec;

    private LockItem lockItem;

    public AwsRepositoryLock(DynamoDBClient dynamoDBClient, String lockName) {
        this(dynamoDBClient, lockName, TIMEOUT_SEC);
    }

    public AwsRepositoryLock(DynamoDBClient dynamoDBClient, String lockName, int timeoutSec) {
        this.lockClient = new AmazonDynamoDBLockClient(
                dynamoDBClient.getLockClientOptionsBuilder().withTimeUnit(TimeUnit.SECONDS).withLeaseDuration(INTERVAL)
                        .withHeartbeatPeriod(INTERVAL / 3).withCreateHeartbeatBackgroundThread(true).build());
        this.lockName = lockName;
        this.timeoutSec = timeoutSec;
    }

    public AwsRepositoryLock lock() throws IOException {
        try {
            Optional<LockItem> lockItemOptional = lockClient.tryAcquireLock(AcquireLockOptions.builder(lockName)
                    .withTimeUnit(TimeUnit.SECONDS).withAdditionalTimeToWaitForLock(timeoutSec)
                    .build());
            if (lockItemOptional.isPresent()) {
                lockItem = lockItemOptional.get();
                return this;
            } else {
                log.error("Can't acquire the lease in {}s.", timeoutSec);
                throw new IOException("Can't acquire the lease in " + timeoutSec + "s.");
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void unlock() {
        lockClient.releaseLock(lockItem);
    }
}
