/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus;
import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus.ACQUIRED;
import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus.RELEASED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class AwsRepositoryLockTest {

    private static final Logger log = LoggerFactory.getLogger(AwsRepositoryLockTest.class);

    private static final String lockName = "repo.lock";

    private DynamoDBClient dynamoDBClient;

    @Before
    public void setup() throws IOException {
        AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        long time = new Date().getTime();
        dynamoDBClient = new DynamoDBClient(ddb, "journaltable-" + time, "locktable-" + time);
        dynamoDBClient.ensureTables();
    }

    @Test
    public void testFailingLock() throws IOException {
        AtomicReference<LockStatus> status = new AtomicReference<>();
        new AwsRepositoryLock(dynamoDBClient, lockName, status::set, 0, 5).lock();
        assertEquals(LockStatus.ACQUIRED, status.get());

        assertThrows("The second lock should fail", IOException.class, () -> 
            new AwsRepositoryLock(dynamoDBClient, lockName, status::set, 0, 5).lock()
        );
        assertEquals(LockStatus.ACQUIRE_FAILED, status.get());
    }

    @Test
    public void testWaitingLock() throws InterruptedException, IOException {
        AtomicReference<LockStatus> status1 = new AtomicReference<>();
        AtomicReference<LockStatus> status2 = new AtomicReference<>();
        Semaphore s = new Semaphore(0);
        new Thread(() -> {
            try {
                RepositoryLock lock = new AwsRepositoryLock(dynamoDBClient, lockName, status1::set, 0, 5).lock();
                s.release();
                Thread.sleep(1000);
                lock.unlock();
            } catch (Exception e) {
                log.error("Can't lock or unlock the repo", e);
            }
        }).start();

        s.acquire();
        assertEquals(ACQUIRED, status1.get());
        new AwsRepositoryLock(dynamoDBClient, lockName, status2::set, 10, 5).lock();
        assertEquals(RELEASED, status1.get());
        assertEquals(ACQUIRED, status2.get());
    }
}
