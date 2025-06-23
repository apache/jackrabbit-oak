/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticsearchRetryPolicyTest {
    static class PassAfterNAttemptsTask implements ElasticRetryPolicy.IOOperation {
        private final int succeedAfterRetries;

        public PassAfterNAttemptsTask(int succeedAfterRetries) {
            this.succeedAfterRetries = succeedAfterRetries;
        }

        int executionCount = 0;

        public void execute() throws IOException {
            executionCount++;
            if (executionCount <= succeedAfterRetries) {
                throw new IOException("Simulated failure. Execution: " + executionCount + ", will succeed after " + succeedAfterRetries + " failed attempts.");
            }
        }
    }

    static class PassAfterElapsedTime implements ElasticRetryPolicy.IOOperation {
        private final int succeedAfterElapsedTimeMillis;
        private long firstExecutionTime = -1;

        public PassAfterElapsedTime(int succeedAfterElapsedTimeMillis) {
            this.succeedAfterElapsedTimeMillis = succeedAfterElapsedTimeMillis;
        }

        public void execute() throws IOException {
            if (firstExecutionTime == -1) {
                firstExecutionTime = System.nanoTime();
            }
            long elapsedTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - firstExecutionTime);
            if (elapsedTime < succeedAfterElapsedTimeMillis) {
                // Simulate a failure until the elapsed time is reached
                throw new IOException("Simulated failure. Elapsed time: " + elapsedTime + "ms, will succeed after " + succeedAfterElapsedTimeMillis + "ms.");
            }
        }
    }

    @Test
    public void succeedAfterNAttempts() throws IOException {
        testPolicy(ElasticRetryPolicy.NO_RETRY, 0);
        testPolicy(new ElasticRetryPolicy(1, 1000, 1, 1), 0);
        testPolicy(new ElasticRetryPolicy(2, 1000, 1, 1), 1);
        testPolicy(new ElasticRetryPolicy(5, 1000, 1, 1), 4);
    }

    @Test
    public void succeedAfterTime() throws IOException {
        // A policy that will keep trying for 40 ms
        ElasticRetryPolicy retryPolicy = new ElasticRetryPolicy(1000, 40, 1, 1);

        // The task will succeed after 20 ms, so we expect the retry policy to succeed after 20 ms
        long startTime = System.nanoTime();
        retryPolicy.withRetries(new PassAfterElapsedTime(20));
        assertTrue(System.nanoTime() - startTime >= TimeUnit.MILLISECONDS.toNanos(20));

        // This task will fail for 60 ms, so the retry policy will also fail because it is set to wait for 50 ms.
        try {
            retryPolicy.withRetries(new PassAfterElapsedTime(60));
            fail("Expected IOException not thrown");
        } catch (IOException e) {
            //pass
        }
    }

    private void testPolicy(ElasticRetryPolicy retryPolicy, int expectedMaxRetries) throws IOException {
        // These should all succeed because the number of retries is higher than the number of attempts until the task succeeds
        for (int i = 0; i <= expectedMaxRetries; i++) {
            retryPolicy.withRetries(new PassAfterNAttemptsTask(i));
        }

        try {
            // This should fail, we are not trying enough times
            retryPolicy.withRetries(new PassAfterNAttemptsTask(expectedMaxRetries + 1));
            fail("Expected IOException not thrown");
        } catch (IOException e) {
            //pass
        }
    }
}