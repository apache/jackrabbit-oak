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

package org.apache.jackrabbit.oak.segment.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class TriggeredOperationTest {

    @Test
    public void trigger() {
        final CountDownLatch triggered = new CountDownLatch(1);

        // Define a triggered operation.

        final TriggeredOperation operation = new TriggeredOperation("test", new Runnable() {

            @Override
            public void run() {
                triggered.countDown();
            }

        });

        // Trigger an execution of the operation.

        operation.trigger();

        // Wait until the operation is actually executed.

        try {
            triggered.await();
        } catch (InterruptedException e) {
            fail("operation not triggered");
        }

        // Stop the background operation. This shouldn't time out.

        try {
            assertTrue(operation.stop(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail("unable to stop the operation");
        }
    }

    @Test
    public void stopTimeOut() {
        final CountDownLatch triggered = new CountDownLatch(1);
        final CountDownLatch terminate = new CountDownLatch(1);

        // Define a triggered operation.

        TriggeredOperation operation = new TriggeredOperation("test", new Runnable() {

            @Override
            public void run() {
                triggered.countDown();

                try {
                    terminate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        });

        // Trigger an execution of the operation.

        operation.trigger();

        // Wait for the execution of the operation.

        try {
            triggered.await();
        } catch (InterruptedException e) {
            fail("operation not triggered");
        }

        // At this point, the operation started its execution and is waiting for
        // this thread to send a signal. If we try to stop the operation, we
        // will receive a timeout.

        try {
            assertFalse(operation.stop(500, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            fail("unable to stop the operation");
        }

        // Signal the operation to terminate so the background thread can be
        // killed.

        terminate.countDown();
    }

    @Test
    public void overlappingTrigger() {
        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch triggered = new CountDownLatch(1);
        final CountDownLatch terminate = new CountDownLatch(1);

        // Define a triggered operation.

        TriggeredOperation operation = new TriggeredOperation("test", new Runnable() {

            @Override
            public void run() {
                triggered.countDown();

                counter.incrementAndGet();

                try {
                    terminate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        });

        // Trigger the first execution of the operation.

        operation.trigger();

        // Wait for the operation to start.

        try {
            triggered.await();
        } catch (InterruptedException e) {
            fail("operation not triggered");
        }

        // At this point the operation is running, waiting for this thread to
        // send a termination signal. Triggering the operation will not execute
        // a new instance of the operation.

        operation.trigger();

        // Send a signal to the operation to terminate its execution.

        terminate.countDown();

        // Stop the operation. This shouldn't time out.

        try {
            assertTrue(operation.stop(500, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            fail("unable to stop the operation");
        }

        // Check that the operation was only executed once.

        assertEquals(1, counter.get());
    }

}
