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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class PeriodicOperationTest {

    @Test
    public void periodicTrigger() {
        final CountDownLatch executions = new CountDownLatch(5);

        // Define an operation to be run ever 100ms.

        PeriodicOperation operation = new PeriodicOperation("test", 100, TimeUnit.MILLISECONDS, new Runnable() {

            @Override
            public void run() {
                executions.countDown();
            }

        });

        // Start running the operation.

        operation.start();

        // Wait for the first 5 executions of the operation. The amount of
        // iterations to wait for is defined by the CountDownLatch above.

        try {
            executions.await();
        } catch (InterruptedException e) {
            fail("operation not triggered");
        }

        // Stop the operation. This shouldn't time out.

        try {
            assertTrue(operation.stop(500, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            fail("unable to stop the operation");
        }
    }

    @Test
    public void stopTimeOut() {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch terminate = new CountDownLatch(1);

        // Define an operation to be run every 100ms.

        PeriodicOperation operation = new PeriodicOperation("test", 100, TimeUnit.MILLISECONDS, new Runnable() {

            @Override
            public void run() {
                started.countDown();

                try {
                    terminate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        });

        // Start running the operation periodically.

        operation.start();

        // Wait until the first execution of the operation begins. At this
        // point, the operation is being waiting for this thread to try and stop
        // it.

        try {
            started.await();
        } catch (InterruptedException e) {
            fail("operation not triggered");
        }

        // Try and stop the operation. Since the operation will keep running
        // until we say otherwise, the stop() method will time out and return
        // false.

        try {
            assertFalse(operation.stop(500, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            fail("unable to stop the operation");
        }

        // Tell the operation to terminate, so the background thread can be
        // killed.

        terminate.countDown();
    }

}
