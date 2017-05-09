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

package org.apache.jackrabbit.oak.commons.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExecutorCloserTest {

    @Test
    public void simple() throws Exception{
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        assertFalse(executorService.isTerminated());

        new ExecutorCloser(executorService).close();
        assertTrue(executorService.isTerminated());
    }

    @Test
    public void nullExecutor() throws Exception{
        new ExecutorCloser(null).close();
    }

    @Test
    public void timeoutHandling() throws Exception{
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        final CountDownLatch latch = new CountDownLatch(1);
        executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                latch.await();
                return null;
            }
        });
        new ExecutorCloser(executorService, 100, TimeUnit.MILLISECONDS).close();
        assertTrue(executorService.isShutdown());
    }

}
