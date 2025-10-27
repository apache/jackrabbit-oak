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
package org.apache.jackrabbit.oak.commons.internal.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Unit cases for ExecutorUtils
 */
public class ExecutorUtilsTest {

    @Test
    public void testDirectExecutorHasOnlyOneInstance() {
        Executor executor = ExecutorUtils.directExecutor();
        Assert.assertSame(ExecutorUtils.directExecutor(), executor);
    }

    @Test
    public void testDirectExecutorRunsInCallingThread() {
        Executor executor = ExecutorUtils.directExecutor();
        final Thread[] runThread = new Thread[1];
        executor.execute(() -> runThread[0] = Thread.currentThread());
        Assert.assertEquals(Thread.currentThread(), runThread[0]);
    }


    @Test
    public void testDirectExecutorServiceReturnNewInstance() {
        ExecutorService executorService = ExecutorUtils.newDirectExecutorService();
        Assert.assertNotSame(ExecutorUtils.newDirectExecutorService(), executorService);
    }

    @Test
    public void testNewDirectExecutorServiceRunsInCallingThread() {
        Executor executor = ExecutorUtils.newDirectExecutorService();
        final Thread[] runThread = new Thread[1];
        executor.execute(() -> runThread[0] = Thread.currentThread());
        Assert.assertEquals(Thread.currentThread(), runThread[0]);
    }

    @Test
    public void testNewDirectExecutorServiceReturnsDirectExecutorService() {
        Object executorService = ExecutorUtils.newDirectExecutorService();
        Assert.assertTrue(executorService instanceof DirectExecutorService);
    }

    @Test
    public void testNewDirectExecutorServiceCallableReturnsValue() throws Exception {
        DirectExecutorService executorService = (DirectExecutorService) ExecutorUtils.newDirectExecutorService();
        Future<String> future = executorService.submit(() -> "test");
        Assert.assertEquals("test", future.get());
    }

}