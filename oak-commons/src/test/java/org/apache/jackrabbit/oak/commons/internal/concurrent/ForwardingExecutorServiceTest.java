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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Unit cases for ForwardingExecutorService
 */
public class ForwardingExecutorServiceTest {

    private ExecutorService mockDelegate;
    private ForwardingExecutorService forwardingService;

    @Before
    public void setUp() {
        mockDelegate = Mockito.mock(ExecutorService.class);
        forwardingService = new ForwardingExecutorService() {
            @Override
            public ExecutorService delegate() {
                return mockDelegate;
            }
        };
    }

    @Test
    public void testExecuteDelegation() {
        Runnable task = Mockito.mock(Runnable.class);
        forwardingService.execute(task);
        Mockito.verify(mockDelegate).execute(task);
    }

    @Test
    public void testSubmitRunnableDelegation() {
        Runnable task = Mockito.mock(Runnable.class);
        Future future = Mockito.mock(Future.class);
        Mockito.when(mockDelegate.submit(task)).thenReturn(future);
        Assert.assertEquals(future, forwardingService.submit(task));
        Mockito.verify(mockDelegate).submit(task);
    }

    @Test
    public void testSubmitRunnableWithResultDelegation() {
        Runnable task = Mockito.mock(Runnable.class);
        Future<String> future = Mockito.mock(Future.class);
        Mockito.when(mockDelegate.submit(task, "result")).thenReturn(future);
        Assert.assertEquals(future, forwardingService.submit(task, "result"));
        Mockito.verify(mockDelegate).submit(task, "result");
    }

    @Test
    public void testSubmitCallableDelegation() {
        Callable<String> callable = Mockito.mock(Callable.class);
        Future<String> future = Mockito.mock(Future.class);
        Mockito.when(mockDelegate.submit(callable)).thenReturn(future);
        Assert.assertEquals(future, forwardingService.submit(callable));
        Mockito.verify(mockDelegate).submit(callable);
    }

    @Test
    public void testShutdownDelegation() {
        forwardingService.shutdown();
        Mockito.verify(mockDelegate).shutdown();
    }

    @Test
    public void testShutdownNowDelegation() {
        List<Runnable> tasks = Collections.emptyList();
        Mockito.when(mockDelegate.shutdownNow()).thenReturn(tasks);
        Assert.assertEquals(tasks, forwardingService.shutdownNow());
        Mockito.verify(mockDelegate).shutdownNow();
    }

    @Test
    public void testIsShutdownDelegation() {
        Mockito.when(mockDelegate.isShutdown()).thenReturn(true);
        Assert.assertTrue(forwardingService.isShutdown());
        Mockito.verify(mockDelegate).isShutdown();
    }

    @Test
    public void testIsTerminatedDelegation() {
        Mockito.when(mockDelegate.isTerminated()).thenReturn(true);
        Assert.assertTrue(forwardingService.isTerminated());
        Mockito.verify(mockDelegate).isTerminated();
    }

    @Test
    public void testAwaitTerminationDelegation() throws InterruptedException {
        Mockito.when(mockDelegate.awaitTermination(1, TimeUnit.SECONDS)).thenReturn(true);
        Assert.assertTrue(forwardingService.awaitTermination(1, TimeUnit.SECONDS));
        Mockito.verify(mockDelegate).awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testInvokeAllDelegation() throws InterruptedException {
        List<Callable<String>> tasks = Arrays.asList(Mockito.mock(Callable.class));
        List<Future<String>> futures = Arrays.asList(Mockito.mock(Future.class));
        Mockito.when(mockDelegate.invokeAll(tasks)).thenReturn(futures);
        Assert.assertEquals(futures, forwardingService.invokeAll(tasks));
        Mockito.verify(mockDelegate).invokeAll(tasks);
    }

    @Test
    public void testInvokeAllWithTimeoutDelegation() throws InterruptedException {
        List<Callable<String>> tasks = Arrays.asList(Mockito.mock(Callable.class));
        List<Future<String>> futures = Arrays.asList(Mockito.mock(Future.class));
        Mockito.when(mockDelegate.invokeAll(tasks, 1, TimeUnit.SECONDS)).thenReturn(futures);
        Assert.assertEquals(futures, forwardingService.invokeAll(tasks, 1, TimeUnit.SECONDS));
        Mockito.verify(mockDelegate).invokeAll(tasks, 1, TimeUnit.SECONDS);
    }

    @Test
    public void testInvokeAnyDelegation() throws Exception {
        List<Callable<String>> tasks = Arrays.asList(Mockito.mock(Callable.class));
        Mockito.when(mockDelegate.invokeAny(tasks)).thenReturn("result");
        Assert.assertEquals("result", forwardingService.invokeAny(tasks));
        Mockito.verify(mockDelegate).invokeAny(tasks);
    }

    @Test
    public void testInvokeAnyWithTimeoutDelegation() throws Exception {
        List<Callable<String>> tasks = Arrays.asList(Mockito.mock(Callable.class));
        Mockito.when(mockDelegate.invokeAny(tasks, 1, TimeUnit.SECONDS)).thenReturn("result");
        Assert.assertEquals("result", forwardingService.invokeAny(tasks, 1, TimeUnit.SECONDS));
        Mockito.verify(mockDelegate).invokeAny(tasks, 1, TimeUnit.SECONDS);
    }

}