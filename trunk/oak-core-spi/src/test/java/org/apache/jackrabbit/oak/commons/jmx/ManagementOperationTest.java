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

package org.apache.jackrabbit.oak.commons.jmx;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean.StatusCode.FAILED;
import static org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean.StatusCode.RUNNING;
import static org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean.StatusCode.SUCCEEDED;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.newManagementOperation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;

import javax.management.openmbean.CompositeData;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.jackrabbit.oak.commons.jmx.ManagementOperation;
import org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ManagementOperationTest {
    private ListeningExecutorService executor;

    @Before
    public void setup() {
        executor = listeningDecorator(newCachedThreadPool());
    }

    @After
    public void tearDown() {
        executor.shutdown();
    }

    @Test(expected = IllegalStateException.class)
    public void notStarted() throws ExecutionException, InterruptedException {
        ManagementOperation<Integer> op = ManagementOperation.done("test", 42);
        assertTrue(op.isDone());
        assertEquals(42, (int) op.get());
        sameThreadExecutor().execute(op);
    }

    @Test
    public void succeeded() throws InterruptedException, ExecutionException, TimeoutException {
        ManagementOperation<Long> op = newManagementOperation("test", new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return 42L;
            }
        });

        executor.execute(op);
        assertEquals(42L, (long) op.get(5, SECONDS));
        assertTrue(op.isDone());
        Status status = op.getStatus();
        assertEquals(op.getId(), status.getId());
        assertEquals(SUCCEEDED, status.getCode());
    }

    @Test
    public void failed() throws InterruptedException, TimeoutException {
        final Exception failure = new Exception("fail");
        ManagementOperation<Void> op = newManagementOperation("test", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                throw failure;
            }
        });

        executor.execute(op);
        try {
            assertEquals(null, op.get(5, SECONDS));
            fail("Expected " + failure);
        } catch (ExecutionException e) {
            assertEquals(failure, e.getCause());
        }
        assertTrue(op.isDone());
        Status status = op.getStatus();
        assertEquals(op.getId(), status.getId());
        assertEquals(FAILED, status.getCode());
        assertEquals("test failed: " + failure.getMessage(), status.getMessage());
    }

    @Test
    public void running() throws InterruptedException {
        final LinkedBlockingDeque<Thread> thread = new LinkedBlockingDeque<Thread>(1);

        ManagementOperation<Void> op = newManagementOperation("test", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                thread.add(currentThread());
                sleep(100000);
                return null;
            }
        });

        executor.execute(op);
        Status status = op.getStatus();
        assertEquals(op.getId(), status.getId());
        assertEquals(RUNNING, status.getCode());

        thread.poll(5, SECONDS).interrupt();
        try {
            op.get();
            fail("Expected InterruptedException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof InterruptedException);
        }
        assertTrue(op.isDone());
        status = op.getStatus();
        assertEquals(op.getId(), status.getId());
        assertEquals(FAILED, status.getCode());
        assertTrue(status.getMessage().contains("test failed: "));
    }

    @Test
    public void cancelled() {
        ManagementOperation<Void> op = newManagementOperation("test", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                return null;
            }
        });

        op.cancel(false);
        executor.execute(op);

        assertTrue(op.isDone());
        Status status = op.getStatus();
        assertEquals(op.getId(), status.getId());
        assertEquals(FAILED, status.getCode());
        assertEquals("test cancelled", status.getMessage());
    }

    private static void checkConversion(Status status) {
        CompositeData cd = status.toCompositeData();
        assertEquals(status.getCode().ordinal(), cd.get("code"));
        assertEquals(status.getId(), cd.get("id"));
        assertEquals(status.getMessage(), cd.get("message"));

        Status status2 = Status.fromCompositeData(cd);
        CompositeData cd2 = status2.toCompositeData();
        assertEquals(status, status2);
        assertEquals(cd, cd2);
    }

    @Test
    public void statusToCompositeDataConversion() {
        checkConversion(Status.unavailable("forty two"));
        checkConversion(Status.none("forty three"));
        checkConversion(Status.initiated("forty four"));
        checkConversion(Status.running("forty five"));
        checkConversion(Status.succeeded("forty six"));
        checkConversion(Status.failed("forty seven"));
    }
}
