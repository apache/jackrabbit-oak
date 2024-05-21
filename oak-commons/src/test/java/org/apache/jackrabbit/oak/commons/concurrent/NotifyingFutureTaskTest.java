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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;
import org.junit.Test;

public class NotifyingFutureTaskTest {

    @Test
    public void onCompletion() throws Exception {
        CountingCallable callable = new CountingCallable();
        CountingRunnable runnable = new CountingRunnable();
        NotifyingFutureTask nft = new NotifyingFutureTask(callable);
        nft.onComplete(runnable);
        nft.run();
        assertEquals(1, callable.count);
        assertEquals(1, runnable.count);

        nft.run();
        assertEquals("Callback should be invoked only once", 1, runnable.count);
    }

    @Test
    public void completed() throws Exception {
        CountingRunnable runnable = new CountingRunnable();
        NotifyingFutureTask nft = NotifyingFutureTask.completed();
        nft.onComplete(runnable);
        assertEquals("Callback should still be invoked if already done", 1, runnable.count);
    }

    private static class CountingRunnable implements Runnable {

        int count;

        @Override
        public void run() {
            count++;
        }
    }

    private static class CountingCallable implements Callable<Void> {

        int count;

        @Override
        public Void call() throws Exception {
            count++;
            return null;
        }
    }

}
