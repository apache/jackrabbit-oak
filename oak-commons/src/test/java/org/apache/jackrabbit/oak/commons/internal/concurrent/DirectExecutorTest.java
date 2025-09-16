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

/**
 * Unit case for {@link DirectExecutor}
 */
public class DirectExecutorTest {

    @Test
    public void testExecuteRunsInCallingThread() {
        final String callingThread = Thread.currentThread().getName();
        final String[] executedThread = new String[1];

        DirectExecutor.INSTANCE.execute(() -> executedThread[0] = Thread.currentThread().getName());

        Assert.assertEquals(callingThread, executedThread[0]);
    }

    @Test
    public void testExecuteRunsImmediately() {
        final boolean[] ran = {false};
        DirectExecutor.INSTANCE.execute(() -> ran[0] = true);
        Assert.assertTrue(ran[0]);
    }
}