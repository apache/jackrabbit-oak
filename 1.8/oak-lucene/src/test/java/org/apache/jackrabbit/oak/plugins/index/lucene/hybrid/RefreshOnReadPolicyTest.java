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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

public class RefreshOnReadPolicyTest {
    private Clock clock = new Clock.Virtual();
    private RecordingRunnable refreshCallback = new RecordingRunnable();
    private RefreshOnReadPolicy policy = new RefreshOnReadPolicy(clock, TimeUnit.SECONDS, 1);
    private long refreshDelta = TimeUnit.SECONDS.toMillis(1) + 1;

    @Test
    public void noRefreshOnReadIfNotUpdated() throws Exception{
        policy.refreshOnReadIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndReset();
    }

    @Test
    public void refreshOnFirstWrite() throws Exception{
        clock.waitUntil(System.currentTimeMillis());

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertInvokedAndReset();
    }

    @Test
    public void refreshOnReadAfterWrite() throws Exception{
        clock.waitUntil(System.currentTimeMillis());

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.reset();
        //Call again without change in time
        policy.refreshOnWriteIfRequired(refreshCallback);

        //This time callback should not be invoked
        refreshCallback.assertNotInvokedAndReset();

        policy.refreshOnReadIfRequired(refreshCallback);
        //On read the callback should be invoked
        refreshCallback.assertInvokedAndReset();
    }

    @Test
    public void refreshOnWriteWithTimeElapsed() throws Exception{
        clock.waitUntil(System.currentTimeMillis());

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.reset();

        //Call again without change in time
        policy.refreshOnWriteIfRequired(refreshCallback);

        //This time callback should not be invoked
        refreshCallback.assertNotInvokedAndReset();

        clock.waitUntil(clock.getTime() + refreshDelta);

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertInvokedAndReset();
    }

}