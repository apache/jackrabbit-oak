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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimedRefreshPolicyTest {
    private Clock clock = new Clock.Virtual();
    private RecordingRunnable refreshCallback = new RecordingRunnable();

    @Test
    public void dirtyAndFirstCheck() throws Exception{
        clock.waitUntil(System.currentTimeMillis());
        TimedRefreshPolicy policy = new TimedRefreshPolicy(false, clock, TimeUnit.SECONDS, 1);
        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();

        policy.updated();

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertInvokedAndReset();

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();
    }

    @Test
    public void dirtyAndNotElapsedTimed() throws Exception{
        clock.waitUntil(System.currentTimeMillis());
        TimedRefreshPolicy policy = new TimedRefreshPolicy(false, clock, TimeUnit.SECONDS, 1);

        policy.updated();
        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertInvokedAndReset();

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();

        policy.updated();
        //Given time has not elapsed it should still be false
        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();
    }

    @Test
    public void dirtyAndElapsedTime() throws Exception{
        clock.waitUntil(System.currentTimeMillis());
        TimedRefreshPolicy policy = new TimedRefreshPolicy(false, clock, TimeUnit.SECONDS, 1);

        policy.updated();
        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertInvokedAndReset();

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();

        policy.updated();
        //Given time has not elapsed it should still be false
        //in both reader and writer mode
        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();

        //Let the refresh delta time elapse
        long refreshDelta = TimeUnit.SECONDS.toMillis(1) + 1;
        clock.waitUntil(System.currentTimeMillis() + refreshDelta);

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertInvokedAndReset();

        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();

        policy.updated();
        //Do similar check for read
        clock.waitUntil(clock.getTime() + refreshDelta);

        policy.refreshOnReadIfRequired(refreshCallback);
        refreshCallback.assertInvokedAndReset();

        policy.refreshOnReadIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();
    }

    @Test
    public void syncIndex() throws Exception{
        clock.waitUntil(System.currentTimeMillis());
        TimedRefreshPolicy policy = new TimedRefreshPolicy(true, clock, TimeUnit.SECONDS, 1);

        policy.updated();
        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertInvokedAndReset();

        policy.updated();

        //Write would not lead to reader refresh as time has yet not expired
        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertInvokedAndReset();

        //Already refreshed so reader should not be refreshed again
        policy.refreshOnReadIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();

        //Checking on write again should not lead to refresh
        policy.refreshOnWriteIfRequired(refreshCallback);
        refreshCallback.assertNotInvokedAndRest();
    }

    private static class RecordingRunnable implements Runnable {
        private boolean invoked;
        @Override
        public void run() {
            invoked = true;
        }

        public void assertInvokedAndReset(){
            assertTrue(invoked);
            reset();
        }

        public void assertNotInvokedAndRest(){
            assertFalse(invoked);
            reset();
        }

        void reset(){
            invoked = false;
        }
    }

}