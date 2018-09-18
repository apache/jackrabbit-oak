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

package org.apache.jackrabbit.oak.plugins.index.importer;

import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbortingIndexerLockTest {

    private AsyncIndexInfoService infoService;
    private IndexStatsMBean statsMBean = mock(IndexStatsMBean.class);

    @Before
    public void setup(){
        infoService = mock(AsyncIndexInfoService.class);

        AsyncIndexInfo info = new AsyncIndexInfo("async", -1,-1, false, statsMBean);
        when(infoService.getInfo("async")).thenReturn(info);
    }

    @Test
    public void lockBasics() throws Exception{
        AbortingIndexerLock lock = new AbortingIndexerLock(infoService);

        when(statsMBean.getStatus()).thenReturn(IndexStatsMBean.STATUS_DONE);

        SimpleToken lockToken = lock.lock("async");
        assertNotNull(lockToken);
        verify(statsMBean).abortAndPause();

        lock.unlock(lockToken);
        verify(statsMBean).resume();

    }

    @Test
    public void lockWithRetry() throws Exception{
        AbortingIndexerLock lock = new AbortingIndexerLock(infoService);

        when(statsMBean.getStatus())
                .thenReturn(IndexStatsMBean.STATUS_RUNNING)
                .thenReturn(IndexStatsMBean.STATUS_RUNNING)
                .thenReturn(IndexStatsMBean.STATUS_DONE);

        SimpleToken lockToken = lock.lock("async");
        assertNotNull(lockToken);
        verify(statsMBean).abortAndPause();

        verify(statsMBean, times(3)).getStatus();
    }

    @Test
    public void lockTimedout() throws Exception{
        Clock.Virtual clock = new Clock.Virtual();
        AbortingIndexerLock lock = new AbortingIndexerLock(infoService, clock);

        when(statsMBean.getStatus())
                .thenReturn(IndexStatsMBean.STATUS_RUNNING)
                .thenReturn(IndexStatsMBean.STATUS_RUNNING)
                .then(invocation -> {
                    clock.waitUntil(AbortingIndexerLock.TIMEOUT_SECONDS * 1000 + 1);
                    return IndexStatsMBean.STATUS_RUNNING;
                });

        try {
            lock.lock("async");
            fail();
        } catch (IllegalStateException ignore) {

        }

        verify(statsMBean).abortAndPause();

        verify(statsMBean, times(3)).getStatus();
    }

}