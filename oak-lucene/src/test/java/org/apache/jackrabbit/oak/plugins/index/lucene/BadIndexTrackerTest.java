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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;

public class BadIndexTrackerTest {

    private VirtualTicker ticker = new VirtualTicker();
    private BadIndexTracker tracker = new BadIndexTracker();

    @Test
    public void basics() throws Exception{
        tracker.markBadIndexForRead("foo", new Exception());
        assertThat(tracker.getIndexPaths(), hasItem("foo"));

        assertTrue(tracker.isIgnoredBadIndex("foo"));

        tracker.markGoodIndex("foo");
        assertFalse(tracker.isIgnoredBadIndex("foo"));
    }

    @Test
    public void updatedIndexesMakesGood() throws Exception{
        tracker.markBadIndexForRead("foo", new Exception());
        assertTrue(tracker.isIgnoredBadIndex("foo"));

        tracker.markGoodIndexes(Collections.singleton("foo"));
        assertFalse(tracker.isIgnoredBadIndex("foo"));
    }

    @Test
    public void recheckDelay() throws Exception{
        tracker = new BadIndexTracker(100);
        tracker.setTicker(ticker);
        tracker.markBadIndexForRead("foo", new Exception());
        ticker.addTime(50, TimeUnit.MILLISECONDS);

        assertTrue(tracker.isIgnoredBadIndex("foo"));

        ticker.addTime(30, TimeUnit.MILLISECONDS);
        assertTrue(tracker.isIgnoredBadIndex("foo"));

        //Now cross the threshold
        ticker.addTime(30, TimeUnit.MILLISECONDS);
        assertFalse(tracker.isIgnoredBadIndex("foo"));

        //However index is still considered bad
        assertThat(tracker.getIndexPaths(), hasItem("foo"));
    }

}