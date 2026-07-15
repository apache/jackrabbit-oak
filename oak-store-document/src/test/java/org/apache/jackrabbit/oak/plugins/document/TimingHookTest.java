/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class TimingHookTest {

    private static long DELAY_MS = 20;

    @Test
    public void commitTime() throws CommitFailedException {
        AtomicLong processingTime = new AtomicLong();
        TimingHook.wrap(
                (before, after, info) -> sleep(),
                (time, unit) -> processingTime.set(unit.toMillis(time))
        ).processCommit(EMPTY_NODE, EMPTY_NODE, CommitInfo.EMPTY);
        // lower bound for processing time is accuracy on Windows (10 ms)
        // because Thread.sleep() may actually sleep less than the specified
        // amount of time on Windows.
        assertThat(processingTime.get(), greaterThanOrEqualTo(DELAY_MS / 2));
    }

    @NotNull
    private NodeState sleep() {
        try {
            Thread.sleep(DELAY_MS);
        } catch (InterruptedException e) {
            // ignore
        }
        return EMPTY_NODE;
    }
}
