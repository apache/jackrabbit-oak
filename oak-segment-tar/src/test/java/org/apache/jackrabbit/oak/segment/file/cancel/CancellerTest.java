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

package org.apache.jackrabbit.oak.segment.file.cancel;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Assert;
import org.junit.Test;

public class CancellerTest {

    private static void assertNotCancelled(Cancellation c) {
        Assert.assertFalse(c.isCancelled());
        Assert.assertFalse(c.getReason().isPresent());
    }

    private static void assertCancelled(Cancellation c, String reason) {
        Assert.assertTrue(c.isCancelled());
        Assert.assertEquals(c.getReason(), Optional.of(reason));
    }

    @Test
    public void emptyCancellerShouldNotCancel() {
        Cancellation c = Canceller.newCanceller().check();
        assertNotCancelled(c);
    }

    @Test
    public void trueConditionShouldCancel() {
        Cancellation c = Canceller.newCanceller().withCondition("reason", () -> true).check();
        assertCancelled(c, "reason");
    }

    @Test
    public void falseConditionShouldNotCancel() {
        Cancellation c = Canceller.newCanceller().withCondition("reason", () -> false).check();
        assertNotCancelled(c);
    }

    @Test
    public void falseConditionShouldCheckParent() {
        Cancellation c = Canceller.newCanceller()
            .withCondition("parent", () -> true)
            .withCondition("child", () -> false)
            .check();
        assertCancelled(c, "parent");
    }

    @Test
    public void expiredTimeoutShouldCancel() throws Exception {
        Canceller canceller = Canceller.newCanceller().withTimeout("reason", 1, TimeUnit.MILLISECONDS);
        Thread.sleep(10);
        Cancellation c = canceller.check();
        assertCancelled(c, "reason");
    }

    @Test
    public void validTimeoutShouldNotCancel() {
        Cancellation c = Canceller.newCanceller().withTimeout("reason", 1, TimeUnit.DAYS).check();
        assertNotCancelled(c);
    }

    @Test
    public void validTimeoutShouldCheckParent() {
        Cancellation c = Canceller.newCanceller()
            .withCondition("parent", () -> true)
            .withTimeout("child", 1, TimeUnit.DAYS)
            .check();
        assertCancelled(c, "parent");
    }

    @Test
    public void shortCircuitShouldCancelWhenParentCancel() {
        Cancellation c = Canceller.newCanceller()
            .withCondition("reason", () -> true)
            .withShortCircuit()
            .check();
        assertCancelled(c, "reason");
    }

    @Test
    public void shortCircuitShouldNotCancelWhenParentDoesNotCancel() {
        Cancellation c = Canceller.newCanceller()
            .withCondition("reason", () -> false)
            .withShortCircuit()
            .check();
        assertNotCancelled(c);
    }

    @Test
    public void shortCircuitShouldBreakCircuitWithParentOnFailure() {
        MutableBoolean b = new MutableBoolean(false);
        Canceller c = Canceller.newCanceller()
            .withCondition("reason", b::booleanValue)
            .withShortCircuit();
        assertNotCancelled(c.check());
        b.setValue(true);
        assertCancelled(c.check(), "reason");
        b.setValue(false);
        assertCancelled(c.check(), "reason");
    }

}
