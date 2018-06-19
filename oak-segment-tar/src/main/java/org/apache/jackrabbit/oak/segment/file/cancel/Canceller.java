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

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Represents a way to check for a cancellation request. Users of this class
 * (possibly cancelable, long-running operations) should periodically check
 * whether a cancellation request has been received.
 */
public class Canceller {

    private static final Canceller ROOT = new Canceller();

    private static final Cancellation NOPE = new Cancellation(false, null);

    /**
     * Create a new {@link Canceller} which is trivially empty. The returned
     * {@link Canceller} will never relay a positive cancellation request.
     *
     * @return an instance of {@link Canceller}.
     */
    public static Canceller newCanceller() {
        return ROOT;
    }

    Canceller() {
        // Prevent instantiation outside of this package.
    }

    /**
     * Check if cancellation has been requested. This method should be invoked
     * periodically, and the returned {@link Cancellation} should be inspected
     * and reacted upon.
     *
     * @return an instance of {@link Cancellation}.
     */
    public Cancellation check() {
        return NOPE;
    }

    /**
     * Return a new {@link Canceller} based on a boolean predicate. The returned
     * instance will relay a positive cancellation request when either the
     * supplied boolean predicate is {@code true} or this {@link Canceller} is
     * cancelled.
     *
     * @param reason    The reason associated to the boolean condition.
     * @param condition A boolean predicate.
     * @return a new instance of {@link Canceller}.
     */
    public Canceller withCondition(String reason, BooleanSupplier condition) {
        return new ConditionCanceller(this, reason, condition);
    }

    /**
     * Return a new {@link Canceller} based on time duration. The returned
     * instance will relay a positive cancellation request when either the
     * duration expires or this {@link Canceller} is cancelled.
     *
     * @param reason   The reason associated to the boolean condition.
     * @param duration The duration for the timeout.
     * @param unit     The time unit for the duration.
     * @return a new instance of {@link Canceller}.
     */
    public Canceller withTimeout(String reason, long duration, TimeUnit unit) {
        return new TimeoutCanceller(this, reason, duration, unit);
    }

    /**
     * Create a new {@link Canceller} based on this {@link Canceller}. The
     * returned instance will be canceled when this instance is canceled, but
     * will never transition back to an "uncanceled" state.
     *
     * @return an new instance of {@link Canceller}.
     */
    public Canceller withShortCircuit() {
        return new ShortCircuitCanceller(this);
    }

}
