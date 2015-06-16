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
package org.apache.jackrabbit.oak.commons;

import java.util.Date;

public final class LongUtils {

    private LongUtils() {}

    /**
     * Sums {@code a} and {@code b} and verifies that it doesn't overflow in
     * signed long arithmetic, in which case {@link Long#MAX_VALUE} will be
     * returned instead of the result.
     *
     * Note: this method is a variant of {@link com.google.common.math.LongMath#checkedAdd(long, long)}
     * that returns {@link Long#MAX_VALUE} instead of throwing {@code ArithmeticException}.
     *
     * @see com.google.common.math.LongMath#checkedAdd(long, long)
     */
    public static long safeAdd(long a, long b) {
        long result = a + b;
        if ((a ^ b) < 0 | (a ^ result) >= 0) {
            return result;
        } else {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Calculate an expiration time based on {@code new Date().getTime()} and
     * the specified {@code expiration} in number of milliseconds.
     *
     * @param expiration The expiration in milliseconds.
     * @return The expiration time.
     */
    public static long calculateExpirationTime(long expiration) {
        return LongUtils.safeAdd(expiration, new Date().getTime());
    }
}
