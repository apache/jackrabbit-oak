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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.jackrabbit.oak.plugins.document.ThrottlingMetrics;
import org.jetbrains.annotations.NotNull;

/**
 * Java Object to represent Mongo throttling metrics
 */
public class MongoThrottlingMetrics implements ThrottlingMetrics {
    @NotNull
    final AtomicDouble oplogWindow;
    private final int threshold;
    private final long throttlingTime;

    MongoThrottlingMetrics(final @NotNull AtomicDouble oplogWindow, int threshold, long throttlingTime) {
        this.oplogWindow = oplogWindow;
        this.threshold = threshold;
        this.throttlingTime = throttlingTime;
    }

    @Override
    public int threshold() {
        return threshold;
    }

    @Override
    public double currValue() {
        return oplogWindow.doubleValue();
    }

    @Override
    public long throttlingTime() {
        return throttlingTime;
    }

    public static MongoThrottlingMetrics of(final @NotNull AtomicDouble oplogWindow, int threshold, long throttlingTime) {
        return new MongoThrottlingMetrics(oplogWindow, threshold, throttlingTime);
    }
}
