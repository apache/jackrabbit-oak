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

package org.apache.jackrabbit.oak.segment.file;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.file.EstimationStrategy.Context;

class SizeDeltaGcEstimation {

    private final EstimationStrategy estimationStrategy;

    private final EstimationStrategy.Context context;

    SizeDeltaGcEstimation(long delta, @Nonnull GCJournal gcJournal, long currentSize, boolean full) {
        if (full) {
            estimationStrategy = new FullSizeDeltaEstimationStrategy();
        } else {
            estimationStrategy = new TailSizeDeltaEstimationStrategy();
        }

        context = new Context() {

            @Override
            public long getSizeDelta() {
                return delta;
            }

            @Override
            public long getCurrentSize() {
                return currentSize;
            }

            @Override
            public GCJournal getGCJournal() {
                return gcJournal;
            }

        };
    }

    public EstimationResult estimate() {
        return estimationStrategy.estimate(context);
    }

}
