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

import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Test handler that does not actually perform recovery, but simply acquires the
 * recovery lock and releases it again with a success status.
 */
public class SimpleRecoveryHandler implements RecoveryHandler {

    private final DocumentStore store;

    private final Clock clock;

    public SimpleRecoveryHandler(@NotNull DocumentStore store,
                                 @NotNull Clock clock) {
        this.store = checkNotNull(store);
        this.clock = checkNotNull(clock);
    }

    @Override
    public boolean recover(int clusterId) {
        // simulate recovery by acquiring recovery lock
        RecoveryLock lock = new RecoveryLock(store, clock, clusterId);
        if (lock.acquireRecoveryLock(clusterId)) {
            lock.releaseRecoveryLock(true);
            return true;
        }
        return false;
    }
}
