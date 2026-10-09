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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.async;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;

/** Holds real {@link CacheActionDispatcher} actions until a test explicitly executes them. */
public final class TestCacheActionDispatcher extends CacheActionDispatcher {
    private final Queue<CacheAction> actions = new ConcurrentLinkedQueue<>();

    /**
     * Creates a real dispatcher whose zero-byte budget rejects writes and invalidations.
     * @return a dispatcher that cannot accept cache actions
     */
    public static CacheActionDispatcher rejectingDispatcher() {
        return new CacheActionDispatcher(0);
    }

    /** Accepts an action without starting a background worker. */
    @Override
    public boolean add(CacheAction action) {
        actions.add(action);
        return true;
    }

    /** Returns how many accepted actions still await execution. */
    public int pendingCount() { return actions.size(); }

    /** Executes the accepted actions in their original order. */
    public void executeAll() {
        while (!actions.isEmpty()) {
            actions.remove().execute();
        }
    }
}
