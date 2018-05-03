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

import java.io.IOException;
import java.util.List;

class SynchronizedGarbageCollectionStrategy implements GarbageCollectionStrategy {

    private final Object lock = new Object();

    private final GarbageCollectionStrategy strategy;

    SynchronizedGarbageCollectionStrategy(GarbageCollectionStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public void collectGarbage(Context context) throws IOException {
        synchronized (lock) {
            strategy.collectGarbage(context);
        }
    }

    @Override
    public void collectFullGarbage(Context context) throws IOException {
        synchronized (lock) {
            strategy.collectFullGarbage(context);
        }
    }

    @Override
    public void collectTailGarbage(Context context) throws IOException {
        synchronized (lock) {
            strategy.collectTailGarbage(context);
        }
    }

    @Override
    public CompactionResult compactFull(Context context) throws IOException {
        synchronized (lock) {
            return strategy.compactFull(context);
        }
    }

    @Override
    public CompactionResult compactTail(Context context) throws IOException {
        synchronized (lock) {
            return strategy.compactTail(context);
        }
    }

    @Override
    public List<String> cleanup(Context context) throws IOException {
        synchronized (lock) {
            return strategy.cleanup(context);
        }
    }

}
