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

    private final GarbageCollectionStrategy strategy;

    SynchronizedGarbageCollectionStrategy(GarbageCollectionStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public synchronized void collectGarbage(Context context) throws IOException {
        strategy.collectGarbage(context);
    }

    @Override
    public synchronized void  collectFullGarbage(Context context) throws IOException {
        strategy.collectFullGarbage(context);
    }

    @Override
    public synchronized void collectTailGarbage(Context context) throws IOException {
        strategy.collectTailGarbage(context);
    }

    @Override
    public synchronized CompactionResult compactFull(Context context) throws IOException {
        return strategy.compactFull(context);
    }

    @Override
    public synchronized CompactionResult compactTail(Context context) throws IOException {
        return strategy.compactTail(context);
    }

    @Override
    public synchronized List<String> cleanup(Context context) throws IOException {
        return strategy.cleanup(context);
    }

}
