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

package org.apache.jackrabbit.oak.segment;

import java.util.function.Supplier;

import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;

/**
 * SegmentNodeStoreMonitor is notified for commit related operations performed by SegmentNodeStore.
 */
public interface SegmentNodeStoreMonitor {

    SegmentNodeStoreMonitor DEFAULT = new SegmentNodeStoreMonitor() {
        @Override
        public void onCommit(Thread t, long time) {

        }

        @Override
        public void onCommitQueued(Thread t, Supplier<GCGeneration> gcGeneration) {

        }
        
        @Override
        public void onCommitDequeued(Thread t, long time) {
            
        }
    };

    /**
     * Notifies the monitor when a new commit was persisted.
     * @param t the thread which initiated the write
     * @param time the time spent for persisting the commit
     */
    void onCommit(Thread t, long time);

    /**
     * Notifies the monitor when a new commit couldn't be persisted, but was
     * queued for later retry.
     * 
     * @param t the thread which initiated the write
     * @param gcGeneration the commit's gc generation
     */
    void onCommitQueued(Thread t, Supplier<GCGeneration> gcGeneration);
    
    /**
     * Notifies the monitor when a queued commit was dequeued for processing.
     * @param t the thread which initiated the write
     * @param time the time spent in the queue
     */
    void onCommitDequeued(Thread t, long time);

}