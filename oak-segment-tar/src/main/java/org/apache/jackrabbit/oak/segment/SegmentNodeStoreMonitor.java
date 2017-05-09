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

/**
 * SegmentNodeStoreMonitor is notified for commit related operations performed by SegmentNodeStore.
 */
public interface SegmentNodeStoreMonitor {

    SegmentNodeStoreMonitor DEFAULT = new SegmentNodeStoreMonitor() {
        @Override
        public void onCommit() {

        }

        @Override
        public void onCommitQueued() {

        }
        
        @Override
        public void onCommitDequeued() {
            
        }
        
        @Override
        public void committedAfter(long time) {
            
        }
        
        @Override
        public void dequeuedAfter(long time) {
            
        }
    };

    /**
     * Notifies the monitor when a new commit was persisted right away
     */
    void onCommit();

    /**
     * Notifies the monitor when a new commit couldn't be persisted, but was
     * queued for later retry
     */
    void onCommitQueued();
    
    /**
     * Notifies the monitor when a queued commit was dequeued for processing.
     */
    void onCommitDequeued();

    /**
     * Notifies the monitor time spent (excluding queuing time) for a commit.
     * @param time the time spent
     */
    void committedAfter(long time);
    
    /**
     * Notifies the monitor time spent in the queue for a commit, before being processed.
     * @param time the time spent
     */
    void dequeuedAfter(long time);
}
