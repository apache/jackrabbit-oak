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

package org.apache.jackrabbit.oak.segment.scheduler;

import java.io.Closeable;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A {@code Scheduler} instance transforms changes to the content tree
 * into a queue of {@link Commit commits}.
 * <p>
 * An implementation is free to employ any scheduling strategy as long
 * as it guarantees all changes are applied atomically without changing
 * the semantics of the changes recorded in the {@code NodeBuilder} or
 * the semantics of the {@code CommitHook} contained in the actual {@code Commit} 
 * passed to the {@link #schedule(Commit, SchedulerOption) schedule}
 * method.
 */
public interface Scheduler {
    
    /**
     * Scheduling options for parameterizing individual commits.
     * (E.g. expedite, prioritize, defer, collapse, coalesce, parallelize, etc).
     *
     */
    interface SchedulerOption {}

    /**
     * Schedule a {@code commit}. This method blocks until the changes in this
     * {@code commit} have been processed and persisted. That is, until a call
     * to {@link Scheduler#getHeadNodeState()} would return a node state reflecting those
     * changes.
     *
     * @param commit    the commit
     * @param schedulingOptions       implementation specific scheduling options
     * @throws CommitFailedException  if the commit failed and none of the changes
     *                                have been applied.
     */
    NodeState schedule(@Nonnull Commit commit, SchedulerOption... schedulingOptions) throws CommitFailedException;
    
    /**
     * Creates a new checkpoint of the latest root of the tree. The checkpoint
     * remains valid for at least as long as requested and allows that state
     * of the repository to be retrieved using the returned opaque string
     * reference.
     * <p>
     * The {@code properties} passed to this methods are associated with the
     * checkpoint and can be retrieved through the {@link #checkpointInfo(String)}
     * method. Its semantics is entirely application specific.
     *
     * @param lifetime time (in milliseconds, &gt; 0) that the checkpoint
     *                 should remain available
     * @param properties properties to associate with the checkpoint
     * @return string reference of this checkpoint
     */
    String checkpoint(long lifetime, @Nonnull Map<String, String> properties);
    
    /**
     * Releases the provided checkpoint. If the provided checkpoint doesn't exist this method should return {@code true}.
     *
     * @param checkpoint string reference of a checkpoint
     * @return {@code true} if the checkpoint was successfully removed, or if it doesn't exist
     */
    boolean removeCheckpoint(String name);
    
    /**
     * Returns the latest state of the tree.
     * @return the latest state.
     */
    NodeState getHeadNodeState();
    
    /**
     * Register a new {@code Observer}. Clients need to call {@link Closeable#close()} 
     * to stop getting notifications on the registered observer and to free up any resources
     * associated with the registration.
     * 
     * @return a {@code Closeable} instance.
     */
    Closeable addObserver(Observer observer); 
}