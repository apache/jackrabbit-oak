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

package org.apache.jackrabbit.oak.api.jmx;

import javax.management.openmbean.TabularData;

import aQute.bnd.annotation.ProviderType;

/**
 * MBean for managing {@link org.apache.jackrabbit.oak.spi.state.NodeStore#checkpoint checkpoints}.
 */
@ProviderType
public interface CheckpointMBean {
    String TYPE = "CheckpointManger";

    /**
     * List the checkpoints that are currently present along with
     * its id, creation time and expiry time.
     * @return
     */
    TabularData listCheckpoints();

    /**
     * Create a new checkpoint with the given {@code lifetime}.
     * @param lifetime
     * @return the id of the newly created checkpoint
     * @see org.apache.jackrabbit.oak.spi.state.NodeStore#checkpoint
     */
    String createCheckpoint(long lifetime);

    /**
     * Release the checkpoint with the given {@code id}.
     * @param id
     * @return  {@code true} on success, {@code false} otherwise.
     * @see org.apache.jackrabbit.oak.spi.state.NodeStore#checkpoint
     */
    boolean releaseCheckpoint(String id);
}
