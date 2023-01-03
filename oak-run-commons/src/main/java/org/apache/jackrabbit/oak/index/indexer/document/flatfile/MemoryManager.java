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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.util.Optional;

/**
 * Implementations of this interface can be used to get updates about remaining heap memory.
 */
public interface MemoryManager {
    /**
     * Indicates type of memory management this instance does.
     */
    enum Type {
        /**
         * Uses JMX memory management mbeans for tracking available memory.
         */
        JMX_BASED,
        /**
         * Maintains memory usage itself. Different users can share this instance and indicate the increase/decrease in
         * their memory usage by calling {@link #changeMemoryUsedBy(long)}, and check if the total memory used by all
         * users of this calls causes the available memory to become low using {@link #isMemoryLow()}
         */
        SELF_MANAGED
    }

    Type getType();

    /**
     * Register a client with this memory manager. All registered clients are informed when available memory level is low.
     * If memory level is already low, client is not registered.
     * NOTE - this method should only be used with {@link Type#JMX_BASED} instance types, otherwise it throws {@link UnsupportedOperationException}
     * @param client client to register
     * @return an optional containing registration id if registration was successful, empty optional otherwise
     */
    Optional<String> registerClient(MemoryManagerClient client);

    /**
     * Deregister a client with the given registrationID.
     * NOTE - this method should only be used with {@link Type#JMX_BASED} instance types, otherwise it throws {@link UnsupportedOperationException}
     * @param registrationID registration id of client to deregister
     * @return true if deregistration was successful, false otherwise.
     */
    boolean deregisterClient(String registrationID);

    /**
     * Checks if available memory is low.
     * NOTE - this method should only be used with {@link Type#SELF_MANAGED} instance types, otherwise it throws {@link UnsupportedOperationException}
     * @return true if available memory is low, false otherwise.
     */
    boolean isMemoryLow();

    /**
     * Adds the provided memory value to existing memory usage. Callers of this method could also provide negative values
     * to indicate reduction in memory usage.
     * NOTE - this method should only be used with {@link Type#SELF_MANAGED} instance types, otherwise it throws {@link UnsupportedOperationException}
     */
    void changeMemoryUsedBy(long memory);

}
