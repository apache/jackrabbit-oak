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

import java.util.concurrent.Phaser;

/**
 * Client interface for {@link MemoryManager}.
 */
@FunctionalInterface
public interface MemoryManagerClient {

    /**
     * This method is invoked by {@link MemoryManager} which this client registers to, when the available heap memory is low.
     * It is expected that the client performs some clean up action to help with dealing with this shortage of memory. Client needs to
     * signal the {@link MemoryManager} when that action completes. The passed phaser needs to be used for that purpose.
     * Client should register with this phaser before performing the clean up and arrive and deregister itself after clean up is done.
     * If no cleanup is possible/required, just register and arrive and deregister immediately.
     * @param phaser phaser used to coordinate with {@link MemoryManager}
     */
    void memoryLow(Phaser phaser);

}
