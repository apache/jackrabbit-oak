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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import javax.annotation.Nonnull;

/**
 * {@code SyncContext} is used as scope for sync operations. Implementations are free to associate any resources with
 * the sync context. The sync context must not be accessed concurrently and must be closed after use.
 */
public interface SyncContext {

    /**
     * Synchronizes an external identity with the repository based on the respective configuration.
     *
     * @param identity the identity to sync.
     * @return {@code true} if the given identity was synced; {@code false} for no change.
     * @throws SyncException if an error occurrs
     */
    boolean sync(@Nonnull ExternalIdentity identity) throws SyncException;

    /**
     * Closes this context and releases any resources bound to it. Note that an implementation must not commit the
     * {@link org.apache.jackrabbit.oak.api.Root} passed during the creation call. This is the responsibility of the
     * application.
     */
    void close();
}