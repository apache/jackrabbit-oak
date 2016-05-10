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
     * Defines if synchronization keeps missing external identities on synchronization of authorizables. Default
     * is {@code false}.
     * @return {@code true} if keep missing.
     */
    boolean isKeepMissing();

    /**
     * See {@link #isKeepMissing()}
     */
    @Nonnull
    SyncContext setKeepMissing(boolean keep);

    /**
     * Defines if synchronization of users always will perform, i.e. ignores the last synced properties.
     * @return {@code true} if forced syncing users
     */
    boolean isForceUserSync();

    /**
     * See {@link #isForceUserSync()}
     */
    @Nonnull
    SyncContext setForceUserSync(boolean force);

    /**
     * Defines if synchronization of groups always will perform, i.e. ignores the last synced properties.
     * @return {@code true} if forced syncing groups
     */
    boolean isForceGroupSync();

    /**
     * See {@link #isForceGroupSync()}
     */
    @Nonnull
    SyncContext setForceGroupSync(boolean force);

    /**
     * Synchronizes an external identity with the repository based on the respective configuration.
     *
     * @param identity the identity to sync.
     * @return the result of the operation
     * @throws SyncException if an error occurs
     */
    @Nonnull
    SyncResult sync(@Nonnull ExternalIdentity identity) throws SyncException;

    /**
     * Synchronizes an authorizable with the corresponding external identity with the repository based on the respective
     * configuration.
     *
     * @param id the id of the authorizable
     * @return the result of the operation
     * @throws SyncException if an error occurs
     */
    @Nonnull
    SyncResult sync(@Nonnull String id) throws SyncException;


    /**
     * Closes this context and releases any resources bound to it. Note that an implementation must not commit the
     * {@link org.apache.jackrabbit.oak.api.Root} passed during the creation call. This is the responsibility of the
     * application.
     */
    void close();

}