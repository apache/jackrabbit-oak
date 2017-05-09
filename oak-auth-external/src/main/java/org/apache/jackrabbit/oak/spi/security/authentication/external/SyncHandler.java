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

import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.api.security.user.UserManager;

/**
 * SyncHandler is used to sync users and groups from an {@link ExternalIdentityProvider}.
 * The synchronization performed within the scope of a {@link SyncContext} which is acquired during the
 * {@link #createContext(ExternalIdentityProvider, org.apache.jackrabbit.api.security.user.UserManager, javax.jcr.ValueFactory)} call.
 *
 * The exact configuration is managed by the sync handler instance. The system may contain several sync handler
 * implementations with different configurations. those are managed by the {@link SyncManager}.
 *
 * @see org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext
 * @see org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager
 */
public interface SyncHandler {

    /**
     * Returns the name of this sync handler.
     * @return sync handler name
     */
    @Nonnull
    String getName();

    /**
     * Initializes a sync context which is used to start the sync operations.
     *
     * @param idp the external identity provider used for syncing
     * @param userManager user manager for managing authorizables
     * @param valueFactory the value factory to create values
     * @return the sync context
     * @throws SyncException if an error occurs
     */
    @Nonnull
    SyncContext createContext(@Nonnull ExternalIdentityProvider idp,
                              @Nonnull UserManager userManager,
                              @Nonnull ValueFactory valueFactory) throws SyncException;

    /**
     * Tries to find the identity with the given authorizable id or name.
     * @param userManager the user manager
     * @param id the id or name of the authorizable
     * @return a synced identity object or {@code null}
     * @throws RepositoryException if an error occurs
     */
    @CheckForNull
    SyncedIdentity findIdentity(@Nonnull UserManager userManager, @Nonnull String id) throws RepositoryException;

    /**
     * Checks if the identity requires sync based on the configuration, type and last sync time.
     * @param identity the identity to check
     * @return {@code true} if the identity requires synchronization.
     */
    boolean requiresSync(@Nonnull SyncedIdentity identity);

    /**
     * Lists all externally synced identities.
     * @param userManager the user manager
     * @return an iterator over all authorizable that are externally synced.
     * @throws RepositoryException if an error occurs
     */
    @Nonnull
    Iterator<SyncedIdentity> listIdentities(@Nonnull UserManager userManager) throws RepositoryException;

}