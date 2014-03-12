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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Represents a synchronized identity managed by a {@link SyncHandler}.
 */
public interface SyncedIdentity {

    /**
     * Returns the internal id or name of the corresponding authorizable.
     * @return the id.
     */
    @Nonnull
    String getId();

    /**
     * Returns the external reference of this identity.
     * @return the reference or {@code null}
     */
    @CheckForNull
    ExternalIdentityRef getExternalIdRef();

    /**
     * Checks if this identity represents a group.
     * @return {@code true} if group.
     */
    boolean isGroup();

    /**
     * Returns the time when this identity was last synced or a value less or equal to 0 if it was never synced.
     * @return the time when this identity was last synced.
     */
    long lastSynced();
}