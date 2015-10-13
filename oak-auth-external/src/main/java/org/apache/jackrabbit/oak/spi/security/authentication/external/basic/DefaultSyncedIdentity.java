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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;

/**
 * Implements a simple synced identity that maps an authorizable id to an external ref.
 */
public class DefaultSyncedIdentity implements SyncedIdentity {

    private final String id;

    private final ExternalIdentityRef ref;

    private final boolean isGroup;

    private final long lastSynced;

    public DefaultSyncedIdentity(@Nonnull String id, @Nullable ExternalIdentityRef ref, boolean isGroup, long lastSynced) {
        this.id = id;
        this.ref = ref;
        this.isGroup = isGroup;
        this.lastSynced = lastSynced;
    }

    @Nonnull
    @Override
    public String getId() {
        return id;
    }

    @CheckForNull
    @Override
    public ExternalIdentityRef getExternalIdRef() {
        return ref;
    }

    @Override
    public boolean isGroup() {
        return isGroup;
    }

    @Override
    public long lastSynced() {
        return lastSynced;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DefaultSyncedIdentity{");
        sb.append("id='").append(id).append('\'');
        sb.append(", ref=").append(ref);
        sb.append(", isGroup=").append(isGroup);
        sb.append(", lastSynced=").append(lastSynced);
        sb.append('}');
        return sb.toString();
    }
}