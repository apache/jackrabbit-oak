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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncedIdentity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class ErrorSyncResult implements SyncResult {

    private final SyncedIdentity syncedIdentity;
    private final Exception error;

    ErrorSyncResult(@NotNull String userId, @Nullable String idpName, @NotNull Exception error) {
        ExternalIdentityRef ref = (idpName != null) ? new ExternalIdentityRef(userId, idpName) : null;
        this.syncedIdentity = new DefaultSyncedIdentity(userId, ref, false, -1);
        this.error = error;
    }

    ErrorSyncResult(@NotNull ExternalIdentityRef ref, @NotNull Exception error) {
        this.syncedIdentity = new DefaultSyncedIdentity(ref.getId(), ref, false, -1);
        this.error = error;
    }

    @NotNull
    @Override
    public SyncedIdentity getIdentity() {
        return syncedIdentity;
    }

    @NotNull
    @Override
    public Status getStatus() {
        return Status.NOP;
    }

    Exception getException() {
        return error;
    }
}