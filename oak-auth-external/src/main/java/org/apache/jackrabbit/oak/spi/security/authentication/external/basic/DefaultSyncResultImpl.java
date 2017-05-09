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

import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;

/**
 * Implements a simple sync result with and id and a status.
 */
public class DefaultSyncResultImpl implements SyncResult {

    private final DefaultSyncedIdentity id;

    private Status status = Status.NOP;

    public DefaultSyncResultImpl(@Nullable DefaultSyncedIdentity id, @Nonnull Status status) {
        this.id = id;
        this.status = status;
    }

    @Override
    @CheckForNull
    public DefaultSyncedIdentity getIdentity() {
        return id;
    }

    @Override
    @Nonnull
    public Status getStatus() {
        return status;
    }

    public void setStatus(@Nonnull Status status) {
        this.status = status;
    }
}