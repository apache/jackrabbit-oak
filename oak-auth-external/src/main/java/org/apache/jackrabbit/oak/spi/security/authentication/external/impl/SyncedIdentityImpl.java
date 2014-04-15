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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;

/**
* {@code SyncedIdentityImpl}...
*/
public class SyncedIdentityImpl implements SyncedIdentity {

    private final String id;

    private final ExternalIdentityRef ref;

    private final boolean isGroup;

    private final long lastSynced;

    public SyncedIdentityImpl(String id, ExternalIdentityRef ref, boolean isGroup, long lastSynced) {
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
}