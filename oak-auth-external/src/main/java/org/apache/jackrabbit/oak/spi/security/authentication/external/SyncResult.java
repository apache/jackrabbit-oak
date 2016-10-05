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
 * Defines the result of a sync operation
 */
public interface SyncResult {

    /**
     * The synchronized identity
     * @return the identity
     */
    @CheckForNull
    SyncedIdentity getIdentity();

    /**
     * The status of the sync operation
     * @return the status
     */
    @Nonnull
    Status getStatus();

    /**
     * Result codes for sync operation.
     */
    enum Status {

        /**
         * No update
         */
        NOP,

        /**
         * authorizable added
         */
        ADD,

        /**
         * authorizable updated
         */
        UPDATE,

        /**
         * authorizable deleted
         */
        DELETE,

        /**
         * authorizable enabled
         */
        ENABLE,

        /**
         * authorizable disabled
         */
        DISABLE,

        /**
         * nothing changed. no such authorizable found.
         */
        NO_SUCH_AUTHORIZABLE,

        /**
         * nothing changed. no such identity found.
         */
        NO_SUCH_IDENTITY,

        /**
         * nothing changed. corresponding identity missing
         */
        MISSING,

        /**
         * nothing changed. idp name not correct
         */
        FOREIGN
    }

}