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

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;

/**
 * Constants used by the external identity management.
 *
 * @since Oak 1.5.3
 */
public interface ExternalIdentityConstants {

    /**
     * Name of the property storing the external identifier.
     * This property is of type {@link org.apache.jackrabbit.oak.api.Type#STRING}
     * and mandatory for external identities that have been synchronized into
     * the repository.
     *
     * @see DefaultSyncContext#REP_EXTERNAL_ID
     */
    String REP_EXTERNAL_ID = DefaultSyncContext.REP_EXTERNAL_ID;

    /**
     * Name of the property storing the date of the last synchronization of an
     * external identity.
     * This property is of type {@link org.apache.jackrabbit.oak.api.Type#DATE}
     *
     * @see DefaultSyncContext#REP_LAST_SYNCED
     */
    String REP_LAST_SYNCED = DefaultSyncContext.REP_LAST_SYNCED;

    /**
     * Name of the property storing the principal names of the external groups
     * a given external identity (user) is member. Not that the set depends on
     * the configured nesting
     * {@link org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig.User#getMembershipNestingDepth() depth}.
     * The existence of this property is optional and will only be created if
     * {@link org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig.User#getDynamicMembership()} is turned on.
     *
     * This property is of type {@link org.apache.jackrabbit.oak.api.Type#STRINGS}.
     * Please note, that for security reasons is system maintained and protected
     * on the Oak level and cannot be manipulated by regular {@code ContentSession}
     * objects irrespective of the effective permissions.
     */
    String REP_EXTERNAL_PRINCIPAL_NAMES = "rep:externalPrincipalNames";

    /**
     * The set of served property names defined by this interface.
     */
    Set<String> RESERVED_PROPERTY_NAMES = ImmutableSet.of(REP_EXTERNAL_ID, REP_EXTERNAL_PRINCIPAL_NAMES);

    /**
     * Configuration parameter to enable special protection of external IDs
     *
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4301">OAK-4301</a>
     */
    String PARAM_PROTECT_EXTERNAL_IDS = "protectExternalId";

    /**
     * Default value for {@link #PARAM_PROTECT_EXTERNAL_IDS}.
     */
    boolean DEFAULT_PROTECT_EXTERNAL_IDS = true;
}