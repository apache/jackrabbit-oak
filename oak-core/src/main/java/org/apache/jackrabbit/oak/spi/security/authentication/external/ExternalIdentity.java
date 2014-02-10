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

import java.security.Principal;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * {@code ExternalIdentity} defines an identity provided by an external system.
 */
public interface ExternalIdentity {

    /**
     * Returns the id of this identity as used in the external system.
     * @return the external id.
     */
    @Nonnull
    ExternalIdentityRef getExternalId();

    /**
     * Returns the local id of this identity as it would be used in this repository. This usually corresponds to
     * {@link org.apache.jackrabbit.api.security.user.Authorizable#getID()}
     *
     * @return the internal id.
     */
    @Nonnull
    String getId();

    /**
     * Returns the desired intermediate relative path of the authorizable to be created. For example, one could map
     * an external hierarchy into the local users and groups hierarchy.
     *
     * @return the intermediate path or {@code null} or empty.
     */
    @CheckForNull
    String getIntermediatePath();

    /**
     * Returns an iterable of the declared groups of this external identity.
     * @return the declared groups
     */
    @Nonnull
    Iterable<? extends ExternalGroup> getGroups();

    /**
     * Returns a map of properties of this external identity.
     * @return the properties
     */
    @Nonnull
    Map<String, ?> getProperties();

    // todo: really?
    Principal getPrincipal();

}