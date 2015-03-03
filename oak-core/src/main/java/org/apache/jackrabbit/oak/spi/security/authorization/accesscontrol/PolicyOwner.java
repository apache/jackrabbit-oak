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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.security.AccessControlPolicy;

/**
 * Interface to improve pluggability of the {@link javax.jcr.security.AccessControlManager},
 * namely the interaction of multiple managers within a
 * single repository. It provides a single method {@link #defines(String, javax.jcr.security.AccessControlPolicy)}
 * that allows to determine the responsible manager upon
 * {@link javax.jcr.security.AccessControlManager#setPolicy(String, javax.jcr.security.AccessControlPolicy) setPolicy}
 * and
 * {@link javax.jcr.security.AccessControlManager#removePolicy(String, javax.jcr.security.AccessControlPolicy) removePolicy}.
 */
public interface PolicyOwner {

    /**
     * Determines if the implementing {@code AccessManager} defines the specified
     * {@code accessControlPolicy} at the given {@code absPath}. If this method
     * returns {@code true} it is expected that the given policy is valid to be
     * {@link javax.jcr.security.AccessControlManager#setPolicy(String, javax.jcr.security.AccessControlPolicy) set}
     * or {@link javax.jcr.security.AccessControlManager#removePolicy(String, javax.jcr.security.AccessControlPolicy) removed}
     * with the manager.
     *
     * @param absPath An absolute path.
     * @param accessControlPolicy The access control policy to be tested.
     * @return {@code true} If the {@code AccessControlManager} implementing this
     * interface can handle the specified {@code accessControlPolicy} at the given {@code path}.
     */
    boolean defines(@Nullable String absPath, @Nonnull AccessControlPolicy accessControlPolicy);
}