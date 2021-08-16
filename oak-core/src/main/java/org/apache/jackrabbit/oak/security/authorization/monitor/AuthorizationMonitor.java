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
package org.apache.jackrabbit.oak.security.authorization.monitor;

import org.apache.jackrabbit.oak.stats.Monitor;
import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface AuthorizationMonitor extends Monitor<AuthorizationMonitor> {

    /**
     * Called to mark an access violation in the default permission validator.
     */
    void accessViolation();

    /**
     * Called to mark unexpected errors related to the permission store. It does does not cover access violations,
     * but actual operational errors that probably need to be investigated. Any triggered event should have a
     * corresponding error logged to make this investigation possible.
     */
    void permissionError();

    /**
     * Called when the {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider} is
     * being refreshed and permission caches are cleared.
     */
    void permissionRefresh();

    /**
     * Called to record the time it takes to eagerly load all permissions for a given principal.
     *
     * @param timeTakenNanos Time in nanoseconds.
     */
    void permissionAllLoaded(long timeTakenNanos);
}
