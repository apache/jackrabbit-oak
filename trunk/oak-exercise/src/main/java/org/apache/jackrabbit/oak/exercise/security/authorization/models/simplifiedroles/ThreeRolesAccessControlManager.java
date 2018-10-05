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
package org.apache.jackrabbit.oak.exercise.security.authorization.models.simplifiedroles;

import javax.jcr.AccessDeniedException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.lock.LockException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import javax.jcr.version.VersionException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class ThreeRolesAccessControlManager implements PolicyOwner, AccessControlManager {

    private final String supportedPath;

    ThreeRolesAccessControlManager(@NotNull Root root, @NotNull String supportedPath, @NotNull SecurityProvider securityProvider) {
        this.supportedPath = supportedPath;
        // EXERCISE
    }

    @Override
    public Privilege[] getSupportedPrivileges(String absPath) throws PathNotFoundException, RepositoryException {
        // EXERCISE
        return new Privilege[0];
    }

    @Override
    public Privilege privilegeFromName(String privilegeName) throws AccessControlException, RepositoryException {
        // EXERCISE
        return null;
    }

    @Override
    public boolean hasPrivileges(String absPath, Privilege[] privileges) throws PathNotFoundException, RepositoryException {
        // EXERCISE
        return false;
    }

    @Override
    public Privilege[] getPrivileges(String absPath) throws PathNotFoundException, RepositoryException {
        // EXERCISE
        return new Privilege[0];
    }

    /**
     * See {@code L5_CustomAccessControlManagementTest.testGetPolicies}
     */
    @Override
    public AccessControlPolicy[] getPolicies(String absPath) throws PathNotFoundException, AccessDeniedException, RepositoryException {
        // EXERCISE
        return new AccessControlPolicy[0];
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(String absPath) throws PathNotFoundException, AccessDeniedException, RepositoryException {
        // EXERCISE
        return new AccessControlPolicy[0];
    }

    @Override
    public AccessControlPolicyIterator getApplicablePolicies(String absPath) throws PathNotFoundException, AccessDeniedException, RepositoryException {
        // EXERCISE
        return new AccessControlPolicyIteratorAdapter(ImmutableList.of());
    }

    @Override
    public void setPolicy(String absPath, AccessControlPolicy policy) throws PathNotFoundException, AccessControlException, AccessDeniedException, LockException, VersionException, RepositoryException {
        // EXERCISE

    }

    @Override
    public void removePolicy(String absPath, AccessControlPolicy policy) throws PathNotFoundException, AccessControlException, AccessDeniedException, LockException, VersionException, RepositoryException {
        // EXERCISE

    }

    @Override
    public boolean defines(@Nullable String absPath, @NotNull AccessControlPolicy accessControlPolicy) {
        if (Utils.isSupportedPath(supportedPath, absPath)) {
            // EXERCISE
        }
        return false;
    }
}
