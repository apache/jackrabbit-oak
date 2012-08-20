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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlContext;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;

/**
 * PermissionProviderImpl... TODO
 */
public class AccessControlContextImpl implements AccessControlContext {

    private static final CompiledPermissions NO_PERMISSIONS = new SimplePermissions(false);
    private static final CompiledPermissions ADMIN_PERMISSIONS = new SimplePermissions(true);

    private Set<Principal> principals;

    //-----------------------------------------------< AccessControlContext >---
    @Override
    public void initialize(Set<Principal> principals) {
        this.principals = principals;
    }

    @Override
    public CompiledPermissions getPermissions() {
        if (principals == null || principals.isEmpty()) {
            return NO_PERMISSIONS;
        } else if (principals.contains(AdminPrincipal.INSTANCE)) {
            return ADMIN_PERMISSIONS;
        } else {
            // TODO: replace with permissions based on ac evaluation
            return new CompiledPermissionImpl(principals);
        }
    }

    @Override
    public ValidatorProvider getPermissionValidatorProvider(CoreValueFactory valueFactory) {
        return new PermissionValidatorProvider(valueFactory, this);
    }

    @Override
    public ValidatorProvider getAccessControlValidatorProvider(CoreValueFactory valueFactory) {
        return new AccessControlValidatorProvider(valueFactory, this);
    }

    //--------------------------------------------------------------------------
    /**
     * Trivial implementation of the {@code CompiledPermissions} interface that
     * either allows or denies all permissions.
     */
    private static final class SimplePermissions implements CompiledPermissions {

        private final boolean allowed;

        private SimplePermissions(boolean allowed) {
            this.allowed = allowed;
        }

        @Override
        public boolean canRead(String path, boolean isProperty) {
            return allowed;
        }

        @Override
        public boolean isGranted(String path, int permissions) {
            return allowed;
        }

    }
}
