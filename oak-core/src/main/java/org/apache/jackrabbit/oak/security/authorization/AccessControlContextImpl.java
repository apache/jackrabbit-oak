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
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlContext;
import org.apache.jackrabbit.oak.spi.security.authorization.AllPermissions;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;

/**
 * PermissionProviderImpl... TODO
 */
class AccessControlContextImpl implements AccessControlContext {

    private final Subject subject;

    AccessControlContextImpl(Subject subject) {
        this.subject = subject;
    }

    //-----------------------------------------------< AccessControlContext >---

    @Override
    public CompiledPermissions getPermissions() {
        Set<Principal> principals = subject.getPrincipals();
        if (principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals)) {
            return AllPermissions.getInstance();
        } else {
            // TODO: replace with permissions based on ac evaluation
            return new CompiledPermissionImpl(principals);
        }
    }

    //--------------------------------------------------------------------------
    private static boolean isAdmin(Set<Principal> principals) {
        for (Principal principal : principals) {
            if (principal instanceof AdminPrincipal) {
                return true;
            }
        }
        return false;
    }
}
