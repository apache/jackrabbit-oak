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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.security.auth.Subject;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for PrincipalBasedAccessControlManager where the editing session (based on a system user with both default and
 * principal-based permission evaluation) lacks permissions to read/modify access control on the target system-principal.
 */
public class AccessControlManagerLimitedSystemUserTest extends AccessControlManagerLimitedUserTest {

    private final String UID = "testSystemSession" + UUID.randomUUID();

    @Override
    public void after() throws Exception {
        try {
            Authorizable a = getUserManager(root).getAuthorizable(UID);
            if (a != null) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    Principal createTestPrincipal() throws Exception {
        User testUser = getUserManager(root).createSystemUser(UID, INTERMEDIATE_PATH);
        root.commit();
        return testUser.getPrincipal();
    }

    Root createTestRoot() throws Exception {
        Set<Principal> principals = Set.of(testPrincipal);
        AuthInfo authInfo = new AuthInfoImpl(UID, Collections.emptyMap(), principals);
        Subject subject = new Subject(true, principals, Set.of(authInfo), Set.of());
        return Java23Subject.doAsPrivileged(subject, (PrivilegedExceptionAction<Root>) () -> getContentRepository().login(null, null).getLatestRoot(), null);
    }

    void grant(@NotNull Principal principal, @Nullable String path, @NotNull String... privNames) throws Exception {
        super.grant(principal, path, privNames);
        setupPrincipalBasedAccessControl(principal, path, privNames);
    }

}