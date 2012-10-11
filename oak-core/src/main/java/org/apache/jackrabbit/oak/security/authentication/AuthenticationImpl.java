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
package org.apache.jackrabbit.oak.security.authentication;

import java.util.Collections;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.PasswordUtility;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuthenticationImpl...
 */
public class AuthenticationImpl implements Authentication {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationImpl.class);

    private final String userId;
    private final UserProvider userProvider;
    private final PrincipalProvider principalProvider;

    public AuthenticationImpl(String userId, UserProvider userProvider, PrincipalProvider principalProvider) {
        this.userId = userId;
        this.userProvider = userProvider;
        this.principalProvider = principalProvider;
    }

    @Override
    public boolean authenticate(Credentials credentials) {
        Tree userTree = getUserTree();
        if (userTree == null || userProvider.isDisabled(userTree)) {
            return false;
        }

        boolean success;
        if (credentials instanceof SimpleCredentials) {
            SimpleCredentials creds = (SimpleCredentials) credentials;
            success = PasswordUtility.isSame(userProvider.getPasswordHash(userTree), creds.getPassword());
        } else if (credentials instanceof ImpersonationCredentials) {
            AuthInfo info = ((ImpersonationCredentials) credentials).getImpersonatorInfo();
            success = impersonate(info, userTree);
        } else {
            // guest login is allowed if an anonymous user exists in the content (see getUserTree above)
            success = (credentials instanceof GuestCredentials);
        }
        return success;
    }

    //--------------------------------------------------------------------------
    private Tree getUserTree() {
        if (userProvider == null || userId == null) {
            return null;
        } else {
            return userProvider.getAuthorizable(userId, AuthorizableType.USER);
        }
    }

    private boolean impersonate(AuthInfo info, Tree userTree) {
        Subject subject = new Subject(true, info.getPrincipals(), Collections.emptySet(), Collections.emptySet());
        try {
            return userProvider.getImpersonation(userTree, principalProvider).allows(subject);
        } catch (RepositoryException e) {
            log.debug("Error while validating impersonation", e.getMessage());
        }
        return false;
    }
}