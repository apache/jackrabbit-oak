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
package org.apache.jackrabbit.oak.security.authentication.user;

import java.util.Collections;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.security.user.CredentialsImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Authentication interface that validates credentials
 * against user information stored in the repository. If no user exists with
 * the specified userID or if the user has been disabled authentication will
 * will fail irrespective of the specified credentials. Otherwise the following
 * validation is performed:
 *
 * <ul>
 *     <li>{@link SimpleCredentials}: Authentication succeeds if userID and
 *     password match the information exposed by the {@link UserManager}.</li>
 *     <li>{@link ImpersonationCredentials}: Authentication succeeds if the
 *     subject to be authenticated is allowed to impersonate the user identified
 *     by the userID.</li>
 *     <li>{@link GuestCredentials}: The authentication succeeds if an 'anonymous'
 *     user exists in the repository.</li>
 * </ul>
 *
 * For any other credentials {@link #authenticate(javax.jcr.Credentials)}
 * will return {@code false} indicating that this implementation is not able
 * to verify their validity.
 */
class UserAuthentication implements Authentication {

    private static final Logger log = LoggerFactory.getLogger(UserAuthentication.class);

    private final String userId;
    private final UserManager userManager;

    UserAuthentication(String userId, UserManager userManager) {
        this.userId = userId;
        this.userManager = userManager;
    }

    @Override
    public boolean authenticate(Credentials credentials) throws LoginException {
        if (userId == null || userManager == null) {
            return false;
        }

        boolean success = false;
        try {
            Authorizable authorizable = userManager.getAuthorizable(userId);
            if (authorizable == null || authorizable.isGroup()) {
                throw new LoginException("Unknown user " + userId);
            }

            User user = (User) authorizable;
            if (user.isDisabled()) {
                throw new LoginException("User with ID " + userId + " has been disabled: "+ user.getDisabledReason());
            }

            if (credentials instanceof SimpleCredentials) {
                SimpleCredentials creds = (SimpleCredentials) credentials;
                Credentials userCreds = user.getCredentials();
                if (userId.equals(creds.getUserID()) && userCreds instanceof CredentialsImpl) {
                    success = PasswordUtility.isSame(((CredentialsImpl) userCreds).getPasswordHash(), creds.getPassword());
                }
                checkSuccess(success, "UserId/Password mismatch.");
            } else if (credentials instanceof ImpersonationCredentials) {
                ImpersonationCredentials ipCreds = (ImpersonationCredentials) credentials;
                AuthInfo info = ipCreds.getImpersonatorInfo();
                success = equalUserId(ipCreds) && impersonate(info, user);
                checkSuccess(success, "Impersonation not allowed.");
            } else {
                // guest login is allowed if an anonymous user exists in the content (see get user above)
                success = (credentials instanceof GuestCredentials);
            }
        } catch (RepositoryException e) {
            throw new LoginException(e.getMessage());
        }
        return success;
    }

    //--------------------------------------------------------------------------
    private static void checkSuccess(boolean success, String msg) throws LoginException {
        if (!success) {
            throw new LoginException(msg);
        }
    }

    private boolean equalUserId(ImpersonationCredentials creds) {
        Credentials base = creds.getBaseCredentials();
        return (base instanceof SimpleCredentials) && userId.equals(((SimpleCredentials) base).getUserID());
    }

    private boolean impersonate(AuthInfo info, User user) {
        try {
            if (info.getUserID().equals(user.getID())) {
                log.debug("User " + info.getUserID() + " wants to impersonate himself -> success.");
                return true;
            } else {
                log.debug("User " + info.getUserID() + " wants to impersonate " + user.getID());
                Subject subject = new Subject(true, info.getPrincipals(), Collections.emptySet(), Collections.emptySet());
                return user.getImpersonation().allows(subject);
            }
        } catch (RepositoryException e) {
            log.debug("Error while validating impersonation", e.getMessage());
        }
        return false;
    }
}