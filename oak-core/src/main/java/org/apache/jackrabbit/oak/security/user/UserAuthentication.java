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
package org.apache.jackrabbit.oak.security.user;

import java.security.Principal;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.login.AccountLockedException;
import javax.security.auth.login.AccountNotFoundException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.CredentialExpiredException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
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
class UserAuthentication implements Authentication, UserConstants {

    private static final Logger log = LoggerFactory.getLogger(UserAuthentication.class);

    private final UserConfiguration config;
    private final Root root;
    private final String loginId;

    private String userId;
    private Principal principal;

    UserAuthentication(@Nonnull UserConfiguration config, @Nonnull Root root, @Nullable String loginId) {
        this.config = config;
        this.root = root;
        this.loginId = loginId;
    }

    //-----------------------------------------------------< Authentication >---
    @Override
    public boolean authenticate(@Nullable Credentials credentials) throws LoginException {
        if (credentials == null || loginId == null) {
            return false;
        }

        boolean success = false;
        try {
            UserManager userManager = config.getUserManager(root, NamePathMapper.DEFAULT);
            Authorizable authorizable = userManager.getAuthorizable(loginId);
            if (authorizable == null) {
                return false;
            }

            if (authorizable.isGroup()) {
                throw new AccountNotFoundException("Not a user " + loginId);
            }

            User user = (User) authorizable;
            if (user.isDisabled()) {
                throw new AccountLockedException("User with ID " + loginId + " has been disabled: "+ user.getDisabledReason());
            }

            if (credentials instanceof SimpleCredentials) {
                SimpleCredentials creds = (SimpleCredentials) credentials;
                Credentials userCreds = user.getCredentials();
                if (loginId.equals(creds.getUserID()) && userCreds instanceof CredentialsImpl) {
                    success = PasswordUtil.isSame(((CredentialsImpl) userCreds).getPasswordHash(), creds.getPassword());
                }
                checkSuccess(success, "UserId/Password mismatch.");

                if (isPasswordExpired(user)) {
                    // change the password if the credentials object has the
                    // UserConstants.CREDENTIALS_ATTRIBUTE_NEWPASSWORD attribute set
                    if (!changePassword(user, creds)) {
                        throw new CredentialExpiredException("User password has expired");
                    }
                }
            } else if (credentials instanceof ImpersonationCredentials) {
                ImpersonationCredentials ipCreds = (ImpersonationCredentials) credentials;
                AuthInfo info = ipCreds.getImpersonatorInfo();
                success = equalUserId(ipCreds, loginId) && impersonate(info, user);
                checkSuccess(success, "Impersonation not allowed.");
            } else {
                // guest login is allowed if an anonymous user exists in the content (see get user above)
                success = (credentials instanceof GuestCredentials) || credentials == PreAuthenticatedLogin.PRE_AUTHENTICATED;
            }
            userId = user.getID();
            principal = user.getPrincipal();
        } catch (RepositoryException e) {
            throw new LoginException(e.getMessage());
        }
        return success;
    }

    @CheckForNull
    @Override
    public String getUserId() {
        if (userId == null) {
            throw new IllegalStateException("UserId can only be retrieved after successful authentication.");
        }
        return userId;
    }

    @CheckForNull
    @Override
    public Principal getUserPrincipal() {
        if (principal == null) {
            throw new IllegalStateException("Principal can only be retrieved after successful authentication.");
        }
        return principal;
    }


    //--------------------------------------------------------------------------
    private static void checkSuccess(boolean success, String msg) throws LoginException {
        if (!success) {
            throw new FailedLoginException(msg);
        }
    }

    private static boolean equalUserId(@Nonnull ImpersonationCredentials creds, @Nonnull String userId) {
        Credentials base = creds.getBaseCredentials();
        return (base instanceof SimpleCredentials) && userId.equals(((SimpleCredentials) base).getUserID());
    }

    private boolean changePassword(User user, SimpleCredentials credentials) {
        try {
            Object newPasswordObject = credentials.getAttribute(CREDENTIALS_ATTRIBUTE_NEWPASSWORD);
            if (newPasswordObject != null) {
                if (newPasswordObject instanceof String) {
                    user.changePassword((String) newPasswordObject);
                    root.commit();
                    log.debug("User " + loginId + ": changed user password");
                    return true;
                } else {
                    log.warn("Aborted password change for user " + loginId
                            + ": provided new password is of incompatible type "
                            + newPasswordObject.getClass().getName());
                }
            }
        } catch (PasswordHistoryException e) {
            credentials.setAttribute(e.getClass().getSimpleName(), e.getMessage());
            log.error("Failed to change password for user " + loginId, e.getMessage());
        } catch (RepositoryException e) {
            log.error("Failed to change password for user " + loginId, e.getMessage());
        } catch (CommitFailedException e) {
            root.refresh();
            log.error("Failed to change password for user " + loginId, e.getMessage());
        }
        return false;
    }

    private boolean impersonate(AuthInfo info, User user) {
        try {
            if (user.getID().equals(info.getUserID())) {
                log.debug("User " + info.getUserID() + " wants to impersonate himself -> success.");
                return true;
            } else {
                log.debug("User " + info.getUserID() + " wants to impersonate " + user.getID());
                Subject subject = new Subject(true, info.getPrincipals(), Collections.emptySet(), Collections.emptySet());
                return user.getImpersonation().allows(subject);
            }
        } catch (RepositoryException e) {
            log.debug("Error while validating impersonation: {}", e.getMessage());
        }
        return false;
    }

    @CheckForNull
    private Long getPasswordLastModified(User user) throws RepositoryException {
        Tree userTree;
        if (user instanceof UserImpl) {
            userTree = ((UserImpl) user).getTree();
        } else {
            userTree = root.getTree(user.getPath());
        }
        PropertyState property = userTree.getChild(REP_PWD).getProperty(REP_PASSWORD_LAST_MODIFIED);
        return (property != null) ? property.getValue(Type.LONG) : null;
    }

    private boolean isPasswordExpired(@Nonnull User user) throws RepositoryException {
        // the password of the "admin" user never expires
        if (user.isAdmin()) {
            return false;
        }

        boolean expired = false;
        ConfigurationParameters params = config.getParameters();
        int maxAge = params.getConfigValue(PARAM_PASSWORD_MAX_AGE, DEFAULT_PASSWORD_MAX_AGE);
        boolean forceInitialPwChange = params.getConfigValue(PARAM_PASSWORD_INITIAL_CHANGE, DEFAULT_PASSWORD_INITIAL_CHANGE);
        if (maxAge > 0) {
            // password expiry is enabled
            Long passwordLastModified = getPasswordLastModified(user);
            if (passwordLastModified == null) {
                // no pw last modified property exists (yet) => expire!
                expired = true;
            } else {
                // calculate expiry time (pw last mod + pw max age) and compare
                long expiryTime = passwordLastModified + TimeUnit.MILLISECONDS.convert(maxAge, TimeUnit.DAYS);
                // System.currentTimeMillis() may be inaccurate on windows. This is accepted for this feature.
                expired = expiryTime < System.currentTimeMillis();
            }
        } else if (forceInitialPwChange) {
            Long passwordLastModified = getPasswordLastModified(user);
            // no pw last modified property exists (yet) => expire!
            expired = (null == passwordLastModified);
        }
        return expired;
    }
}
