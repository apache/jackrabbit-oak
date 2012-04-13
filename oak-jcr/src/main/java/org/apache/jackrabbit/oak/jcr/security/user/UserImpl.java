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
package org.apache.jackrabbit.oak.jcr.security.user;

import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import java.security.Principal;

/**
 * UserImpl...
 */
public class UserImpl extends AuthorizableImpl implements User {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(UserImpl.class);

    UserImpl(NodeImpl node, UserManagerImpl userManager) {
        super(node, userManager);
    }

    //-------------------------------------------------------< Authorizable >---
    @Override
    public boolean isGroup() {
        return false;
    }

    @Override
    public Principal getPrincipal() throws RepositoryException {
        String principalName = getPrincipalName();
        return new ItemBasedPrincipalImpl(principalName, getNode());

    }

    //---------------------------------------------------------------< User >---
    @Override
    public boolean isAdmin() {
        try {
            return getUserManager().isAdminId(getID());
        } catch (RepositoryException e) {
            // should never get here
            log.error("Internal error while retrieving UserID.", e);
            return false;
        }
    }

    @Override
    public Credentials getCredentials() throws RepositoryException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public Impersonation getImpersonation() throws RepositoryException {
       return new ImpersonationImpl(this);
    }

    @Override
    public void changePassword(String password) throws RepositoryException {
        UserManagerImpl userManager = getUserManager();
        userManager.onPasswordChange(this, password);
        userManager.setPassword(getNode(), password, true);
    }

    @Override
    public void changePassword(String password, String oldPassword) throws RepositoryException {
        // make sure the old password matches.
        String pwHash = null;
        if (getNode().hasProperty(REP_PASSWORD)) {
            pwHash = getNode().getProperty(REP_PASSWORD).getString();
        }
        if (!PasswordUtility.isSame(pwHash, oldPassword)) {
            throw new RepositoryException("Failed to change password: Old password does not match.");
        }
        changePassword(password);
    }

    @Override
    public void disable(String reason) throws RepositoryException {
        if (isAdmin()) {
            throw new RepositoryException("The administrator user cannot be disabled.");
        }
        if (reason == null) {
            if (isDisabled()) {
                // enable the user again.
                getUserManager().removeInternalProperty(getNode(), REP_DISABLED);
            } // else: nothing to do.
        } else {
            getUserManager().setInternalProperty(getNode(), REP_DISABLED, reason, PropertyType.STRING);
        }
    }

    @Override
    public boolean isDisabled() throws RepositoryException {
        return getNode().hasProperty(REP_DISABLED);
    }

    @Override
    public String getDisabledReason() throws RepositoryException {
        if (isDisabled()) {
            return getNode().getProperty(REP_DISABLED).getString();
        } else {
            return null;
        }
    }
}